"""DWS 橱窗号视频商品成交事实 ETL。

数据源是 TikTok Shop 后台的 RPA 抓取结果（`videos.tiktok_shop_rpa_video_stats_snapshots`），
同一统计日会被反复抓取多次，这里按 `stats_date × tiktok_video_id × product_title` 保留最后一次抓取。

`product_title` 为空表示该视频当日没有匹配到商品，没有商品就没有成交口径，直接排除。

整张源表只有百行量级，走全表重建而不是窗口增量：没有水位判定，也就没有漏刷分区的路径。
"""

import argparse
from datetime import datetime
import logging
from zoneinfo import ZoneInfo

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
VIDEO_SOURCE_DATASET_ID = "videos"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_social_affiliate_video_product_daily"
DIM_VIDEO_TABLE_NAME = "dim_video"
SOURCE_TABLE_NAME = "tiktok_shop_rpa_video_stats_snapshots"
CURRENCY_DICT_TABLE_NAME = "currency_conversion_dict"
AFFILIATE_CHANNEL = "tiktok"

client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "构建橱窗号视频商品成交事实表"
            "（dt × video_key × product_title），全表重建"
        )
    )
    return parser.parse_args()


def table_id(table_name: str) -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{table_name}"


def target_table_exists() -> bool:
    try:
        client.get_table(table_id(TABLE_NAME))
        return True
    except NotFound:
        return False


def build_select_sql() -> str:
    return f"""
    WITH
    -- 汇率字典只有当前快照，没有历史；USD 自身不在字典里，按 1 折算。
    currency_rate_dim AS (
        SELECT
            UPPER(from_currency) AS from_currency,
            MAX(exchange_rate) AS usd_rate
        FROM `{table_id(CURRENCY_DICT_TABLE_NAME)}`
        WHERE UPPER(to_currency) = 'USD'
        GROUP BY 1
    ),
    -- RPA 一天内会重复抓取同一统计日，只保留最后一次抓取结果。
    latest_snapshot AS (
        SELECT * EXCEPT(row_num)
        FROM (
            SELECT
                stats_date,
                tiktok_video_id,
                product_title,
                product_url,
                product_matched_by,
                username,
                currency,
                gmv,
                items_sold,
                fetched_at,
                ROW_NUMBER() OVER (
                    PARTITION BY stats_date, tiktok_video_id, product_title
                    ORDER BY fetched_at DESC, id DESC
                ) AS row_num
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.{SOURCE_TABLE_NAME}`
            -- 没有商品名就没有商品成交口径，不产出。
            WHERE product_title IS NOT NULL
                AND stats_date IS NOT NULL
                AND NULLIF(tiktok_video_id, '') IS NOT NULL
        )
        WHERE row_num = 1
    )
    SELECT
        s.stats_date AS dt,
        '{AFFILIATE_CHANNEL}' AS channel,
        s.tiktok_video_id AS video_id,
        CONCAT('{AFFILIATE_CHANNEL}:', s.tiktok_video_id) AS video_key,
        s.username AS account_handle,
        s.product_title,
        s.product_url,
        -- 视频与商品的关联方式（当前是 publish_date，按发布日期推断，不是官方归因）。
        s.product_matched_by,
        -- 金额以 TikTok Shop 后台货币计价，跨账号聚合金额必须用 gmv_usd。
        UPPER(s.currency) AS currency,
        IFNULL(s.gmv, 0) AS gmv,
        IFNULL(s.items_sold, 0) AS items_sold,
        -- 字典缺该币种时保持空值，不静默按 1 折算成一个假的美元金额。
        IFNULL(s.gmv, 0) * IF(UPPER(s.currency) = 'USD', 1, r.usd_rate) AS gmv_usd,
        s.fetched_at,
        CURRENT_TIMESTAMP() AS update_time
    FROM latest_snapshot s
    -- 视频范围与 dim_video 对齐，绑定前存量视频已在维表落库时排除。
    INNER JOIN `{table_id(DIM_VIDEO_TABLE_NAME)}` v
        ON v.channel = '{AFFILIATE_CHANNEL}'
        AND v.video_key = CONCAT('{AFFILIATE_CHANNEL}:', s.tiktok_video_id)
    LEFT JOIN currency_rate_dim r
        ON r.from_currency = UPPER(s.currency)
    """


def validate(staging_table: str) -> None:
    result = list(
        client.query(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT CONCAT(
                    CAST(dt AS STRING), '|', video_key, '|', product_title
                )) AS unique_key_count,
                COUNTIF(dt IS NULL OR video_key IS NULL OR product_title IS NULL)
                    AS null_key_count,
                COUNTIF(currency IS NULL) AS null_currency_count,
                COUNTIF(gmv IS NULL OR items_sold IS NULL) AS null_metric_count,
                COUNTIF(gmv < 0 OR items_sold < 0) AS negative_metric_count,
                COUNTIF(gmv_usd IS NULL) AS missing_rate_count
            FROM `{staging_table}`
            """
        ).result()
    )[0]
    if result.row_count == 0:
        raise RuntimeError("橱窗号视频商品成交事实校验失败：staging 无数据")
    if result.row_count != result.unique_key_count:
        raise RuntimeError("橱窗号视频商品成交事实校验失败：统计日、视频和商品不唯一")
    if result.null_key_count:
        raise RuntimeError("橱窗号视频商品成交事实校验失败：主键字段存在空值")
    if result.null_currency_count:
        raise RuntimeError("橱窗号视频商品成交事实校验失败：存在没有货币单位的金额行")
    if result.null_metric_count:
        raise RuntimeError("橱窗号视频商品成交事实校验失败：GMV 或成交件数存在空值")
    if result.negative_metric_count:
        raise RuntimeError("橱窗号视频商品成交事实校验失败：存在负数 GMV 或成交件数")
    if result.missing_rate_count:
        logging.warning(
            "%s 有 %s 行的币种在汇率字典中缺失，gmv_usd 保持空值",
            TABLE_NAME,
            result.missing_rate_count,
        )


def run() -> None:
    target_table = table_id(TABLE_NAME)
    run_id = datetime.now(ZoneInfo(TORONTO_TZ)).strftime("%Y%m%d%H%M%S")
    staging_table = f"{target_table}_tmp_{run_id}"
    target_dropped = False

    try:
        client.query(
            f"""
            CREATE TABLE `{staging_table}`
            PARTITION BY dt
            CLUSTER BY channel, video_key AS
            {build_select_sql()}
            """
        ).result()

        validate(staging_table)

        if target_table_exists():
            client.query(f"DROP TABLE `{target_table}`").result()
            target_dropped = True
        client.query(f"ALTER TABLE `{staging_table}` RENAME TO {TABLE_NAME}").result()
    except Exception:
        if target_dropped:
            logging.error("目标表已删除但换名未完成，staging 表保留待恢复: %s", staging_table)
        else:
            client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
        raise

    logging.info("%s 刷新完成", TABLE_NAME)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    started_at = datetime.now(ZoneInfo(TORONTO_TZ))
    parse_args()
    run()
    logging.info(
        "%s 完成，耗时: %.1f 秒",
        TABLE_NAME,
        (datetime.now(ZoneInfo(TORONTO_TZ)) - started_at).total_seconds(),
    )
