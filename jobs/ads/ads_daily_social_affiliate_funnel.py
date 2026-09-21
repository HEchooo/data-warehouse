"""ADS 【Affiliate】爆款漏斗 ETL。

按 dt × region 出活跃联盟号、当天发布视频数、在投 / 当天新开投视频数、第一次投放 ROI>2.5 的视频数。量级小，全表重建。
"""

import argparse
from datetime import datetime
import logging
from zoneinfo import ZoneInfo

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "ads_daily_social_affiliate_funnel"
SNAPSHOT_TABLE_NAME = "dws_social_creator_snapshot_daily"
DIM_AFFILIATION_TABLE_NAME = "dim_ecn_account_affiliation"
DIM_VIDEO_TABLE_NAME = "dim_video"
VIDEO_DAILY_TABLE_NAME = "dws_social_affiliate_project_video_daily"
CURRENCY_DICT_TABLE_NAME = "currency_conversion_dict"
AFFILIATE_USAGE_TYPE = "tiktok-affiliate"
AFFILIATE_CHANNEL = "tiktok"
ROI_HIT_THRESHOLD = 2.5
HANDLE_KEY = "LOWER(LTRIM({alias}.account_handle, '@'))"

client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="构建【Affiliate】爆款漏斗日报（dt × region），全表重建")
    return parser.parse_args()


def table_id(table_name: str) -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{table_name}"


def target_table_exists() -> bool:
    try:
        client.get_table(table_id(TABLE_NAME))
        return True
    except NotFound:
        return False


def handle_key(alias: str) -> str:
    return HANDLE_KEY.format(alias=alias)


def build_select_sql() -> str:
    return f"""
    WITH
    -- 汇率字典只有当前快照；USD 自身不在字典里，按 1 折算。缺币种不补 1。
    currency_rate AS (
        SELECT
            UPPER(from_currency) AS from_currency,
            MAX(exchange_rate) AS usd_rate
        FROM `{table_id(CURRENCY_DICT_TABLE_NAME)}`
        WHERE UPPER(to_currency) = 'USD'
        GROUP BY 1
    ),
    -- 日历骨架：橱窗号快照最早日到昨天（美东）。缺快照的日子沿用最近一次快照。
    date_spine AS (
        SELECT dt
        FROM UNNEST(GENERATE_DATE_ARRAY(
            (
                SELECT MIN(dt)
                FROM `{table_id(SNAPSHOT_TABLE_NAME)}`
                WHERE channel = '{AFFILIATE_CHANNEL}'
                    AND usage_type = '{AFFILIATE_USAGE_TYPE}'
            ),
            DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY)
        )) AS dt
    ),
    -- 每个统计日取当天或之前最近一次橱窗号快照分区。
    snapshot_as_of AS (
        SELECT
            d.dt,
            MAX(s.dt) AS snapshot_dt
        FROM date_spine d
        INNER JOIN `{table_id(SNAPSHOT_TABLE_NAME)}` s
            ON s.channel = '{AFFILIATE_CHANNEL}'
            AND s.usage_type = '{AFFILIATE_USAGE_TYPE}'
            AND s.dt <= d.dt
        GROUP BY d.dt
    ),
    -- 按封禁日还原当时是否活跃，带上首次绑定的联盟地区。
    daily_creator AS (
        SELECT
            a.dt,
            s.creator_id,
            dim.affiliation_label AS region,
            CASE
                WHEN dim.tiktok_showcase_status = 'permanently_banned'
                    AND (
                        dim.tiktok_showcase_status_updated_at IS NULL
                        OR DATE(dim.tiktok_showcase_status_updated_at, '{TORONTO_TZ}') <= a.dt
                    )
                THEN FALSE
                ELSE TRUE
            END AS is_active
        FROM snapshot_as_of a
        INNER JOIN `{table_id(SNAPSHOT_TABLE_NAME)}` s
            ON s.channel = '{AFFILIATE_CHANNEL}'
            AND s.usage_type = '{AFFILIATE_USAGE_TYPE}'
            AND s.dt = a.snapshot_dt
        LEFT JOIN `{table_id(DIM_AFFILIATION_TABLE_NAME)}` dim
            ON dim.platform = '{AFFILIATE_CHANNEL}'
            AND {handle_key("dim")} = {handle_key("s")}
    ),
    -- 当天快照里出现过的 dt × region；该地区当天没活跃号也留行。
    spine AS (
        SELECT DISTINCT
            dt,
            region
        FROM daily_creator
    ),
    -- 当天仍活跃的联盟号数。
    active_account AS (
        SELECT
            dt,
            region,
            COUNT(DISTINCT creator_id) AS active_account_cnt
        FROM daily_creator
        WHERE is_active
        GROUP BY dt, region
    ),
    -- 视频级首次消耗日。DWS 在首次消耗前的行会把 first_ad_dt 置空，MIN 忽略空值。
    video_first_ad AS (
        SELECT
            video_key,
            MIN(first_ad_dt) AS first_ad_dt
        FROM `{table_id(VIDEO_DAILY_TABLE_NAME)}`
        GROUP BY video_key
    ),
    -- 活跃联盟号当天发布的视频；在投 = 后来有过消耗；新增 = 发布当天就开投。
    published_video AS (
        SELECT
            v.published_dt AS dt,
            a.region,
            COUNT(DISTINCT v.video_key) AS published_video_cnt,
            COUNT(DISTINCT IF(fa.first_ad_dt IS NOT NULL, v.video_key, NULL)) AS advertised_video_cnt,
            COUNT(DISTINCT IF(fa.first_ad_dt = v.published_dt, v.video_key, NULL)) AS new_ad_video_cnt
        FROM `{table_id(DIM_VIDEO_TABLE_NAME)}` v
        INNER JOIN daily_creator a
            ON a.creator_id = v.creator_id
            AND a.dt = v.published_dt
            AND a.is_active
        LEFT JOIN video_first_ad fa
            ON fa.video_key = v.video_key
        WHERE v.channel = '{AFFILIATE_CHANNEL}'
            AND v.published_dt IS NOT NULL
        GROUP BY 1, 2
    ),
    -- 第一次投放 = ad_run_cnt = 1 的那些天，ROI 用 SUM(广告 GMV 美元) / SUM(spend)。
    first_run AS (
        SELECT
            d.video_key,
            ANY_VALUE(d.published_dt) AS published_dt,
            ANY_VALUE(d.creator_id) AS creator_id,
            SUM(d.spend) AS spend,
            SUM(
                d.ad_gmv * IF(UPPER(d.ad_currency) = 'USD', 1, r.usd_rate)
            ) AS ad_gmv_usd
        FROM `{table_id(VIDEO_DAILY_TABLE_NAME)}` d
        LEFT JOIN currency_rate r
            ON r.from_currency = UPPER(d.ad_currency)
        WHERE d.ad_run_cnt = 1
        GROUP BY d.video_key
    ),
    -- 发布日当天账号仍活跃，且第一次投放 ROI > 2.5。
    first_run_hit AS (
        SELECT
            f.published_dt AS dt,
            a.region,
            COUNT(*) AS first_run_hit_video_cnt
        FROM first_run f
        INNER JOIN daily_creator a
            ON a.creator_id = f.creator_id
            AND a.dt = f.published_dt
            AND a.is_active
        WHERE f.spend > 0
            AND SAFE_DIVIDE(f.ad_gmv_usd, f.spend) > {ROI_HIT_THRESHOLD}
        GROUP BY 1, 2
    )
    SELECT
        s.dt,
        s.region,
        IFNULL(a.active_account_cnt, 0) AS active_account_cnt,
        IFNULL(p.published_video_cnt, 0) AS published_video_cnt,
        IFNULL(p.advertised_video_cnt, 0) AS advertised_video_cnt,
        IFNULL(p.new_ad_video_cnt, 0) AS new_ad_video_cnt,
        IFNULL(h.first_run_hit_video_cnt, 0) AS first_run_hit_video_cnt,
        CURRENT_TIMESTAMP() AS update_time
    FROM spine s
    LEFT JOIN active_account a
        ON a.dt = s.dt
        AND a.region IS NOT DISTINCT FROM s.region
    LEFT JOIN published_video p
        ON p.dt = s.dt
        AND p.region IS NOT DISTINCT FROM s.region
    LEFT JOIN first_run_hit h
        ON h.dt = s.dt
        AND h.region IS NOT DISTINCT FROM s.region
    """


def validate(staging_table: str) -> None:
    result = list(
        client.query(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT CONCAT(CAST(dt AS STRING), '|', IFNULL(region, ''))) AS unique_key_count,
                COUNTIF(dt IS NULL) AS null_dt_count,
                COUNTIF(active_account_cnt IS NULL) AS null_account_count,
                COUNTIF(published_video_cnt IS NULL) AS null_published_count,
                COUNTIF(advertised_video_cnt IS NULL) AS null_advertised_count,
                COUNTIF(new_ad_video_cnt IS NULL) AS null_new_ad_count,
                COUNTIF(first_run_hit_video_cnt IS NULL) AS null_hit_count,
                COUNTIF(advertised_video_cnt > published_video_cnt) AS advertised_over_published_count,
                COUNTIF(new_ad_video_cnt > advertised_video_cnt) AS new_ad_over_advertised_count,
                COUNTIF(first_run_hit_video_cnt > advertised_video_cnt) AS hit_over_advertised_count,
                COUNTIF(first_run_hit_video_cnt > published_video_cnt) AS hit_over_published_count,
                COUNTIF(dt >= CURRENT_DATE('{TORONTO_TZ}')) AS future_dt_count,
            FROM `{staging_table}`
            """
        ).result()
    )[0]
    if result.row_count == 0:
        raise RuntimeError("联盟爆款漏斗校验失败：staging 无数据")
    if result.row_count != result.unique_key_count:
        raise RuntimeError("联盟爆款漏斗校验失败：dt 和 region 不唯一")
    if result.null_dt_count:
        raise RuntimeError("联盟爆款漏斗校验失败：dt 存在空值")
    if (
        result.null_account_count
        or result.null_published_count
        or result.null_advertised_count
        or result.null_new_ad_count
        or result.null_hit_count
    ):
        raise RuntimeError("联盟爆款漏斗校验失败：计数存在空值")
    if result.advertised_over_published_count:
        raise RuntimeError("联盟爆款漏斗校验失败：在投视频数超过当天发布数")
    if result.new_ad_over_advertised_count:
        raise RuntimeError("联盟爆款漏斗校验失败：当天新开投视频数超过在投视频数")
    if result.hit_over_advertised_count:
        raise RuntimeError("联盟爆款漏斗校验失败：爆款视频数超过在投视频数")
    if result.hit_over_published_count:
        raise RuntimeError("联盟爆款漏斗校验失败：爆款视频数超过当天发布数")
    if result.future_dt_count:
        raise RuntimeError("联盟爆款漏斗校验失败：含当天或未来分区")


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
            CLUSTER BY region AS
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
