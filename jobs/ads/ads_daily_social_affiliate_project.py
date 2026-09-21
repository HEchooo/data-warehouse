"""ADS 【Affiliate】投放数据看板 ETL。

取 max、转 USD、现算 ROI / CPM / CPC / CPS / 广告投放后带来播放点击率 / 广告投放后带来详情页购买转化率，
并去掉已投流但 spend 与广告 GMV 都为 0 的行。量级小，全表重建。
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
TABLE_NAME = "ads_daily_social_affiliate_project"
SOURCE_TABLE_NAME = "dws_social_affiliate_project_video_daily"
CURRENCY_DICT_TABLE_NAME = "currency_conversion_dict"
ECN_WEB_BASE_URL = "http://34.55.116.212/echo-matrix"

client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="构建【Affiliate】投放数据看板日报（dt × video_key），全表重建"
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
    -- 汇率字典只有当前快照；USD 自身不在字典里，按 1 折算。缺币种不补 1。
    currency_rate AS (
        SELECT
            UPPER(from_currency) AS from_currency,
            MAX(exchange_rate) AS usd_rate
        FROM `{table_id(CURRENCY_DICT_TABLE_NAME)}`
        WHERE UPPER(to_currency) = 'USD'
        GROUP BY 1
    ),
    -- 先把两套金额转成美元，再取 max。
    converted AS (
        SELECT
            d.dt,
            d.region,
            d.channel,
            d.creator_id,
            d.account_name,
            d.account_handle,
            d.video_id,
            d.video_key,
            d.published_dt,
            d.video_link,
            d.ecn_task_id,
            d.spend,
            d.ad_gmv,
            d.ad_gmv * IF(UPPER(d.ad_currency) = 'USD', 1, ar.usd_rate) AS ad_gmv_usd,
            d.rpa_gmv,
            d.rpa_gmv * IF(UPPER(d.rpa_currency) = 'USD', 1, rr.usd_rate) AS rpa_gmv_usd,
            d.ad_items,
            d.ad_purchases,
            d.rpa_items,
            d.items_before_ads,
            d.view_increment,
            d.like_increment,
            d.impressions,
            d.clicks,
            d.video_play_actions,
            d.onsite_on_web_detail,
            d.first_ad_dt,
            d.ad_run_cnt,
            d.ad_currency,
            d.rpa_currency,
            d.has_ad
        FROM `{table_id(SOURCE_TABLE_NAME)}` d
        LEFT JOIN currency_rate ar
            ON ar.from_currency = UPPER(d.ad_currency)
        LEFT JOIN currency_rate rr
            ON rr.from_currency = UPPER(d.rpa_currency)
        -- 已连上广告且消耗、广告 GMV 都为 0：接口全零填充，看板去掉。未投流行 has_ad=false，保留。
        WHERE NOT (
            d.has_ad
            AND IFNULL(d.spend, 0) = 0
            AND IFNULL(d.ad_gmv, 0) = 0
        )
    ),
    -- 行级 GREATEST：一侧为空时用另一侧，两侧都空则空。
    ranked AS (
        SELECT
            * EXCEPT(has_ad),
            CASE
                WHEN ad_gmv_usd IS NULL AND rpa_gmv_usd IS NULL THEN NULL
                ELSE GREATEST(IFNULL(ad_gmv_usd, 0), IFNULL(rpa_gmv_usd, 0))
            END AS sales_amount_usd,
            CASE
                WHEN ad_items IS NULL AND rpa_items IS NULL THEN NULL
                ELSE GREATEST(IFNULL(ad_items, 0), IFNULL(rpa_items, 0))
            END AS sales_items
        FROM converted
    )
    SELECT
        dt,
        region,
        channel,
        creator_id,
        account_name,
        account_handle,
        video_id,
        video_key,
        published_dt,
        'video_url' AS video_url,
        video_link,
        CASE
            WHEN NULLIF(ecn_task_id, '') IS NULL THEN NULL
            ELSE CONCAT('{ECN_WEB_BASE_URL}/dashboard/video-tasks/', ecn_task_id)
        END AS ecn_detail_url,
        spend,
        ad_gmv,
        ad_gmv_usd,
        rpa_gmv,
        rpa_gmv_usd,
        sales_amount_usd,
        ad_items,
        ad_purchases,
        rpa_items,
        sales_items,
        items_before_ads,
        sales_items - items_before_ads - IFNULL(ad_items, 0) AS organic_items_after_ads,
        view_increment,
        like_increment,
        first_ad_dt,
        ad_run_cnt,
        impressions,
        clicks,
        video_play_actions,
        onsite_on_web_detail,
        SAFE_DIVIDE(spend, impressions) * 1000 AS cpm,
        SAFE_DIVIDE(spend, clicks) AS cpc,
        SAFE_DIVIDE(spend, ad_purchases) AS cps,
        SAFE_DIVIDE(clicks, video_play_actions) AS play_click_rate,
        SAFE_DIVIDE(ad_purchases, clicks) AS detail_purchase_cvr,
        SAFE_DIVIDE(ad_gmv_usd, spend) AS roi,
        ad_currency,
        rpa_currency,
        CURRENT_TIMESTAMP() AS update_time
    FROM ranked
    """


def validate(staging_table: str) -> None:
    result = list(
        client.query(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT CONCAT(CAST(dt AS STRING), '|', video_key)) AS unique_key_count,
                COUNTIF(dt IS NULL OR video_key IS NULL) AS null_key_count,
                COUNTIF(channel IS NULL OR channel = '') AS null_channel_count,
                COUNTIF(spend > 0 AND ad_gmv_usd > 0 AND roi IS NULL) AS broken_roi_count,
                COUNTIF(spend > 0 AND impressions > 0 AND cpm IS NULL) AS broken_cpm_count,
                COUNTIF(spend > 0 AND clicks > 0 AND cpc IS NULL) AS broken_cpc_count,
                COUNTIF(spend > 0 AND ad_purchases > 0 AND cps IS NULL) AS broken_cps_count,
                COUNTIF(
                    clicks > 0
                    AND video_play_actions > 0
                    AND play_click_rate IS NULL
                ) AS broken_play_click_rate_count,
                COUNTIF(
                    ad_purchases > 0
                    AND clicks > 0
                    AND detail_purchase_cvr IS NULL
                ) AS broken_detail_cvr_count,
                COUNTIF(
                    IFNULL(spend, 0) = 0
                    AND IFNULL(ad_gmv, 0) = 0
                    AND ad_gmv IS NOT NULL
                    AND spend IS NOT NULL
                ) AS zero_fill_count
            FROM `{staging_table}`
            """
        ).result()
    )[0]
    if result.row_count == 0:
        raise RuntimeError("联盟项目看板日报校验失败：staging 无数据")
    if result.row_count != result.unique_key_count:
        raise RuntimeError("联盟项目看板日报校验失败：统计日和视频不唯一")
    if result.null_key_count:
        raise RuntimeError("联盟项目看板日报校验失败：主键字段存在空值")
    if result.null_channel_count:
        raise RuntimeError("联盟项目看板日报校验失败：channel 存在空值")
    if result.broken_roi_count:
        raise RuntimeError("联盟项目看板日报校验失败：有消耗有广告 GMV 的行算不出 ROI")
    if result.broken_cpm_count:
        raise RuntimeError("联盟项目看板日报校验失败：有消耗有展示的行算不出 CPM")
    if result.broken_cpc_count:
        raise RuntimeError("联盟项目看板日报校验失败：有消耗有点击的行算不出 CPC")
    if result.broken_cps_count:
        raise RuntimeError("联盟项目看板日报校验失败：有消耗有成交笔数的行算不出 CPS")
    if result.broken_play_click_rate_count:
        raise RuntimeError("联盟项目看板日报校验失败：有点击有广告播放的行算不出广告投放后带来播放点击率")
    if result.broken_detail_cvr_count:
        raise RuntimeError("联盟项目看板日报校验失败：有成交有点击的行算不出广告投放后带来详情页购买转化率")
    if result.zero_fill_count:
        raise RuntimeError("联盟项目看板日报校验失败：仍有 spend 与广告 GMV 都为 0 的已投流行")


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
            CLUSTER BY creator_id, video_key AS
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
