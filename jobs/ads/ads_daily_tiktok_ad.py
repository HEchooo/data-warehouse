"""ADS 【Social Content】Makepod 店铺 TK 广告投放数据日报 ETL。

粒度 dt × advertiser_id × ad_id，与 TikTok 后台 AUCTION_AD 报表逐列对齐。

派生率值只用于看明细单行；Superset 汇总必须走 SUM(分子) / SUM(分母) 的 SQL metric。
数据量小，走全表重建，不做窗口增量。
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
TABLE_NAME = "ads_daily_tiktok_ad"
SOURCE_TABLE_NAME = "dws_tiktok_ad_daily"

client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="构建 TikTok 广告投放日报（dt × advertiser_id × ad_id），全表重建"
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
    -- 率值全部由分子分母现算，与 TikTok 后台一致：成本类为「消耗 / 次数」，率值类为百分比。
    -- SAFE_DIVIDE 保证分母为 0 时返回空值而不是报错，空值语义等同后台的 "-"。
    SELECT
        dt,
        advertiser_id,
        campaign_id,
        campaign_name,
        adgroup_id,
        adgroup_name,
        ad_id,
        ad_name,
        store_id,
        currency,
        spend,
        impressions,
        clicks,
        conversion,
        result,
        secondary_goal_result,
        SAFE_DIVIDE(spend, clicks) AS cpc,
        SAFE_DIVIDE(spend, impressions) * 1000 AS cpm,
        SAFE_DIVIDE(clicks, impressions) * 100 AS ctr,
        SAFE_DIVIDE(spend, conversion) AS cost_per_conversion,
        SAFE_DIVIDE(conversion, clicks) * 100 AS conversion_rate,
        SAFE_DIVIDE(spend, result) AS cost_per_result,
        SAFE_DIVIDE(result, impressions) * 100 AS result_rate,
        SAFE_DIVIDE(spend, secondary_goal_result) AS cost_per_secondary_goal_result,
        SAFE_DIVIDE(secondary_goal_result, clicks) * 100 AS secondary_goal_result_rate,
        CURRENT_TIMESTAMP() AS update_time
    FROM `{table_id(SOURCE_TABLE_NAME)}`
    """


def validate(staging_table: str) -> None:
    result = list(
        client.query(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT CONCAT(CAST(dt AS STRING), '|', advertiser_id, '|', ad_id))
                    AS unique_key_count,
                COUNTIF(spend > 0 AND clicks > 0 AND cpc IS NULL) AS broken_cpc_count,
                COUNTIF(currency IS NULL) AS null_currency_count
            FROM `{staging_table}`
            """
        ).result()
    )[0]
    if result.row_count == 0:
        raise RuntimeError("TikTok 广告投放日报校验失败：staging 无数据")
    if result.row_count != result.unique_key_count:
        raise RuntimeError("TikTok 广告投放日报校验失败：统计日、广告主和广告不唯一")
    if result.broken_cpc_count:
        raise RuntimeError("TikTok 广告投放日报校验失败：有消耗有点击的行算不出 CPC")
    if result.null_currency_count:
        raise RuntimeError("TikTok 广告投放日报校验失败：存在没有货币单位的金额行")


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
            CLUSTER BY advertiser_id, store_id, ad_id AS
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
