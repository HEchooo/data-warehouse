"""ADS 【Social Content】TikTok GMV Max 投放日报 ETL。

粒度 dt × advertiser_id × store_id × campaign_id，与 TikTok 后台 GMV Max 看板逐列对齐。

派生值只用于看明细单行；Superset 汇总必须走 SUM(分子) / SUM(分母) 的 SQL metric。
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
TABLE_NAME = "ads_daily_tiktok_gmv_max"
SOURCE_TABLE_NAME = "dws_tiktok_gmv_max_daily"

client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="构建 TikTok GMV Max 投放日报（dt × advertiser_id × store_id × campaign_id），全表重建"
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
    -- 派生值由分子分母现算，与 TikTok 后台一致：
    -- 平均下单成本 = 成本 / SKU 订单数，ROI = 总收入 / 成本。
    -- SAFE_DIVIDE 保证分母为 0 时返回空值而不是报错，空值语义等同后台的 "-"。
    SELECT
        dt,
        advertiser_id,
        store_id,
        campaign_id,
        campaign_name,
        currency,
        cost,
        orders,
        gross_revenue,
        SAFE_DIVIDE(cost, orders) AS cost_per_order,
        SAFE_DIVIDE(gross_revenue, cost) AS roi,
        CURRENT_TIMESTAMP() AS update_time
    FROM `{table_id(SOURCE_TABLE_NAME)}`
    """


def validate(staging_table: str) -> None:
    result = list(
        client.query(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT CONCAT(
                    CAST(dt AS STRING), '|', advertiser_id, '|', store_id, '|', campaign_id
                )) AS unique_key_count,
                COUNTIF(cost > 0 AND orders > 0 AND cost_per_order IS NULL) AS broken_cpo_count,
                COUNTIF(cost > 0 AND roi IS NULL) AS broken_roi_count,
                COUNTIF(currency IS NULL) AS null_currency_count
            FROM `{staging_table}`
            """
        ).result()
    )[0]
    if result.row_count == 0:
        raise RuntimeError("TikTok GMV Max 投放日报校验失败：staging 无数据")
    if result.row_count != result.unique_key_count:
        raise RuntimeError("TikTok GMV Max 投放日报校验失败：统计日、广告主、店铺和计划不唯一")
    if result.broken_cpo_count:
        raise RuntimeError("TikTok GMV Max 投放日报校验失败：有成本有订单的行算不出平均下单成本")
    if result.broken_roi_count:
        raise RuntimeError("TikTok GMV Max 投放日报校验失败：有成本的行算不出 ROI")
    if result.null_currency_count:
        raise RuntimeError("TikTok GMV Max 投放日报校验失败：存在没有货币单位的金额行")


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
            CLUSTER BY advertiser_id, store_id, campaign_id AS
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
