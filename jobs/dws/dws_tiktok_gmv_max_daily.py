"""DWS TikTok GMV Max 投放事实 ETL。

只固化可加的分子分母，ROI 与平均下单成本交给下游按 SUM(分子) / SUM(分母) 重算。

同时过滤掉 API 生成的全零填充行：`/gmv_max/report/get/` 按 campaign × 天返回笛卡尔积，
campaign 未投放的日子也会给一行 cost=0 / orders=0 / gross_revenue=0，这类行没有信息量，
留着会让「有多少个 campaign 在投」这类计数口径直接错。

单广告主量级很小，走全表重建而不是窗口增量：没有水位判定，也就没有漏刷分区的路径。
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
TABLE_NAME = "dws_tiktok_gmv_max_daily"
SOURCE_TABLE_NAME = "ods_tiktok_gmv_max_daily"

client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="构建 TikTok GMV Max 投放事实表（dt × advertiser_id × store_id × campaign_id），全表重建"
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
    -- 原始层已按 dt × advertiser_id × store_id × campaign_id 去重，这里只挑可加口径并统一空值语义。
    SELECT
        dt,
        advertiser_id,
        store_id,
        campaign_id,
        NULLIF(campaign_name, '') AS campaign_name,
        -- 金额以广告主账户货币计价，跨广告主聚合金额前必须先按 currency 分组或换算
        currency,
        IFNULL(cost, 0) AS cost,
        IFNULL(orders, 0) AS orders,
        IFNULL(gross_revenue, 0) AS gross_revenue,
        CURRENT_TIMESTAMP() AS update_time
    FROM `{table_id(SOURCE_TABLE_NAME)}`
    -- 过滤 API 的全零填充行：campaign 那天既没花钱也没出单
    WHERE IFNULL(cost, 0) > 0
        OR IFNULL(orders, 0) > 0
        OR IFNULL(gross_revenue, 0) > 0
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
                COUNTIF(
                    dt IS NULL OR advertiser_id IS NULL OR store_id IS NULL OR campaign_id IS NULL
                ) AS null_key_count,
                COUNTIF(currency IS NULL) AS null_currency_count,
                COUNTIF(gross_revenue > 0 AND orders = 0) AS revenue_without_order_count
            FROM `{staging_table}`
            """
        ).result()
    )[0]
    if result.row_count == 0:
        raise RuntimeError("TikTok GMV Max 投放事实校验失败：staging 无数据")
    if result.row_count != result.unique_key_count:
        raise RuntimeError("TikTok GMV Max 投放事实校验失败：统计日、广告主、店铺和计划不唯一")
    if result.null_key_count:
        raise RuntimeError("TikTok GMV Max 投放事实校验失败：主键字段存在空值")
    if result.null_currency_count:
        raise RuntimeError("TikTok GMV Max 投放事实校验失败：存在没有货币单位的金额行")
    if result.revenue_without_order_count:
        # 有收入没订单说明归因回填只到了一半，下游的平均下单成本会失真，但不阻断
        logging.warning(
            "有 %d 行有收入但订单为 0，可能是归因回填中间态", result.revenue_without_order_count
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
