from datetime import datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery

# 配置日志记录
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# 支持直接执行 jobs/ads/ads_daily.py 时导入同级脚本
ADS_ROOT = Path(__file__).resolve().parent
if str(ADS_ROOT) not in sys.path:
    sys.path.insert(0, str(ADS_ROOT))

from ads_daily_new import run_ads_daily_new
from ads_daily_video import run_ads_daily_video
from ads_daily_total import run_ads_daily_total
from ads_daily_content_performance import run_ads_daily_content_performance
from ads_daily_home_module_performance import run_ads_daily_home_module_performance
from ads_daily_user_duration_frequency import run_ads_daily_user_duration_frequency
from ads_daily_tryon_confirm import run_ads_daily_tryon_confirm
from ads_daily_tryon_add_cart_conversion import run_ads_daily_tryon_add_cart_conversion
from ads_daily_post_tryon_confirm import run_ads_daily_post_tryon_confirm
from ads_daily_post_performance import run_ads_daily_post_performance
from ads_daily_column_performance import run_ads_daily_column_performance
from ads_daily_product_tryon_performance import run_ads_daily_product_tryon_performance

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
client = bigquery.Client(project=PROJECT_ID)


def incremental_dates_sql(source_table, target_table, select_expr="dt"):
    return f"""
    SELECT DISTINCT {select_expr} AS dt
    FROM `{PROJECT_ID}.{DATASET_ID}.{source_table}`
    WHERE update_time > (
        SELECT COALESCE(MAX(update_time), TIMESTAMP('1970-01-01'))
        FROM `{PROJECT_ID}.{DATASET_ID}.{target_table}`
    )
    AND dt <= CURRENT_DATE('{TORONTO_TZ}')
    """


def get_dates_to_process():
    """
    找出 DWS 层有新数据但 ADS 层尚未处理的日期。
    同时包含前一天用于回刷次日留存率。
    同时纳入下载 DWS 与内容表现日报的增量日期。
    使用多伦多时间（America/Toronto）。

    说明：
    - 允许处理到多伦多当天 dt（当天数据可能不完整，适合看趋势与实时观察）。
    - 留存/到期类指标是否产出，仍由各子任务内部的“成熟度”逻辑决定（不强制当天出结果）。
    """
    query = "\nUNION DISTINCT\n".join(
        [
            # 支持 T+0 展示：只要 DWS 已有多伦多“当天/昨天”的分区，就强制纳入这两天 dt。
            # 这样可以避免因为 update_time 对比（DWS <= ADS）导致“当天有数据但 ADS 不刷新”的情况，
            # 同时保证“昨天”的次日留存等需要 T+1 才成熟的指标能在今天回刷。
            f"""
            SELECT DISTINCT dt
            FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
            WHERE dt IN (
                CURRENT_DATE('{TORONTO_TZ}'),
                DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY)
            )
            """,
            incremental_dates_sql("dws_device_daily", "ads_daily_total"),
            incremental_dates_sql(
                "dws_device_daily",
                "ads_daily_total",
                "DATE_SUB(dt, INTERVAL 1 DAY)",
            ),
            incremental_dates_sql("dws_download_daily", "ads_daily_new"),
            incremental_dates_sql("dws_video_daily", "ads_daily_video"),
            incremental_dates_sql(
                "dws_device_daily",
                "ads_daily_content_performance",
            ),
            incremental_dates_sql(
                "dws_device_daily",
                "ads_daily_tryon_confirm",
            ),
            incremental_dates_sql(
                "dws_device_daily",
                "ads_daily_tryon_add_cart_conversion",
            ),
            incremental_dates_sql(
                "dws_device_daily",
                "ads_daily_post_tryon_confirm",
            ),
            incremental_dates_sql(
                "dws_device_daily",
                "ads_daily_post_performance",
            ),
            incremental_dates_sql(
                "dws_device_daily",
                "ads_daily_column_performance",
            ),
            incremental_dates_sql(
                "dws_device_daily",
                "ads_daily_product_tryon_performance",
            ),
            incremental_dates_sql(
                "dws_device_daily",
                "ads_daily_home_module_performance",
            ),
            incremental_dates_sql(
                "dws_device_daily",
                "ads_daily_user_duration_frequency",
            ),
            incremental_dates_sql(
                "dws_device_daily",
                "ads_daily_user_duration_frequency",
                "DATE_SUB(dt, INTERVAL 1 DAY)",
            ),
            incremental_dates_sql(
                "dws_device_daily",
                "ads_daily_user_duration_frequency",
                "DATE_SUB(dt, INTERVAL 7 DAY)",
            ),
            incremental_dates_sql(
                "dws_device_daily",
                "ads_daily_user_duration_frequency",
                "DATE_SUB(dt, INTERVAL 30 DAY)",
            ),
        ]
    )
    query += "\nORDER BY dt"
    results = client.query(query).result()
    return [row.dt for row in results]


if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)

    dates = get_dates_to_process()
    if not dates:
        logging.info("没有新数据需要处理")
        exit(0)

    logging.info(f"待处理日期: {dates}")

    run_ads_daily_new(dates)
    logging.info("-" * 50)

    run_ads_daily_video(dates)
    logging.info("-" * 50)

    run_ads_daily_total(dates)
    logging.info("-" * 50)

    run_ads_daily_content_performance(dates)
    logging.info("-" * 50)

    run_ads_daily_tryon_confirm(dates)
    logging.info("-" * 50)

    run_ads_daily_tryon_add_cart_conversion(dates)
    logging.info("-" * 50)

    run_ads_daily_post_tryon_confirm(dates)
    logging.info("-" * 50)

    run_ads_daily_post_performance(dates)
    logging.info("-" * 50)

    run_ads_daily_column_performance(dates)
    logging.info("-" * 50)

    run_ads_daily_product_tryon_performance(dates)
    logging.info("-" * 50)

    run_ads_daily_home_module_performance(dates)
    logging.info("-" * 50)

    run_ads_daily_user_duration_frequency(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"ADS ETL 完成, 耗时: {elapsed:.1f} 秒")
