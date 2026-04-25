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
from ads_daily_total import run_ads_daily_total
from ads_daily_user_duration_frequency import run_ads_daily_user_duration_frequency
from ads_daily_tryon_confirm import run_ads_daily_tryon_confirm
from ads_daily_tryon_add_cart_conversion import run_ads_daily_tryon_add_cart_conversion
from ads_daily_post_tryon_confirm import run_ads_daily_post_tryon_confirm
from ads_daily_product_tryon_performance import run_ads_daily_product_tryon_performance
from ads_daily_investor import run_ads_daily_investor
from ads_daily_retention_user_feature import run_ads_daily_retention_user_feature

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


def device_t0_dates_sql():
    """
    支持 T+0 展示：只要 dws_device_daily 已有多伦多当天/昨天分区，就强制纳入。
    这样可以避免同日数据已到但因 update_time 对比未触发 ADS 刷新。
    """
    return f"""
    SELECT DISTINCT dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
    WHERE dt IN (
        CURRENT_DATE('{TORONTO_TZ}'),
        DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY)
    )
    """


def execute_dates_query(parts):
    """
    把多段日期 SQL 做 UNION DISTINCT，返回排序后的日期列表。
    空列表直接返回空，避免生成非法 SQL。
    """
    if not parts:
        return []

    query = "\nUNION DISTINCT\n".join(parts)
    query += "\nORDER BY dt"
    results = client.query(query).result()
    return [row.dt for row in results]


def get_total_dates_to_process():
    """
    ads_daily_total 的日期集合。
    依赖 dws_device_daily，并额外回刷前一天以支撑次留类指标成熟。
    """
    return execute_dates_query(
        [
            device_t0_dates_sql(),
            incremental_dates_sql("dws_device_daily", "ads_daily_total"),
            incremental_dates_sql(
                "dws_device_daily",
                "ads_daily_total",
                "DATE_SUB(dt, INTERVAL 1 DAY)",
            ),
        ]
    )


def get_content_dates_to_process():
    """
    事件内容类 ADS 的日期集合。
    只保留仍走共享设备水位的内容事件 ADS。
    """
    return execute_dates_query(
        [
            device_t0_dates_sql(),
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
                "ads_daily_product_tryon_performance",
            ),
        ]
    )


def get_download_dates_to_process():
    """
    ads_daily_new 的日期集合。
    下载链路回刷只影响新增报表，不再拖动其他事件类 ADS。
    """
    return execute_dates_query(
        [
            incremental_dates_sql("dws_download_daily", "ads_daily_new"),
        ]
    )


def get_frequency_dates_to_process():
    """
    ads_daily_user_duration_frequency 的日期集合。
    依赖 dws_device_daily，并保留 1/7/30 天回看窗口。
    """
    return execute_dates_query(
        [
            device_t0_dates_sql(),
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


def get_retention_dates_to_process():
    """
    ads_daily_retention_user_feature 的日期集合。
    依赖 dws_user_daily，并保留 1/7 天回看窗口。
    """
    return execute_dates_query(
        [
            incremental_dates_sql(
                "dws_user_daily",
                "ads_daily_retention_user_feature",
            ),
            incremental_dates_sql(
                "dws_user_daily",
                "ads_daily_retention_user_feature",
                "DATE_SUB(dt, INTERVAL 1 DAY)",
            ),
            incremental_dates_sql(
                "dws_user_daily",
                "ads_daily_retention_user_feature",
                "DATE_SUB(dt, INTERVAL 7 DAY)",
            ),
        ]
    )


def get_investor_dates_to_process():
    """
    ads_daily_investor 的日期集合。
    投资人报表跨事件、用户、视频、Appsflyer 与内容 DWS，多链路合并处理。
    """
    return execute_dates_query(
        [
            incremental_dates_sql(
                "dws_device_daily",
                "ads_daily_investor",
            ),
            incremental_dates_sql(
                "dws_user_daily",
                "ads_daily_investor",
            ),
            incremental_dates_sql(
                "dws_user_daily",
                "ads_daily_investor",
                "DATE_SUB(dt, INTERVAL 1 DAY)",
            ),
            incremental_dates_sql(
                "dws_user_daily",
                "ads_daily_investor",
                "DATE_SUB(dt, INTERVAL 7 DAY)",
            ),
            incremental_dates_sql(
                "dws_video_daily",
                "ads_daily_investor",
            ),
            incremental_dates_sql(
                "dws_appsflyer_download_daily",
                "ads_daily_investor",
            ),
            incremental_dates_sql(
                "dws_content_item_device_daily",
                "ads_daily_investor",
            ),
            incremental_dates_sql(
                "dws_product_detail_daily",
                "ads_daily_investor",
            ),
        ]
    )


def log_dates(label, dates):
    """统一打印每组待处理日期，空组也显式记录。"""
    logging.info(f"{label}待处理日期: {dates if dates else '[]'}")


if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)

    total_dates = get_total_dates_to_process()
    content_dates = get_content_dates_to_process()
    download_dates = get_download_dates_to_process()
    frequency_dates = get_frequency_dates_to_process()
    investor_dates = get_investor_dates_to_process()
    retention_dates = get_retention_dates_to_process()

    if not any(
        [
            total_dates,
            content_dates,
            download_dates,
            frequency_dates,
            investor_dates,
            retention_dates,
        ]
    ):
        logging.info("没有新数据需要处理")
        exit(0)

    log_dates("总量类", total_dates)
    log_dates("内容事件类", content_dates)
    log_dates("下载类", download_dates)
    log_dates("时长频次类", frequency_dates)
    log_dates("投资人类", investor_dates)
    log_dates("留存类", retention_dates)

    if download_dates:
        run_ads_daily_new(download_dates)
        logging.info("-" * 50)

    if total_dates:
        run_ads_daily_total(total_dates)
        logging.info("-" * 50)

    if content_dates:
        run_ads_daily_tryon_confirm(content_dates)
        logging.info("-" * 50)

        run_ads_daily_tryon_add_cart_conversion(content_dates)
        logging.info("-" * 50)

        run_ads_daily_post_tryon_confirm(content_dates)
        logging.info("-" * 50)

        run_ads_daily_product_tryon_performance(content_dates)
        logging.info("-" * 50)

    if frequency_dates:
        run_ads_daily_user_duration_frequency(frequency_dates)
        logging.info("-" * 50)

    if investor_dates:
        run_ads_daily_investor(investor_dates)
        logging.info("-" * 50)

    if retention_dates:
        run_ads_daily_retention_user_feature(retention_dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"ADS ETL 完成, 耗时: {elapsed:.1f} 秒")
