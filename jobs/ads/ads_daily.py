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


def log_dates(label, dates):
    """统一打印每组待处理日期，空组也显式记录。"""
    logging.info(f"{label}待处理日期: {dates if dates else '[]'}")


if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)

    total_dates = get_total_dates_to_process()
    download_dates = get_download_dates_to_process()
    if not any([total_dates, download_dates]):
        logging.info("没有新数据需要处理")
        exit(0)

    log_dates("总量类", total_dates)
    log_dates("下载类", download_dates)

    if download_dates:
        run_ads_daily_new(download_dates)
        logging.info("-" * 50)

    if total_dates:
        run_ads_daily_total(total_dates)
        logging.info("-" * 50)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"ADS ETL 完成, 耗时: {elapsed:.1f} 秒")
