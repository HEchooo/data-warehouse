from datetime import datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery

# 配置日志记录
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# 支持直接执行 jobs/ads/ads_daily/ads_daily.py 时导入同级包
ADS_ROOT = Path(__file__).resolve().parent.parent
if str(ADS_ROOT) not in sys.path:
    sys.path.insert(0, str(ADS_ROOT))

from ads_daily_new.ads_daily_new import run_ads_daily_new
from ads_daily_total.ads_daily_total import run_ads_daily_total
from ads_daily_content_performance.ads_daily_content_performance import (
    run_ads_daily_content_performance,
)

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
client = bigquery.Client(project=PROJECT_ID)


def get_dates_to_process():
    """
    找出 DWS 层有新数据但 ADS 层尚未处理的日期。
    同时包含前一天用于回刷次日留存率。
    同时纳入下载 DWS 与内容表现日报的增量日期。
    使用多伦多时间（America/Toronto）。
    """
    query = f"""
    -- 事件新日期：DWS 有更新但 ADS 尚未处理
    SELECT DISTINCT dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
    WHERE update_time > (
        SELECT COALESCE(MAX(update_time), TIMESTAMP('1970-01-01'))
        FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_total`
    )
    AND dt <= DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY)

    UNION DISTINCT

    -- 前一天：用于回刷次日留存率
    SELECT DISTINCT DATE_SUB(dt, INTERVAL 1 DAY)
    FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
    WHERE update_time > (
        SELECT COALESCE(MAX(update_time), TIMESTAMP('1970-01-01'))
        FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_total`
    )
    AND dt <= DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY)

    UNION DISTINCT

    -- 下载新日期：下载 DWS 有更新但 ads_daily_new 尚未处理
    SELECT DISTINCT dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dws_download_daily`
    WHERE update_time > (
        SELECT COALESCE(MAX(update_time), TIMESTAMP('1970-01-01'))
        FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_new`
    )
    AND dt <= DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY)

    UNION DISTINCT

    -- 内容表现新日期：DWS 有更新但内容 ADS 尚未处理
    SELECT DISTINCT dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
    WHERE update_time > (
        SELECT COALESCE(MAX(update_time), TIMESTAMP('1970-01-01'))
        FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_content_performance`
    )
    AND dt <= DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY)

    ORDER BY dt
    """
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

    run_ads_daily_total(dates)
    logging.info("-" * 50)

    run_ads_daily_content_performance(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"ADS ETL 完成, 耗时: {elapsed:.1f} 秒")
