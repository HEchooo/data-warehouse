from google.cloud import bigquery
from datetime import datetime, timezone
import logging

# 配置日志记录
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# 项目和数据集配置
PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
DOWNLOAD_LOOKBACK_DAYS = 90
client = bigquery.Client(project=PROJECT_ID)


def get_dates_to_process():
    """
    获取需要刷新的下载日期（近 N 天，T+1）。
    下载来源:
    - ods_ios_download.begin_date
    - ods_android_download.date
    使用多伦多时间（America/Toronto）。
    """
    query = f"""
    WITH recent_ios_dates AS (
        SELECT DISTINCT begin_date AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.ods_ios_download`
        WHERE begin_date BETWEEN DATE_SUB(CURRENT_DATE('America/Toronto'), INTERVAL {DOWNLOAD_LOOKBACK_DAYS} DAY)
            AND DATE_SUB(CURRENT_DATE('America/Toronto'), INTERVAL 1 DAY)
    ),
    recent_android_dates AS (
        SELECT DISTINCT date AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.ods_android_download`
        WHERE date BETWEEN DATE_SUB(CURRENT_DATE('America/Toronto'), INTERVAL {DOWNLOAD_LOOKBACK_DAYS} DAY)
            AND DATE_SUB(CURRENT_DATE('America/Toronto'), INTERVAL 1 DAY)
    )
    SELECT dt
    FROM (
        SELECT dt FROM recent_ios_dates
        UNION DISTINCT
        SELECT dt FROM recent_android_dates
    )
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dwd_download(dates):
    """
    统一下载明细口径到 dwd_download:
    - iOS: product_type_identifier = 1 的 units
    - Android: daily_device_installs
    """
    dates_str = ", ".join([f"'{d}'" for d in dates])

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.dwd_download`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.dwd_download`
    (dt, platform, country_code, new_download_count, source, update_time)
    WITH
    ios_download AS (
        SELECT
            begin_date AS dt,
            'iOS' AS platform,
            country_code,
            CAST(SUM(units) AS INT64) AS new_download_count,
            'app_store' AS source
        FROM `{PROJECT_ID}.{DATASET_ID}.ods_ios_download`
        WHERE begin_date IN ({dates_str})
            AND product_type_identifier = 1
        GROUP BY begin_date, country_code
    ),
    android_download AS (
        SELECT
            date AS dt,
            'Android' AS platform,
            country AS country_code,
            CAST(SUM(daily_device_installs) AS INT64) AS new_download_count,
            'google_play' AS source
        FROM `{PROJECT_ID}.{DATASET_ID}.ods_android_download`
        WHERE date IN ({dates_str})
        GROUP BY date, country
    )
    SELECT
        dt,
        platform,
        country_code,
        new_download_count,
        source,
        CURRENT_TIMESTAMP() AS update_time
    FROM ios_download

    UNION ALL

    SELECT
        dt,
        platform,
        country_code,
        new_download_count,
        source,
        CURRENT_TIMESTAMP() AS update_time
    FROM android_download;
    """
    logging.info("开始处理: dwd_download")
    job = client.query(query)
    job.result()
    logging.info(f"dwd_download 刷新完成, 处理日期: {dates}")


if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)

    dates = get_dates_to_process()
    if not dates:
        logging.info("没有下载数据需要处理")
        exit(0)

    logging.info(f"待处理下载日期: {dates}")
    run_dwd_download(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"DWD 下载 ETL 完成, 耗时: {elapsed:.1f} 秒")
