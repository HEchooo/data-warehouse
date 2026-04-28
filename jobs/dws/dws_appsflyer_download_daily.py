from datetime import datetime, timedelta, timezone
import logging
from zoneinfo import ZoneInfo

from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
APPSFLYER_REBUILD_DAYS = 30
client = bigquery.Client(project=PROJECT_ID)


def dates_to_sql_list(dates):
    return ", ".join([f"'{d}'" for d in dates])


def get_appsflyer_dates_to_process(rebuild_days=APPSFLYER_REBUILD_DAYS):
    end_date = datetime.now(ZoneInfo(TORONTO_TZ)).date()
    start_date = end_date - timedelta(days=rebuild_days - 1)
    return [start_date + timedelta(days=offset) for offset in range(rebuild_days)]


def run_dws_appsflyer_download_daily(dates):
    """
    聚合可复用的 AppsFlyer 新增下载事实。

    粒度：
    - dt × platform
    """
    if not dates:
        logging.info("dws_appsflyer_download_daily 无待处理日期，跳过")
        return

    dates_str = dates_to_sql_list(dates)
    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.dws_appsflyer_download_daily`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.dws_appsflyer_download_daily`
    (
        dt,
        platform,
        new_download_count,
        update_time
    )
    WITH
    -- 本批待处理日期列表。
    metric_dates AS (
        SELECT DATE(dt_text) AS dt
        FROM UNNEST([{dates_str}]) AS dt_text
    ),
    -- 固定保留 AppsFlyer 已接入的平台，确保缺数日期也能输出 0 值。
    platform_list AS (
        SELECT 'iOS' AS platform
        UNION ALL
        SELECT 'Android' AS platform
    ),
    -- 汇总 AppsFlyer 原始下载到日期 × 平台。
    daily_download AS (
        SELECT
            dt,
            platform,
            CAST(SUM(installs) AS INT64) AS new_download_count
        FROM `{PROJECT_ID}.{DATASET_ID}.ods_appsflyer_download`
        WHERE dt IN ({dates_str})
        GROUP BY dt, platform
    )
    SELECT
        d.dt,
        p.platform,
        COALESCE(a.new_download_count, 0) AS new_download_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM metric_dates d
    CROSS JOIN platform_list p
    LEFT JOIN daily_download a
        ON d.dt = a.dt
        AND p.platform = a.platform;
    """
    logging.info("开始处理: dws_appsflyer_download_daily")
    job = client.query(query)
    job.result()
    logging.info(f"dws_appsflyer_download_daily 刷新完成, 处理日期: {dates}")


if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)

    dates = get_appsflyer_dates_to_process()
    if not dates:
        logging.info("没有 AppsFlyer 下载数据需要处理")
        exit(0)

    logging.info(f"AppsFlyer 下载待处理日期: {dates}")
    run_dws_appsflyer_download_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"dws_appsflyer_download_daily 执行完成, 耗时: {elapsed:.1f} 秒")
