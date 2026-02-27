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
client = bigquery.Client(project=PROJECT_ID)


def get_dates_to_process():
    """
    找出 DWS 层有新数据但 ADS 层尚未处理的日期。
    同时包含前一天用于回刷次日留存率。
    同时纳入下载 DWS 的增量日期。
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
    AND dt <= DATE_SUB(CURRENT_DATE('America/Toronto'), INTERVAL 1 DAY)

    UNION DISTINCT

    -- 前一天：用于回刷次日留存率
    SELECT DISTINCT DATE_SUB(dt, INTERVAL 1 DAY)
    FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
    WHERE update_time > (
        SELECT COALESCE(MAX(update_time), TIMESTAMP('1970-01-01'))
        FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_total`
    )
    AND dt <= DATE_SUB(CURRENT_DATE('America/Toronto'), INTERVAL 1 DAY)

    UNION DISTINCT

    -- 下载新日期：下载 DWS 有更新但 ads_daily_new 尚未处理
    SELECT DISTINCT dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dws_download_daily`
    WHERE update_time > (
        SELECT COALESCE(MAX(update_time), TIMESTAMP('1970-01-01'))
        FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_new`
    )
    AND dt <= DATE_SUB(CURRENT_DATE('America/Toronto'), INTERVAL 1 DAY)

    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_ads_daily_new(dates):
    """
    新增报表:
    - device_count: 新增设备数 (is_new_device = TRUE)
    - user_count: 新增注册用户数 (is_new_user = TRUE)
    - avg_duration_sec: 新增设备的平均停留时长
    - avg_content_consume_count: 新增设备的人均内容消费数
    - next_day_retention_rate: 新增设备的次日留存率
    - new_download_count: 新增下载量（来自 dws_download_daily）
    使用多伦多时间（America/Toronto）。
    """
    dates_str = ", ".join([f"'{d}'" for d in dates])

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_new`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ads_daily_new`
    (dt, platform, device_count, user_count,
     avg_duration_sec, avg_content_consume_count, next_day_retention_rate, new_download_count,
     update_time)
    WITH
    new_device_metrics AS (
        SELECT
            dt,
            platform,
            COUNT(DISTINCT prop_device_id) AS device_count,
            CAST(SUM(session_duration_sec) / COUNT(DISTINCT prop_device_id) AS NUMERIC) AS avg_duration_sec,
            CAST(SUM(content_consume_count) / COUNT(DISTINCT prop_device_id) AS NUMERIC) AS avg_content_consume_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
        WHERE dt IN ({dates_str})
            AND is_new_device = TRUE
        GROUP BY dt, platform
    ),
    new_user_metrics AS (
        SELECT
            dt,
            platform,
            COUNT(DISTINCT prop_user_id) AS user_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        WHERE dt IN ({dates_str})
            AND is_new_user = TRUE
        GROUP BY dt, platform
    ),
    new_device_retention AS (
        SELECT
            t.dt,
            t.platform,
            COUNT(DISTINCT t.prop_device_id) AS new_count,
            COUNT(DISTINCT n.prop_device_id) AS retained_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily` t
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dws_device_daily` n
            ON t.prop_device_id = n.prop_device_id
            AND n.dt = DATE_ADD(t.dt, INTERVAL 1 DAY)
            AND n.platform = t.platform
        WHERE t.dt IN ({dates_str})
            AND t.is_new_device = TRUE
        GROUP BY t.dt, t.platform
    ),
    download_metrics AS (
        SELECT
            dt,
            platform,
            CAST(SUM(new_download_count) AS INT64) AS new_download_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_download_daily`
        WHERE dt IN ({dates_str})
        GROUP BY dt, platform
    ),
    metric_keys AS (
        SELECT dt, platform FROM new_device_metrics
        UNION DISTINCT
        SELECT dt, platform FROM new_user_metrics
        UNION DISTINCT
        SELECT dt, platform FROM new_device_retention
        UNION DISTINCT
        SELECT dt, platform FROM download_metrics
    )
    SELECT
        k.dt,
        k.platform,
        COALESCE(d.device_count, 0),
        COALESCE(u.user_count, 0),
        COALESCE(d.avg_duration_sec, 0),
        COALESCE(d.avg_content_consume_count, 0),
        CAST(CASE
            WHEN COALESCE(r.new_count, 0) = 0 THEN 0
            ELSE ROUND(r.retained_count / r.new_count, 4)
        END AS NUMERIC),
        COALESCE(dl.new_download_count, 0),
        CURRENT_TIMESTAMP()
    FROM metric_keys k
    LEFT JOIN new_device_metrics d
        ON k.dt = d.dt AND k.platform = d.platform
    LEFT JOIN new_user_metrics u
        ON k.dt = u.dt AND k.platform = u.platform
    LEFT JOIN new_device_retention r
        ON k.dt = r.dt AND k.platform = r.platform
    LEFT JOIN download_metrics dl
        ON k.dt = dl.dt AND k.platform = dl.platform;
    """
    logging.info(f"开始处理: ads_daily_new")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_new 刷新完成, 处理日期: {dates}")


def run_ads_daily_total(dates):
    """
    总量报表:
    - device_count: 活跃设备数
    - user_count: 活跃注册用户数
    - avg_duration_sec: 活跃设备的平均停留时长
    - avg_content_consume_count: 活跃设备的人均内容消费数
    - next_day_retention_rate: 活跃设备的次日留存率
    使用多伦多时间（America/Toronto）。
    """
    dates_str = ", ".join([f"'{d}'" for d in dates])

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_total`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ads_daily_total`
    (dt, platform, device_count, user_count,
     avg_duration_sec, avg_content_consume_count, next_day_retention_rate,
     update_time)
    WITH
    device_metrics AS (
        SELECT
            dt,
            platform,
            COUNT(DISTINCT prop_device_id) AS device_count,
            CAST(SUM(session_duration_sec) / COUNT(DISTINCT prop_device_id) AS NUMERIC) AS avg_duration_sec,
            CAST(SUM(content_consume_count) / COUNT(DISTINCT prop_device_id) AS NUMERIC) AS avg_content_consume_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
        WHERE dt IN ({dates_str})
        GROUP BY dt, platform
    ),
    user_metrics AS (
        SELECT
            dt,
            platform,
            COUNT(DISTINCT prop_user_id) AS user_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        WHERE dt IN ({dates_str})
        GROUP BY dt, platform
    ),
    device_retention AS (
        SELECT
            t.dt,
            t.platform,
            COUNT(DISTINCT t.prop_device_id) AS active_count,
            COUNT(DISTINCT n.prop_device_id) AS retained_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily` t
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dws_device_daily` n
            ON t.prop_device_id = n.prop_device_id
            AND n.dt = DATE_ADD(t.dt, INTERVAL 1 DAY)
            AND n.platform = t.platform
        WHERE t.dt IN ({dates_str})
        GROUP BY t.dt, t.platform
    )
    SELECT
        COALESCE(d.dt, u.dt, r.dt) AS dt,
        COALESCE(d.platform, u.platform, r.platform) AS platform,
        COALESCE(d.device_count, 0),
        COALESCE(u.user_count, 0),
        COALESCE(d.avg_duration_sec, 0),
        COALESCE(d.avg_content_consume_count, 0),
        CAST(CASE
            WHEN COALESCE(r.active_count, 0) = 0 THEN 0
            ELSE ROUND(r.retained_count / r.active_count, 4)
        END AS NUMERIC),
        CURRENT_TIMESTAMP()
    FROM device_metrics d
    FULL OUTER JOIN user_metrics u
        ON d.dt = u.dt AND d.platform = u.platform
    FULL OUTER JOIN device_retention r
        ON COALESCE(d.dt, u.dt) = r.dt AND COALESCE(d.platform, u.platform) = r.platform;
    """
    logging.info(f"开始处理: ads_daily_total")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_total 刷新完成, 处理日期: {dates}")


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

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"ADS ETL 完成, 耗时: {elapsed:.1f} 秒")
