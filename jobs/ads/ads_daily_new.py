import logging

from google.cloud import bigquery

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
client = bigquery.Client(project=PROJECT_ID)


def dates_to_sql_list(dates):
    return ", ".join([f"'{d}'" for d in dates])


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
    dates_str = dates_to_sql_list(dates)

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
    logging.info("开始处理: ads_daily_new")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_new 刷新完成, 处理日期: {dates}")
