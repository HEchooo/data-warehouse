import logging

from google.cloud import bigquery

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
client = bigquery.Client(project=PROJECT_ID)


def dates_to_sql_list(dates):
    return ", ".join([f"'{d}'" for d in dates])


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
    dates_str = dates_to_sql_list(dates)

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
    logging.info("开始处理: ads_daily_total")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_total 刷新完成, 处理日期: {dates}")
