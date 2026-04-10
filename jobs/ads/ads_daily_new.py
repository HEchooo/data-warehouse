import logging

from google.cloud import bigquery

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
client = bigquery.Client(project=PROJECT_ID)


def dates_to_sql_list(dates):
    return ", ".join([f"'{d}'" for d in dates])


def event_ts_expr():
    """
    将 logAt_timestamp 先按 prop_timezone 解释为真实时间，再转成 UTC TIMESTAMP。
    logAt_timestamp 存的是“本地时间字面值”，不能直接当 UTC 使用。
    """
    return (
        "TIMESTAMP("
        "DATETIME(logAt_timestamp, 'UTC'), "
        "COALESCE(NULLIF(prop_timezone, ''), 'UTC')"
        ")"
    )


def platform_expr():
    return (
        "CASE "
        "WHEN prop_url IS NOT NULL AND prop_url != '' THEN 'h5' "
        "WHEN LOWER(prop_os) = 'ios' THEN 'iOS' "
        "WHEN LOWER(prop_os) IN ('android', 'harmony') THEN 'Android' "
        "ELSE 'unknown' "
        "END"
    )


def run_ads_daily_new(dates):
    """
    新增报表:
    - device_count: 新增设备数 (is_new_device = TRUE)
    - user_count: 新增注册用户数 (is_new_user = TRUE)
    - new_user_login_rate: 新增注册用户 / 新增设备
    - anonymous_device_rate: 未登录设备数 / 当日总设备数
    - anonymous_old_device_rate: 未登录老设备数 / 当日总设备数
    - avg_duration_sec: 新增设备的平均停留时长
    - avg_content_consume_count: 新增设备的人均内容消费数
    - next_day_retention_rate: 新增设备的次日留存率
    - new_download_count: 新增下载量（来自 dws_download_daily）
    使用多伦多时间（America/Toronto）。
    """
    dates_str = dates_to_sql_list(dates)
    event_ts = event_ts_expr()
    platform_case = platform_expr()

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_new`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ads_daily_new`
    (dt, platform, device_count, user_count, new_user_login_rate,
     anonymous_device_rate, anonymous_old_device_rate, avg_duration_sec,
     avg_content_consume_count, next_day_retention_rate, new_download_count, update_time)
    WITH
    -- 新增设备指标：复用设备日表中的新增设备、人均时长和人均内容消费。
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
    -- 新增注册用户指标：复用用户日表中的新增注册用户数。
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
    -- 新增设备次留：以新增设备 cohort 关联次日同平台活跃。
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
    -- 全量活跃设备：为未登录占比提供统一分母。
    total_device_metrics AS (
        SELECT
            dt,
            platform,
            COUNT(DISTINCT prop_device_id) AS total_device_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
        WHERE dt IN ({dates_str})
        GROUP BY dt, platform
    ),
    -- 设备登录态基础表：按天、平台、设备聚合是否出现过 user_id 为空的事件。
    device_login_base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            {platform_case} AS platform,
            prop_device_id,
            MAX(
                CASE
                    WHEN prop_user_id IS NULL OR prop_user_id = '' THEN 1
                    ELSE 0
                END
            ) AS has_anonymous_event
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND prop_device_id IS NOT NULL
            AND prop_device_id != ''
        GROUP BY dt, platform, prop_device_id
    ),
    -- 未登录设备指标：统计出现过匿名事件的设备数，并拆出其中的新增设备。
    anonymous_device_metrics AS (
        SELECT
            b.dt,
            b.platform,
            COUNT(DISTINCT b.prop_device_id) AS anonymous_device_count,
            COUNT(DISTINCT IF(f.first_active_date = b.dt, b.prop_device_id, NULL))
                AS anonymous_new_device_count
        FROM device_login_base b
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_device_first_active` f
            ON b.prop_device_id = f.prop_device_id
        WHERE b.has_anonymous_event = 1
        GROUP BY b.dt, b.platform
    ),
    -- 下载指标：沿用下载日表汇总新增下载量。
    download_metrics AS (
        SELECT
            dt,
            platform,
            CAST(SUM(new_download_count) AS INT64) AS new_download_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_download_daily`
        WHERE dt IN ({dates_str})
        GROUP BY dt, platform
    ),
    -- 指标主键：收敛所有来源的 dt × platform，避免漏行。
    metric_keys AS (
        SELECT dt, platform FROM new_device_metrics
        UNION DISTINCT
        SELECT dt, platform FROM new_user_metrics
        UNION DISTINCT
        SELECT dt, platform FROM new_device_retention
        UNION DISTINCT
        SELECT dt, platform FROM total_device_metrics
        UNION DISTINCT
        SELECT dt, platform FROM anonymous_device_metrics
        UNION DISTINCT
        SELECT dt, platform FROM download_metrics
    )
    SELECT
        k.dt AS dt,
        k.platform AS platform,
        COALESCE(d.device_count, 0) AS device_count,
        COALESCE(u.user_count, 0) AS user_count,
        CAST(
            COALESCE(
                ROUND(
                    SAFE_DIVIDE(COALESCE(u.user_count, 0), COALESCE(d.device_count, 0)),
                    4
                ),
                0
            ) AS NUMERIC
        ) AS new_user_login_rate,
        CAST(
            COALESCE(
                ROUND(
                    SAFE_DIVIDE(
                        COALESCE(a.anonymous_device_count, 0),
                        COALESCE(t.total_device_count, 0)
                    ),
                    4
                ),
                0
            ) AS NUMERIC
        ) AS anonymous_device_rate,
        CAST(
            COALESCE(
                ROUND(
                    SAFE_DIVIDE(
                        COALESCE(a.anonymous_device_count, 0) - COALESCE(a.anonymous_new_device_count, 0),
                        COALESCE(t.total_device_count, 0)
                    ),
                    4
                ),
                0
            ) AS NUMERIC
        ) AS anonymous_old_device_rate,
        COALESCE(d.avg_duration_sec, 0) AS avg_duration_sec,
        COALESCE(d.avg_content_consume_count, 0) AS avg_content_consume_count,
        CAST(CASE
            WHEN COALESCE(r.new_count, 0) = 0 THEN 0
            ELSE ROUND(r.retained_count / r.new_count, 4)
        END AS NUMERIC) AS next_day_retention_rate,
        COALESCE(dl.new_download_count, 0) AS new_download_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM metric_keys k
    LEFT JOIN new_device_metrics d
        ON k.dt = d.dt AND k.platform = d.platform
    LEFT JOIN new_user_metrics u
        ON k.dt = u.dt AND k.platform = u.platform
    LEFT JOIN new_device_retention r
        ON k.dt = r.dt AND k.platform = r.platform
    LEFT JOIN total_device_metrics t
        ON k.dt = t.dt AND k.platform = t.platform
    LEFT JOIN anonymous_device_metrics a
        ON k.dt = a.dt AND k.platform = a.platform
    LEFT JOIN download_metrics dl
        ON k.dt = dl.dt AND k.platform = dl.platform;
    """
    logging.info("开始处理: ads_daily_new")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_new 刷新完成, 处理日期: {dates}")
