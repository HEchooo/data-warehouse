import logging
from datetime import date

from google.cloud import bigquery

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
client = bigquery.Client(project=PROJECT_ID)


def dates_to_sql_list(dates):
    return ", ".join([f"'{d}'" for d in dates])


def dates_to_array_sql(dates):
    return ", ".join([f"DATE '{d}'" for d in dates])


def event_ts_expr():
    """
    将 logAt_timestamp 先按 prop_timezone 解释为真实时间，再转成 UTC TIMESTAMP。
    logAt_timestamp 存的是“本地时间字面值”，不能直接当 UTC 使用。

    复用 ads_daily_content_performance 的口径。
    """
    return (
        "TIMESTAMP(" "DATETIME(logAt_timestamp, 'UTC'), " "COALESCE(NULLIF(prop_timezone, ''), 'UTC')" ")"
    )


def run_ads_daily_user_duration_frequency(dates: list[date]):
    """
    用户时长与访问频次（日粒度，按 user_segment 预聚合）:
    - user_segment: 全量设备 / 游客 / 已登录用户
    - dau_uv: 日活 UV（visitor_id）
    - total_duration_min: 总使用时长（分钟，仅 app_launch，args_session_duration 去重求和）
    - avg_duration_min: 人均使用时长（分钟）
    - app_launch_count: 启动次数（总，仅 app_launch，duration_event_id 去重计数）
    - avg_visit_freq: 人均访问频次（app_launch_count / dau_uv）
    - next_day_retention_rate / day_7_retention_rate / day_30_retention_rate: 留存率（cohort 跟随 visitor_id 口径）
    - mau_uv: 当月月活（自然月，dt 所属月份去重 visitor_id）
    - dau_mau: DAU/MAU

    使用多伦多时间（America/Toronto）。
    """

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    min_date = min(dates)
    max_date = max(dates)
    event_ts = event_ts_expr()

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_user_duration_frequency`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ads_daily_user_duration_frequency`
    (dt, user_segment, dau_uv,
     total_duration_min, avg_duration_min,
     app_launch_count, avg_visit_freq,
     next_day_retention_rate, day_7_retention_rate, day_30_retention_rate,
     mau_uv, dau_mau,
     update_time)
    WITH
    -- 待刷新的日期列表。
    date_list AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 固定产出三类用户分群，保证每个日期都有完整维度行。
    segment_list AS (
        SELECT '全量设备' AS user_segment
        UNION ALL
        SELECT '游客' AS user_segment
        UNION ALL
        SELECT '已登录用户' AS user_segment
    ),
    -- 拉取事件明细，并一次性准备三种分群各自的统计主体。
    base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            event_name,
            NULLIF(prop_device_id, '') AS all_device_id,
            IF(
                prop_user_id IS NULL OR prop_user_id = '',
                NULLIF(prop_device_id, ''),
                NULL
            ) AS guest_device_id,
            IF(
                prop_user_id IS NOT NULL AND prop_user_id != '',
                prop_user_id,
                NULL
            ) AS logged_in_user_id,
            COALESCE(raw_event_id, hash_id) AS duration_event_id,
            CAST(args_session_duration AS FLOAT64) AS duration_ms
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE(logAt_timestamp) BETWEEN DATE_SUB(DATE '{min_date}', INTERVAL 30 DAY)
            AND DATE_ADD(DATE '{max_date}', INTERVAL 31 DAY)
            AND DATE({event_ts}, '{TORONTO_TZ}') BETWEEN DATE_TRUNC(DATE '{min_date}', MONTH)
                AND LEAST(
                    DATE_ADD(DATE '{max_date}', INTERVAL 30 DAY),
                    CURRENT_DATE('{TORONTO_TZ}')
                )
    ),
    -- 将同一份事件展开到三类分群，统一成“当前分群下的统计主体”字段。
    segmented_base AS (
        SELECT
            dt,
            '全量设备' AS user_segment,
            event_name,
            all_device_id AS subject_id,
            duration_event_id,
            duration_ms
        FROM base
        WHERE all_device_id IS NOT NULL

        UNION ALL

        SELECT
            dt,
            '游客' AS user_segment,
            event_name,
            guest_device_id AS subject_id,
            duration_event_id,
            duration_ms
        FROM base
        WHERE guest_device_id IS NOT NULL

        UNION ALL

        SELECT
            dt,
            '已登录用户' AS user_segment,
            event_name,
            logged_in_user_id AS subject_id,
            duration_event_id,
            duration_ms
        FROM base
        WHERE logged_in_user_id IS NOT NULL
    ),
    -- 取每天每个分群的活跃主体集合，作为 UV、留存、MAU 的统一底座。
    active_visitors AS (
        SELECT DISTINCT
            dt,
            user_segment,
            subject_id
        FROM segmented_base
        WHERE subject_id IS NOT NULL
    ),
    -- 统计每天每个分群的日活 UV。
    dau AS (
        SELECT
            d.dt,
            s.user_segment,
            COUNT(DISTINCT a.subject_id) AS dau_uv
        FROM date_list d
        CROSS JOIN segment_list s
        LEFT JOIN active_visitors a
            ON a.dt = d.dt
            AND a.user_segment = s.user_segment
        GROUP BY d.dt, s.user_segment
    ),
    -- 对 app_launch 事件按分群和原始事件标识去重，避免停留时长与启动次数重复计算。
    app_launch_dedup AS (
        SELECT
            dt,
            user_segment,
            duration_event_id,
            MAX(duration_ms) AS duration_ms
        FROM segmented_base
        WHERE event_name = 'app_launch'
            AND subject_id IS NOT NULL
            AND duration_event_id IS NOT NULL
            AND duration_ms IS NOT NULL
        GROUP BY dt, user_segment, duration_event_id
    ),
    -- 汇总每天每个分群的启动次数与总使用时长。
    launch_and_duration AS (
        SELECT
            dt,
            user_segment,
            COUNT(*) AS app_launch_count,
            SUM(duration_ms) / 60000.0 AS total_duration_min
        FROM app_launch_dedup
        GROUP BY dt, user_segment
    ),
    -- 计算每天每个分群的 1/7/30 日留存分子分母。
    retention AS (
        SELECT
            d.dt,
            s.user_segment,
            COUNT(DISTINCT a0.subject_id) AS active_uv,
            COUNT(DISTINCT a1.subject_id) AS retained_1d_uv,
            COUNT(DISTINCT a7.subject_id) AS retained_7d_uv,
            COUNT(DISTINCT a30.subject_id) AS retained_30d_uv
        FROM date_list d
        CROSS JOIN segment_list s
        LEFT JOIN active_visitors a0
            ON a0.dt = d.dt
            AND a0.user_segment = s.user_segment
        LEFT JOIN active_visitors a1
            ON a1.subject_id = a0.subject_id
            AND a1.user_segment = s.user_segment
            AND a1.dt = DATE_ADD(d.dt, INTERVAL 1 DAY)
        LEFT JOIN active_visitors a7
            ON a7.subject_id = a0.subject_id
            AND a7.user_segment = s.user_segment
            AND a7.dt = DATE_ADD(d.dt, INTERVAL 7 DAY)
        LEFT JOIN active_visitors a30
            ON a30.subject_id = a0.subject_id
            AND a30.user_segment = s.user_segment
            AND a30.dt = DATE_ADD(d.dt, INTERVAL 30 DAY)
        GROUP BY d.dt, s.user_segment
    ),
    -- 统计每天每个分群对应自然月的月活 UV。
    mau AS (
        SELECT
            d.dt,
            s.user_segment,
            COUNT(DISTINCT a.subject_id) AS mau_uv
        FROM date_list d
        CROSS JOIN segment_list s
        LEFT JOIN active_visitors a
            ON a.user_segment = s.user_segment
            AND a.dt BETWEEN DATE_TRUNC(d.dt, MONTH) AND LEAST(
                DATE_SUB(DATE_ADD(DATE_TRUNC(d.dt, MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY),
                CURRENT_DATE('{TORONTO_TZ}')
            )
        GROUP BY d.dt, s.user_segment
    )
    SELECT
        d.dt,
        s.user_segment,
        COALESCE(u.dau_uv, 0) AS dau_uv,

        CAST(COALESCE(ld.total_duration_min, 0) AS NUMERIC) AS total_duration_min,
        CAST(CASE
            WHEN COALESCE(u.dau_uv, 0) = 0 THEN 0
            ELSE ROUND(CAST(COALESCE(ld.total_duration_min, 0) AS NUMERIC) / CAST(u.dau_uv AS NUMERIC), 4)
        END AS NUMERIC) AS avg_duration_min,

        COALESCE(ld.app_launch_count, 0) AS app_launch_count,
        CAST(CASE
            WHEN COALESCE(u.dau_uv, 0) = 0 THEN 0
            ELSE ROUND(CAST(COALESCE(ld.app_launch_count, 0) AS NUMERIC) / CAST(u.dau_uv AS NUMERIC), 4)
        END AS NUMERIC) AS avg_visit_freq,

        CAST(CASE
            WHEN COALESCE(r.active_uv, 0) = 0 THEN 0
            WHEN DATE_ADD(d.dt, INTERVAL 1 DAY) > DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY) THEN NULL
            ELSE ROUND(CAST(COALESCE(r.retained_1d_uv, 0) AS NUMERIC) / CAST(r.active_uv AS NUMERIC), 4)
        END AS NUMERIC) AS next_day_retention_rate,

        CAST(CASE
            WHEN COALESCE(r.active_uv, 0) = 0 THEN 0
            WHEN DATE_ADD(d.dt, INTERVAL 7 DAY) > DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY) THEN NULL
            ELSE ROUND(CAST(COALESCE(r.retained_7d_uv, 0) AS NUMERIC) / CAST(r.active_uv AS NUMERIC), 4)
        END AS NUMERIC) AS day_7_retention_rate,

        CAST(CASE
            WHEN COALESCE(r.active_uv, 0) = 0 THEN 0
            WHEN DATE_ADD(d.dt, INTERVAL 30 DAY) > DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY) THEN NULL
            ELSE ROUND(CAST(COALESCE(r.retained_30d_uv, 0) AS NUMERIC) / CAST(r.active_uv AS NUMERIC), 4)
        END AS NUMERIC) AS day_30_retention_rate,

        COALESCE(m.mau_uv, 0) AS mau_uv,
        CAST(CASE
            WHEN COALESCE(m.mau_uv, 0) = 0 THEN 0
            ELSE ROUND(CAST(COALESCE(u.dau_uv, 0) AS NUMERIC) / CAST(m.mau_uv AS NUMERIC), 4)
        END AS NUMERIC) AS dau_mau,

        CURRENT_TIMESTAMP() AS update_time
    FROM date_list d
    CROSS JOIN segment_list s
    LEFT JOIN dau u
        ON u.dt = d.dt
        AND u.user_segment = s.user_segment
    LEFT JOIN launch_and_duration ld
        ON ld.dt = d.dt
        AND ld.user_segment = s.user_segment
    LEFT JOIN retention r
        ON r.dt = d.dt
        AND r.user_segment = s.user_segment
    LEFT JOIN mau m
        ON m.dt = d.dt
        AND m.user_segment = s.user_segment;
    """

    logging.info("开始处理: ads_daily_user_duration_frequency")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_user_duration_frequency 刷新完成, 处理日期: {dates}")
