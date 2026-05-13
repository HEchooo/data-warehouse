from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_device_activity_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6

client = bigquery.Client(project=PROJECT_ID)


def configure_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{datetime.now().strftime('%Y-%m-%d')}_{TABLE_NAME}.log"

    logger = logging.getLogger()
    logger.handlers.clear()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return log_path


def dates_to_sql_list(dates: list[date]) -> str:
    return ", ".join([f"'{d}'" for d in dates])


def dates_to_array_sql(dates: list[date]) -> str:
    return ", ".join([f"DATE '{d}'" for d in dates])


def target_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


def target_table_exists() -> bool:
    try:
        client.get_table(target_table_id())
        return True
    except NotFound:
        return False


def get_target_row_count() -> int:
    if not target_table_exists():
        return 0

    query = f"""
    SELECT COUNT(*) AS row_count
    FROM `{target_table_id()}`
    """
    rows = list(client.query(query).result())
    return int(rows[0].row_count) if rows else 0


def event_ts_expr() -> str:
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


def get_device_activity_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    用户活动 DWS 直接依赖事件表，不复用共享水位。

    规则：
    - 目标表不存在或为空时，按事件表历史业务日全量初始化
    - 目标表已存在时：
      - 当天事件变化重刷当天分区
      - 次留 / 7 留 / 30 留分母分子回刷 `dt-1 / dt-7 / dt-30`
      - MAU 受同月任一活跃日影响，需要回刷目标表中同月已有分区
    """
    event_ts = event_ts_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    WITH
    -- 最近 6 小时变更的事件业务日。
    changed_event_dates AS (
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS source_dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
    ),
    -- 最近 6 小时首活维表变化的用户，可能影响该用户所有后续活跃日的新老标签。
    changed_first_active_users AS (
        SELECT
            prop_user_id,
            first_active_date
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_user_first_active`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
            AND first_active_date IS NOT NULL
    ),
    -- 首活变化影响的已登录用户活跃日期。
    first_active_impacted_dates AS (
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log` e
        JOIN changed_first_active_users u
            ON e.prop_user_id = u.prop_user_id
        WHERE DATE({event_ts}, '{TORONTO_TZ}') BETWEEN u.first_active_date
            AND CURRENT_DATE('{TORONTO_TZ}')
    ),
    -- 当天活跃与时长统计直接受源日期影响。
    direct_dates AS (
        SELECT source_dt AS dt
        FROM changed_event_dates
        UNION DISTINCT
        SELECT dt
        FROM first_active_impacted_dates
    ),
    -- 留存观察日的活跃变化会回推到更早 cohort 日期。
    retention_dates AS (
        SELECT DATE_SUB(source_dt, INTERVAL 1 DAY) AS dt
        FROM changed_event_dates
        UNION DISTINCT
        SELECT DATE_SUB(source_dt, INTERVAL 7 DAY) AS dt
        FROM changed_event_dates
        UNION DISTINCT
        SELECT DATE_SUB(source_dt, INTERVAL 30 DAY) AS dt
        FROM changed_event_dates
    ),
    -- MAU 是自然月口径，同月任一活跃变化都要回刷目标表中该月已有日期。
    month_existing_dates AS (
        SELECT DISTINCT t.dt
        FROM `{target_table_id()}` t
        JOIN (
            SELECT DISTINCT DATE_TRUNC(source_dt, MONTH) AS month_dt
            FROM changed_event_dates
        ) m
            ON DATE_TRUNC(t.dt, MONTH) = m.month_dt
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt FROM direct_dates
        UNION DISTINCT
        SELECT dt FROM retention_dates
        UNION DISTINCT
        SELECT dt FROM month_existing_dates
    )
    WHERE dt IS NOT NULL
        AND dt <= CURRENT_DATE('{TORONTO_TZ}')
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_device_activity_daily(dates: list[date]) -> None:
    """
    产出设备 / 游客 / 登录用户新老分群的日活动事实层。

    粒度：
    - dt × user_segment

    指标：
    - dau_uv：分群日活主体数
    - total_duration_min：app_launch 停留时长去重后汇总分钟数
    - app_launch_count：app_launch 去重后启动次数
    - retained_1d_uv / retained_7d_uv / retained_30d_uv：对应当日 cohort 留存分子
    - mau_uv：自然月 MAU
    """
    if not dates:
        logging.info("没有新的 dws_device_activity_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    min_date = min(dates)
    max_date = max(dates)
    event_ts = event_ts_expr()

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        user_segment,
        dau_uv,
        total_duration_min,
        app_launch_count,
        retained_1d_uv,
        retained_7d_uv,
        retained_30d_uv,
        mau_uv,
        update_time
    )
    WITH
    -- 当前批次待重算的日期列表。
    date_list AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 固定产出五类用户分群，保证每个日期都有完整维度行。
    segment_list AS (
        SELECT '全量设备' AS user_segment
        UNION ALL
        SELECT '游客' AS user_segment
        UNION ALL
        SELECT '已登录用户' AS user_segment
        UNION ALL
        SELECT '新用户' AS user_segment
        UNION ALL
        SELECT '老用户' AS user_segment
    ),
    -- 事件基础层：一次性准备设备、游客和登录用户统计主体。
    event_base AS (
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
        WHERE DATE(logAt_timestamp) BETWEEN DATE_TRUNC(DATE '{min_date}', MONTH)
            AND DATE_ADD(DATE '{max_date}', INTERVAL 31 DAY)
            AND DATE({event_ts}, '{TORONTO_TZ}') BETWEEN DATE_TRUNC(DATE '{min_date}', MONTH)
            AND LEAST(
                DATE_ADD(DATE '{max_date}', INTERVAL 30 DAY),
                CURRENT_DATE('{TORONTO_TZ}')
            )
    ),
    -- 首活补充层：复用用户级首活维表，不在当前窗口内现算首次日期。
    base AS (
        SELECT
            e.dt,
            e.event_name,
            e.all_device_id,
            e.guest_device_id,
            e.logged_in_user_id,
            f.first_active_date,
            e.duration_event_id,
            e.duration_ms
        FROM event_base e
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_user_first_active` f
            ON e.logged_in_user_id = f.prop_user_id
    ),
    -- 分群展开层：把同一份事件映射成各分群自己的统计主体。
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

        UNION ALL

        SELECT
            dt,
            '新用户' AS user_segment,
            event_name,
            logged_in_user_id AS subject_id,
            duration_event_id,
            duration_ms
        FROM base
        WHERE logged_in_user_id IS NOT NULL
            AND first_active_date = dt

        UNION ALL

        SELECT
            dt,
            '老用户' AS user_segment,
            event_name,
            logged_in_user_id AS subject_id,
            duration_event_id,
            duration_ms
        FROM base
        WHERE logged_in_user_id IS NOT NULL
            AND first_active_date < dt
    ),
    -- 活跃主体集合层：统一承接 DAU、当日 cohort 和 MAU 的主体集合。
    active_subjects AS (
        SELECT DISTINCT
            dt,
            user_segment,
            subject_id
        FROM segmented_base
        WHERE subject_id IS NOT NULL
    ),
    -- 留存观察层：新老用户未来留存只要求同一 user_id 再活跃，不要求未来仍属于当日分群。
    retention_active_subjects AS (
        SELECT
            dt,
            user_segment,
            subject_id
        FROM active_subjects
        WHERE user_segment IN ('全量设备', '游客', '已登录用户')

        UNION ALL

        SELECT
            dt,
            '新用户' AS user_segment,
            subject_id
        FROM active_subjects
        WHERE user_segment = '已登录用户'

        UNION ALL

        SELECT
            dt,
            '老用户' AS user_segment,
            subject_id
        FROM active_subjects
        WHERE user_segment = '已登录用户'
    ),
    -- 日活聚合层：产出每个日期和分群的 DAU。
    dau_metrics AS (
        SELECT
            d.dt,
            s.user_segment,
            COUNT(DISTINCT a.subject_id) AS dau_uv
        FROM date_list d
        CROSS JOIN segment_list s
        LEFT JOIN active_subjects a
            ON a.dt = d.dt
            AND a.user_segment = s.user_segment
        GROUP BY d.dt, s.user_segment
    ),
    -- app_launch 去重层：避免拆行导致启动次数和时长重复累计。
    app_launch_dedup AS (
        SELECT
            dt,
            user_segment,
            duration_event_id,
            MAX(duration_ms) AS duration_ms
        FROM segmented_base
        WHERE event_name = 'app_launch'
            AND duration_event_id IS NOT NULL
            AND duration_ms IS NOT NULL
        GROUP BY dt, user_segment, duration_event_id
    ),
    -- 启动与停留时长聚合层：产出启动次数和总使用时长。
    launch_metrics AS (
        SELECT
            dt,
            user_segment,
            COUNT(*) AS app_launch_count,
            CAST(SUM(duration_ms) / 60000.0 AS NUMERIC) AS total_duration_min
        FROM app_launch_dedup
        GROUP BY dt, user_segment
    ),
    -- 留存分子聚合层：把未来活跃回推成 1 / 7 / 30 日留存人数。
    retention_metrics AS (
        SELECT
            d.dt,
            s.user_segment,
            COUNT(DISTINCT a1.subject_id) AS retained_1d_uv,
            COUNT(DISTINCT a7.subject_id) AS retained_7d_uv,
            COUNT(DISTINCT a30.subject_id) AS retained_30d_uv
        FROM date_list d
        CROSS JOIN segment_list s
        LEFT JOIN active_subjects a0
            ON a0.dt = d.dt
            AND a0.user_segment = s.user_segment
        LEFT JOIN retention_active_subjects a1
            ON a1.user_segment = s.user_segment
            AND a1.subject_id = a0.subject_id
            AND a1.dt = DATE_ADD(d.dt, INTERVAL 1 DAY)
        LEFT JOIN retention_active_subjects a7
            ON a7.user_segment = s.user_segment
            AND a7.subject_id = a0.subject_id
            AND a7.dt = DATE_ADD(d.dt, INTERVAL 7 DAY)
        LEFT JOIN retention_active_subjects a30
            ON a30.user_segment = s.user_segment
            AND a30.subject_id = a0.subject_id
            AND a30.dt = DATE_ADD(d.dt, INTERVAL 30 DAY)
        GROUP BY d.dt, s.user_segment
    ),
    -- 自然月 MAU 聚合层：每个日期使用所在月统一月活口径。
    mau_metrics AS (
        SELECT
            d.dt,
            s.user_segment,
            COUNT(DISTINCT a.subject_id) AS mau_uv
        FROM date_list d
        CROSS JOIN segment_list s
        LEFT JOIN active_subjects a
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
        CAST(COALESCE(l.total_duration_min, 0) AS NUMERIC) AS total_duration_min,
        COALESCE(l.app_launch_count, 0) AS app_launch_count,
        COALESCE(r.retained_1d_uv, 0) AS retained_1d_uv,
        COALESCE(r.retained_7d_uv, 0) AS retained_7d_uv,
        COALESCE(r.retained_30d_uv, 0) AS retained_30d_uv,
        COALESCE(m.mau_uv, 0) AS mau_uv,
        CURRENT_TIMESTAMP() AS update_time
    FROM date_list d
    CROSS JOIN segment_list s
    LEFT JOIN dau_metrics u
        ON u.dt = d.dt
        AND u.user_segment = s.user_segment
    LEFT JOIN launch_metrics l
        ON l.dt = d.dt
        AND l.user_segment = s.user_segment
    LEFT JOIN retention_metrics r
        ON r.dt = d.dt
        AND r.user_segment = s.user_segment
    LEFT JOIN mau_metrics m
        ON m.dt = d.dt
        AND m.user_segment = s.user_segment;
    """
    logging.info("开始处理: dws_device_activity_daily")
    job = client.query(query)
    job.result()
    logging.info("dws_device_activity_daily 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_device_activity_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 dws_device_activity_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_device_activity_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dws_device_activity_daily 完成, 耗时: %.1f 秒", elapsed)
