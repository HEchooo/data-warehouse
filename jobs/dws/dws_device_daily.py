from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from jobs.event_activity_rules import PASSIVE_ACTIVITY_EXCLUDED_EVENTS_SQL


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_device_daily"
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
CONTENT_EVENTS = (
    "'v_product_detail',"
    "'v_star_post_detail',"
    "'v_magazine_post_detail',"
    "'v_brand_post_detail',"
    "'v_kol_post_detail'"
)

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
    return (
        "TIMESTAMP("
        "DATETIME(logAt_timestamp, 'UTC'), "
        "COALESCE(NULLIF(prop_timezone, ''), 'UTC')"
        ")"
    )


def get_dws_device_daily_dates_to_process() -> list[date]:
    """
    按 dws_device_daily 自身水位判定待处理日期。

    规则：
    - 目标表不存在或为空时，全量初始化所有设备活跃日期
    - 目标表已存在时，只处理源表 update_time 晚于目标表最大 update_time 的日期
    """
    event_ts = event_ts_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE prop_device_id IS NOT NULL
            AND prop_device_id != ''
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
    WHERE prop_device_id IS NOT NULL
        AND prop_device_id != ''
        AND update_time > (
            SELECT COALESCE(MAX(update_time), TIMESTAMP('1970-01-01'))
            FROM `{target_table_id()}`
        )
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_device_daily(dates: list[date]) -> None:
    """
    产出设备日事实。

    粒度：
    - dt × prop_device_id × platform

    语义：
    - first_active_date / is_new_device 复用 dim_device_first_active
    - has_anonymous_event 下沉设备日匿名访问标记，供 ads_daily_new 直接消费
    - session_duration_sec / content_consume_count 保持原有去重口径
    """
    if not dates:
        logging.info("没有新的 dws_device_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    event_ts = event_ts_expr()

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        prop_device_id,
        platform,
        first_active_date,
        is_new_device,
        has_anonymous_event,
        content_consume_count,
        session_duration_sec,
        update_time
    )
    WITH
    -- 统一事件时间与设备基础字段。
    normalized AS (
        SELECT
            hash_id,
            {event_ts} AS event_ts_utc,
            event_name,
            prop_device_id,
            prop_user_id,
            prop_os,
            prop_url,
            raw_event_id,
            args_session_duration
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
    ),
    -- 截取本批日期内的事件明细。
    base AS (
        SELECT
            hash_id,
            event_ts_utc,
            DATE(event_ts_utc, '{TORONTO_TZ}') AS dt,
            event_name,
            prop_device_id,
            prop_user_id,
            prop_os,
            prop_url,
            raw_event_id,
            args_session_duration
        FROM normalized
        WHERE DATE(event_ts_utc, '{TORONTO_TZ}') IN ({dates_str})
            AND event_name NOT IN ({PASSIVE_ACTIVITY_EXCLUDED_EVENTS_SQL})
    ),
    -- 汇总每天活跃的设备键。
    active_devices AS (
        SELECT
            dt,
            prop_device_id,
            CASE
                WHEN prop_url IS NOT NULL AND prop_url != '' THEN 'h5'
                WHEN LOWER(prop_os) = 'ios' THEN 'iOS'
                WHEN LOWER(prop_os) IN ('android', 'harmony') THEN 'Android'
                ELSE 'unknown'
            END AS platform
        FROM base
        WHERE prop_device_id IS NOT NULL
            AND prop_device_id != ''
        GROUP BY dt, prop_device_id, platform
    ),
    -- 标记每天每设备是否出现过匿名事件。
    device_anonymous_flag AS (
        SELECT
            dt,
            prop_device_id,
            CASE
                WHEN prop_url IS NOT NULL AND prop_url != '' THEN 'h5'
                WHEN LOWER(prop_os) = 'ios' THEN 'iOS'
                WHEN LOWER(prop_os) IN ('android', 'harmony') THEN 'Android'
                ELSE 'unknown'
            END AS platform,
            MAX(
                CASE
                    WHEN prop_user_id IS NULL OR prop_user_id = '' THEN 1
                    ELSE 0
                END
            ) AS has_anonymous_event_int
        FROM base
        WHERE prop_device_id IS NOT NULL
            AND prop_device_id != ''
        GROUP BY dt, prop_device_id, platform
    ),
    -- 提取 app_launch 时长事件并做事件级去重。
    app_launch_events AS (
        SELECT
            dt,
            prop_device_id,
            CASE
                WHEN prop_url IS NOT NULL AND prop_url != '' THEN 'h5'
                WHEN LOWER(prop_os) = 'ios' THEN 'iOS'
                WHEN LOWER(prop_os) IN ('android', 'harmony') THEN 'Android'
                ELSE 'unknown'
            END AS platform,
            COALESCE(raw_event_id, hash_id) AS duration_event_id,
            CAST(args_session_duration AS FLOAT64) AS duration_ms
        FROM base
        WHERE prop_device_id IS NOT NULL
            AND prop_device_id != ''
            AND event_name = 'app_launch'
            AND args_session_duration IS NOT NULL
            AND COALESCE(raw_event_id, hash_id) IS NOT NULL
    ),
    -- 保留同一原始时长事件的唯一值。
    deduped_app_launch_events AS (
        SELECT
            dt,
            prop_device_id,
            platform,
            duration_event_id,
            MAX(duration_ms) AS duration_ms
        FROM app_launch_events
        GROUP BY dt, prop_device_id, platform, duration_event_id
    ),
    -- 提取内容消费事件并按 raw_event_id 去重。
    content_events AS (
        SELECT
            dt,
            prop_device_id,
            CASE
                WHEN prop_url IS NOT NULL AND prop_url != '' THEN 'h5'
                WHEN LOWER(prop_os) = 'ios' THEN 'iOS'
                WHEN LOWER(prop_os) IN ('android', 'harmony') THEN 'Android'
                ELSE 'unknown'
            END AS platform,
            raw_event_id
        FROM base
        WHERE prop_device_id IS NOT NULL
            AND prop_device_id != ''
            AND event_name IN ({CONTENT_EVENTS})
            AND raw_event_id IS NOT NULL
        GROUP BY dt, prop_device_id, platform, raw_event_id
    ),
    -- 汇总设备日内容消费次数。
    device_content_agg AS (
        SELECT
            dt,
            prop_device_id,
            platform,
            COUNT(*) AS content_consume_count
        FROM content_events
        GROUP BY dt, prop_device_id, platform
    ),
    -- 汇总设备日停留时长。
    device_duration_agg AS (
        SELECT
            dt,
            prop_device_id,
            platform,
            SUM(duration_ms) / 1000.0 AS total_duration_sec
        FROM deduped_app_launch_events
        GROUP BY dt, prop_device_id, platform
    )
    SELECT
        a.dt,
        a.prop_device_id,
        a.platform,
        f.first_active_date,
        (f.first_active_date = a.dt) AS is_new_device,
        COALESCE(an.has_anonymous_event_int, 0) = 1 AS has_anonymous_event,
        COALESCE(c.content_consume_count, 0) AS content_consume_count,
        COALESCE(d.total_duration_sec, 0) AS session_duration_sec,
        CURRENT_TIMESTAMP() AS update_time
    FROM active_devices a
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_device_first_active` f
        ON a.prop_device_id = f.prop_device_id
    LEFT JOIN device_anonymous_flag an
        ON a.dt = an.dt
        AND a.prop_device_id = an.prop_device_id
        AND a.platform = an.platform
    LEFT JOIN device_content_agg c
        ON a.dt = c.dt
        AND a.prop_device_id = c.prop_device_id
        AND a.platform = c.platform
    LEFT JOIN device_duration_agg d
        ON a.dt = d.dt
        AND a.prop_device_id = d.prop_device_id
        AND a.platform = d.platform;
    """
    logging.info("开始处理: dws_device_daily")
    client.query(query).result()
    logging.info(f"dws_device_daily 刷新完成, 处理日期: {dates}")


if __name__ == "__main__":
    log_path = configure_logging()
    start_time = datetime.now(timezone.utc)

    dates = get_dws_device_daily_dates_to_process()
    if not dates:
        logging.info("没有新的 dws_device_daily 数据需要处理")
        logging.info(f"日志文件: {log_path}")
        raise SystemExit(0)

    logging.info(f"dws_device_daily 待处理日期: {dates}")
    run_dws_device_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"dws_device_daily 执行完成, 耗时: {elapsed:.1f} 秒")
    logging.info(f"日志文件: {log_path}")
