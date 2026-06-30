from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
V3_DATASET_ID = "v3_decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_ai_fashion_tryon_okr_user_daily"
USER_DAILY_SOURCE_TABLE_NAME = "dws_user_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6

TRYON_TASK_TYPES = (
    "QUICK_TRY_ON",
    "TRY_ON",
    "OUTFIT",
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


def dates_to_array_sql(dates: list[date]) -> str:
    return ", ".join([f"DATE '{d}'" for d in dates])


def string_values_to_sql_list(values: tuple[str, ...]) -> str:
    return ", ".join([f"'{value}'" for value in values])


def target_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


def user_daily_source_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{USER_DAILY_SOURCE_TABLE_NAME}"


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


def service_ts_expr(field_name: str = "gmt_create") -> str:
    return f"TIMESTAMP({field_name}, 'UTC')"


def get_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    event_ts = event_ts_expr()
    service_ts = service_ts_expr()
    tryon_task_types_sql = string_values_to_sql_list(TRYON_TASK_TYPES)
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        WITH
        user_daily_dates AS (
            SELECT DISTINCT dt
            FROM `{user_daily_source_table_id()}`
            WHERE prop_user_id IS NOT NULL
                AND prop_user_id != ''
        ),
        event_dates AS (
            SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
            FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
            WHERE prop_user_id IS NOT NULL
                AND CAST(prop_user_id AS STRING) != ''
                AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
        ),
        tryon_dates AS (
            SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS dt
            FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
            WHERE task_type IN ({tryon_task_types_sql})
                AND DATE({service_ts}, '{TORONTO_TZ}') IS NOT NULL
        )
        SELECT DISTINCT dt
        FROM (
            SELECT dt FROM user_daily_dates
            UNION DISTINCT
            SELECT dt FROM event_dates
            UNION DISTINCT
            SELECT dt FROM tryon_dates
        )
        WHERE dt IS NOT NULL
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    WITH
    changed_user_daily_dates AS (
        SELECT DISTINCT dt
        FROM `{user_daily_source_table_id()}`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
    ),
    changed_event_dates AS (
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND prop_user_id IS NOT NULL
            AND CAST(prop_user_id AS STRING) != ''
            AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
    ),
    changed_tryon_dates AS (
        SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE {service_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND task_type IN ({tryon_task_types_sql})
            AND DATE({service_ts}, '{TORONTO_TZ}') IS NOT NULL
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt FROM changed_user_daily_dates
        UNION DISTINCT
        SELECT dt FROM changed_event_dates
        UNION DISTINCT
        SELECT dt FROM changed_tryon_dates
    )
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws(dates: list[date]) -> None:
    if not dates:
        logging.info("没有新的 %s 日期需要处理", TABLE_NAME)
        return

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    tryon_task_types_sql = string_values_to_sql_list(TRYON_TASK_TYPES)
    event_ts = event_ts_expr()
    service_ts = service_ts_expr()

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        user_id,
        tryon_task_count,
        tryon_success_duration_task_count,
        tryon_success_total_duration_ms,
        product_click_pv_raw,
        update_time
    )
    WITH
    -- 目标日期骨架。
    date_spine AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 活跃用户基座：复用 dws_user_daily，排除被动触达事件带来的伪活跃。
    active_users AS (
        SELECT DISTINCT
            dt,
            prop_user_id AS user_id
        FROM `{user_daily_source_table_id()}`
        WHERE dt IN ({dates_str})
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
    ),
    -- 事件基础层：统一 Toronto 切天、统一 user_id，仅用于商品点击 PV 计算。
    dwd_base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            event_name,
            CAST(prop_user_id AS STRING) AS user_id,
            NULLIF(product_code, '') AS product_code,
            raw_event_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND prop_user_id IS NOT NULL
            AND CAST(prop_user_id AS STRING) != ''
    ),
    -- 服务端试穿任务用户日聚合。
    service_tryon_user_daily AS (
        SELECT
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            COALESCE(
                NULLIF(JSON_VALUE(request_param, '$.user_id'), ''),
                NULLIF(JSON_VALUE(request_param, '$.userId'), ''),
                NULLIF(CAST(user_id AS STRING), '')
            ) AS user_id,
            COUNT(*) AS tryon_task_count,
            COUNTIF(
                status = 'SUCCESS'
                AND SAFE_CAST(JSON_VALUE(output, '$.totalDuration') AS INT64) IS NOT NULL
            ) AS tryon_success_duration_task_count,
            SUM(IF(
                status = 'SUCCESS',
                COALESCE(SAFE_CAST(JSON_VALUE(output, '$.totalDuration') AS INT64), 0),
                0
            )) AS tryon_success_total_duration_ms
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE DATE({service_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND task_type IN ({tryon_task_types_sql})
        GROUP BY dt, user_id
        HAVING user_id IS NOT NULL AND user_id != ''
    ),
    -- 商品点击 PV 用户日聚合（raw_event_id 去重）。
    user_product_click_daily AS (
        SELECT
            dt,
            user_id,
            COUNT(DISTINCT IF(
                STARTS_WITH(event_name, 'c') AND product_code IS NOT NULL,
                raw_event_id,
                NULL
            )) AS product_click_pv_raw
        FROM dwd_base
        GROUP BY dt, user_id
    )
    -- 以 DAU 用户为基座，LEFT JOIN 试穿与商品点击。
    SELECT
        a.dt,
        a.user_id,
        COALESCE(t.tryon_task_count, 0) AS tryon_task_count,
        COALESCE(t.tryon_success_duration_task_count, 0)
            AS tryon_success_duration_task_count,
        COALESCE(t.tryon_success_total_duration_ms, 0)
            AS tryon_success_total_duration_ms,
        COALESCE(c.product_click_pv_raw, 0) AS product_click_pv_raw,
        CURRENT_TIMESTAMP() AS update_time
    FROM active_users a
    LEFT JOIN service_tryon_user_daily t
        ON t.dt = a.dt AND t.user_id = a.user_id
    LEFT JOIN user_product_click_daily c
        ON c.dt = a.dt AND c.user_id = a.user_id;
    """
    logging.info("开始处理: %s", TABLE_NAME)
    job = client.query(query)
    job.result()
    logging.info("%s 刷新完成, 处理日期: %s", TABLE_NAME, dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 %s 日期需要处理", TABLE_NAME)
        raise SystemExit(0)

    run_dws(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
