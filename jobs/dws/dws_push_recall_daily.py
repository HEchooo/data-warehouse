from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
EASTERN_TZ = "America/New_York"
TABLE_NAME = "dws_push_recall_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6

PUSH_EVENT_NAMES = (
    "send_push",
    "c_push",
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


def string_values_to_sql_list(values: tuple[str, ...]) -> str:
    return ", ".join([f"'{value}'" for value in values])


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


def get_push_recall_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    push recall 主题 DWS 只依赖事件表，不复用共享水位。

    规则：
    - 目标表不存在或为空时，按历史有效事件业务日全量初始化
    - 目标表已存在时，只取最近 6 小时有效事件变更映射出的业务日
    """
    event_ts = event_ts_expr()
    push_events_sql = string_values_to_sql_list(PUSH_EVENT_NAMES)
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    base_filter = f"""
        event_name IN ({push_events_sql})
        AND NULLIF(TRIM(JSON_VALUE(ext, '$.push.recall_strategy')), '') IS NOT NULL
        AND NULLIF(TRIM(JSON_VALUE(ext, '$.push.run_id')), '') IS NOT NULL
        AND COALESCE(NULLIF(prop_user_id, ''), NULLIF(prop_device_id, '')) IS NOT NULL
        AND DATE({event_ts}, '{EASTERN_TZ}') IS NOT NULL
    """

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT DATE({event_ts}, '{EASTERN_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE {base_filter}
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT DATE({event_ts}, '{EASTERN_TZ}') AS dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
    WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
        AND {base_filter}
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_push_recall_daily(dates: list[date]) -> None:
    """
    产出 push recall 主题中间层。

    粒度：
    - dt × recall_strategy × push_run_id × actor_id

    指标：
    - has_send：该唯一主体是否收到过 send_push
    - has_click：该唯一主体是否发生过 c_push
    """
    if not dates:
        logging.info("没有新的 dws_push_recall_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    event_ts = event_ts_expr()
    push_events_sql = string_values_to_sql_list(PUSH_EVENT_NAMES)

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        recall_strategy,
        push_run_id,
        actor_id,
        has_send,
        has_click,
        update_time
    )
    WITH
    -- push 主题基础事件层：统一业务日、召回类型、run_id 与主体口径。
    base_events AS (
        SELECT
            DATE({event_ts}, '{EASTERN_TZ}') AS dt,
            NULLIF(TRIM(JSON_VALUE(ext, '$.push.recall_strategy')), '') AS recall_strategy,
            NULLIF(TRIM(JSON_VALUE(ext, '$.push.run_id')), '') AS push_run_id,
            COALESCE(NULLIF(prop_user_id, ''), NULLIF(prop_device_id, '')) AS actor_id,
            event_name
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{EASTERN_TZ}') IN ({dates_str})
            AND event_name IN ({push_events_sql})
    ),
    -- 过滤无效键，并把发送/点击动作映射成唯一主体级标记。
    normalized_events AS (
        SELECT
            dt,
            recall_strategy,
            push_run_id,
            actor_id,
            event_name = 'send_push' AS is_send,
            event_name = 'c_push' AS is_click
        FROM base_events
        WHERE recall_strategy IS NOT NULL
            AND push_run_id IS NOT NULL
            AND actor_id IS NOT NULL
    ),
    -- 唯一主体层：同一主体同一 run_id 同一天内只保留一行。
    unique_subject_daily AS (
        SELECT
            dt,
            recall_strategy,
            push_run_id,
            actor_id,
            MAX(CAST(is_send AS INT64)) AS has_send,
            MAX(CAST(is_click AS INT64)) AS has_click
        FROM normalized_events
        GROUP BY dt, recall_strategy, push_run_id, actor_id
    )
    SELECT
        dt,
        recall_strategy,
        push_run_id,
        actor_id,
        has_send,
        has_click,
        CURRENT_TIMESTAMP() AS update_time
    FROM unique_subject_daily;
    """
    logging.info("开始处理: dws_push_recall_daily")
    client.query(query).result()
    logging.info("dws_push_recall_daily 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_push_recall_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 dws_push_recall_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_push_recall_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dws_push_recall_daily 完成, 耗时: %.1f 秒", elapsed)
