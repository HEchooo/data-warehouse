from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_tryon_user_daily"
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


def get_tryon_user_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    试穿转化用户 DWS 只依赖试穿/加购事件，不复用共享水位。

    规则：
    - 目标表不存在或为空时，按 c_tryon / c_add_cart 历史业务日初始化
    - 目标表已存在时，只取最近 6 小时相关事件变更映射出的业务日
    """
    event_ts = event_ts_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE event_name IN ('c_tryon', 'c_add_cart')
            AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
    WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
        AND event_name IN ('c_tryon', 'c_add_cart')
        AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_tryon_user_daily(dates: list[date]) -> None:
    """
    产出试穿转化用户事实层。

    粒度：
    - dt × visitor_id

    指标：
    - tryon_event_count：按 action_event_id 去重后的试穿事件数
    - add_cart_event_count：按 action_event_id 去重后的加购事件数
    - has_tryon / has_add_cart：用户级转化标记
    """
    if not dates:
        logging.info("没有新的 dws_tryon_user_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    event_ts = event_ts_expr()

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        visitor_id,
        tryon_event_count,
        add_cart_event_count,
        has_tryon,
        has_add_cart,
        update_time
    )
    WITH
    -- 事件基础层：抽取试穿/加购事件，并统一访客主键与事件去重主键。
    base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            event_name,
            COALESCE(NULLIF(prop_user_id, ''), NULLIF(prop_device_id, '')) AS visitor_id,
            COALESCE(raw_event_id, hash_id) AS action_event_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND event_name IN ('c_tryon', 'c_add_cart')
    ),
    -- 动作去重层：先按原始动作去重，避免 dwd 展开导致访客行为放大。
    dedup_events AS (
        SELECT
            dt,
            event_name,
            action_event_id,
            ANY_VALUE(visitor_id) AS visitor_id
        FROM base
        WHERE visitor_id IS NOT NULL
            AND action_event_id IS NOT NULL
        GROUP BY dt, event_name, action_event_id
    ),
    -- 用户日聚合层：产出试穿/加购事件次数与布尔标记。
    user_daily AS (
        SELECT
            dt,
            visitor_id,
            SUM(IF(event_name = 'c_tryon', 1, 0)) AS tryon_event_count,
            SUM(IF(event_name = 'c_add_cart', 1, 0)) AS add_cart_event_count
        FROM dedup_events
        GROUP BY dt, visitor_id
    )
    SELECT
        dt,
        visitor_id,
        tryon_event_count,
        add_cart_event_count,
        IF(tryon_event_count > 0, 1, 0) AS has_tryon,
        IF(add_cart_event_count > 0, 1, 0) AS has_add_cart,
        CURRENT_TIMESTAMP() AS update_time
    FROM user_daily;
    """
    logging.info("开始处理: dws_tryon_user_daily")
    job = client.query(query)
    job.result()
    logging.info("dws_tryon_user_daily 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_tryon_user_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 dws_tryon_user_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_tryon_user_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dws_tryon_user_daily 完成, 耗时: %.1f 秒", elapsed)
