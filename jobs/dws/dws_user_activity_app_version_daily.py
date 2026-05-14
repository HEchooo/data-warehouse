from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_user_activity_app_version_daily"
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


def get_user_activity_app_version_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    用户版本活动 DWS 同时依赖用户活动事实和事件侧版本字段。

    规则：
    - 目标表不存在或为空时，按 dws_user_activity_daily 已落地历史分区全量初始化
    - 目标表已存在时，只读取最近 6 小时内相关上游变更的 `dt`
    """
    event_ts = event_ts_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_activity_daily`
        WHERE dt IS NOT NULL
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    WITH
    -- 最近 6 小时变更的用户活动业务日，直接影响 cohort 和试穿标记。
    changed_user_activity_dates AS (
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_activity_daily`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND dt IS NOT NULL
    ),
    -- 最近 6 小时变更的事件业务日，直接影响版本号展开结果。
    changed_event_dates AS (
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
            AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt FROM changed_user_activity_dates
        UNION DISTINCT
        SELECT dt FROM changed_event_dates
    )
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_user_activity_app_version_daily(dates: list[date]) -> None:
    """
    产出用户版本活动事实。

    粒度：
    - dt × user_id × app_version

    语义：
    - first_active_date / used_ai_tryon 复用 dws_user_activity_daily
    - app_version 基于 dwd_event_log 当日已登录事件展开；同一用户同一天多个版本会保留多行
    """
    if not dates:
        logging.info("没有新的 dws_user_activity_app_version_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    min_date = min(dates)
    max_date = max(dates)
    event_ts = event_ts_expr()
    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        user_id,
        app_version,
        first_active_date,
        used_ai_tryon,
        update_time
    )
    WITH
    -- 用户活动事实：复用已登录 cohort、首活日与试穿布尔标记。
    user_activity_fact AS (
        SELECT
            dt,
            user_id,
            first_active_date,
            used_ai_tryon
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_activity_daily`
        WHERE dt IN ({dates_str})
            AND first_active_date IS NOT NULL
    ),
    -- 事件版本明细：按天保留用户命中过的所有完整版本号；空值统一收敛到 unknown。
    user_version_event_daily AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            prop_user_id AS user_id,
            COALESCE(NULLIF(TRIM(prop_app_version), ''), 'unknown') AS app_version
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE(logAt_timestamp) BETWEEN DATE_SUB(DATE '{min_date}', INTERVAL 1 DAY)
            AND DATE_ADD(DATE '{max_date}', INTERVAL 1 DAY)
            AND DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
        GROUP BY dt, user_id, app_version
    ),
    -- 用户版本活动事实：把 cohort 用户展开到当天命中的每个版本号。
    user_activity_app_version_fact AS (
        SELECT
            a.dt,
            a.user_id,
            v.app_version,
            a.first_active_date,
            a.used_ai_tryon
        FROM user_activity_fact a
        INNER JOIN user_version_event_daily v
            ON a.dt = v.dt
            AND a.user_id = v.user_id
    )
    SELECT
        dt,
        user_id,
        app_version,
        first_active_date,
        used_ai_tryon,
        CURRENT_TIMESTAMP() AS update_time
    FROM user_activity_app_version_fact;
    """
    logging.info("开始处理: dws_user_activity_app_version_daily")
    client.query(query).result()
    logging.info("dws_user_activity_app_version_daily 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_user_activity_app_version_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 dws_user_activity_app_version_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_user_activity_app_version_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dws_user_activity_app_version_daily 完成, 耗时: %.1f 秒", elapsed)
