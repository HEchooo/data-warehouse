from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_tryon_confirm_post_daily"
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


def get_tryon_confirm_post_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    帖子级试穿点击 DWS 只依赖 c_tryon_confirm 事件，不复用共享水位。

    规则：
    - 目标表不存在或为空时，按 c_tryon_confirm 历史业务日初始化
    - 目标表已存在时，只取最近 6 小时相关事件变更映射出的业务日
    """
    event_ts = event_ts_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE event_name = 'c_tryon_confirm'
            AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
    WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
        AND event_name = 'c_tryon_confirm'
        AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_tryon_confirm_post_daily(dates: list[date]) -> None:
    """
    产出帖子级试穿点击使用事实层。

    粒度：
    - dt × post_code

    指标：
    - click_use_pv：按 raw_event_id 去重后的点击使用次数，仅保留已登录事件
    - click_use_uv：按 user_id 去重后的点击使用用户数
    """
    if not dates:
        logging.info("没有新的 dws_tryon_confirm_post_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    event_ts = event_ts_expr()

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        post_code,
        click_use_pv,
        click_use_uv,
        update_time
    )
    WITH
    -- 事件基础层：抽取帖子级试穿点击事件，并统一已登录用户口径。
    base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            NULLIF(prop_user_id, '') AS user_id,
            raw_event_id,
            post_code
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND event_name = 'c_tryon_confirm'
    ),
    -- 原始事件去重层：只保留已登录且可归属到帖子的点击使用事件。
    dedup_events AS (
        SELECT
            dt,
            post_code,
            raw_event_id,
            ANY_VALUE(user_id) AS user_id
        FROM base
        WHERE raw_event_id IS NOT NULL
            AND user_id IS NOT NULL
            AND post_code IS NOT NULL
        GROUP BY dt, post_code, raw_event_id
    ),
    -- 日帖子聚合层：产出 PV 与已登录 UV 基础事实。
    metric_daily AS (
        SELECT
            dt,
            post_code,
            COUNT(*) AS click_use_pv,
            COUNT(DISTINCT user_id) AS click_use_uv
        FROM dedup_events
        GROUP BY dt, post_code
    )
    SELECT
        dt,
        post_code,
        click_use_pv,
        click_use_uv,
        CURRENT_TIMESTAMP() AS update_time
    FROM metric_daily;
    """
    logging.info("开始处理: dws_tryon_confirm_post_daily")
    job = client.query(query)
    job.result()
    logging.info("dws_tryon_confirm_post_daily 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_tryon_confirm_post_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 dws_tryon_confirm_post_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_tryon_confirm_post_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dws_tryon_confirm_post_daily 完成, 耗时: %.1f 秒", elapsed)
