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
TABLE_NAME = "dws_push_daily"
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


def service_ts_expr(field_name: str = "gmt_create") -> str:
    """v3 服务表的 gmt_create 是 UTC DATETIME，这里统一转成 TIMESTAMP。"""
    return f"TIMESTAMP({field_name}, 'UTC')"


def service_change_ts_expr() -> str:
    """
    Datastream 源表优先用 source_timestamp 判断最近变更；
    缺失时回退到 gmt_update / gmt_create，避免漏掉已读状态更新。
    """
    return (
        "COALESCE("
        "TIMESTAMP_MILLIS(datastream_metadata.source_timestamp), "
        "TIMESTAMP(COALESCE(gmt_update, gmt_create), 'UTC')"
        ")"
    )


def get_push_daily_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    push 主题 DWS 只依赖 v3_decom.user_notice。

    规则：
    - 目标表不存在或为空时，按首个发送业务日起全量初始化到今天
    - 目标表已存在时，重刷最近 6 小时发生变化的发送业务日
    - 同时固定补当前日和前一日，保证零发送日也能及时落盘
    """
    service_ts = service_ts_expr()
    service_change_ts = service_change_ts_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        WITH
        -- push 主题首个业务日。
        source_bounds AS (
            SELECT MIN(DATE({service_ts}, '{TORONTO_TZ}')) AS min_dt
            FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_notice`
            WHERE DATE({service_ts}, '{TORONTO_TZ}') IS NOT NULL
        )
        SELECT dt
        FROM source_bounds,
        UNNEST(
            IF(
                min_dt IS NULL,
                ARRAY<DATE>[],
                GENERATE_DATE_ARRAY(min_dt, CURRENT_DATE('{TORONTO_TZ}'))
            )
        ) AS dt
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    WITH
    -- 最近 6 小时发生变化的发送业务日。
    changed_push_dates AS (
        SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_notice`
        WHERE {service_change_ts}
                >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND DATE({service_ts}, '{TORONTO_TZ}') IS NOT NULL
    ),
    -- 固定补最近两天，保证新自然日即使零发送也能产出 0 值行。
    recent_dates AS (
        SELECT DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY) AS dt
        UNION DISTINCT
        SELECT CURRENT_DATE('{TORONTO_TZ}') AS dt
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt FROM changed_push_dates
        UNION DISTINCT
        SELECT dt FROM recent_dates
    )
    WHERE dt IS NOT NULL
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_push_daily(dates: list[date]) -> None:
    """
    产出 push 主题日事实层。

    粒度：
    - dt

    指标：
    - push_send_count：当日发送消息条数
    - read_count：当日发送消息中当前已读条数
    """
    if not dates:
        logging.info("没有新的 dws_push_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    service_ts = service_ts_expr()

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        push_send_count,
        read_count,
        update_time
    )
    WITH
    -- 本批待产出的日期清单。
    target_dates AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- user_notice 按发送业务日聚合的消息分子分母。
    push_daily AS (
        SELECT
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            COUNT(1) AS push_send_count,
            COUNTIF(read_status = 1) AS read_count
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_notice`
        WHERE DATE({service_ts}, '{TORONTO_TZ}') IN ({dates_str})
        GROUP BY 1
    )
    SELECT
        target_dates.dt,
        COALESCE(push_daily.push_send_count, 0) AS push_send_count,
        COALESCE(push_daily.read_count, 0) AS read_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM target_dates
    LEFT JOIN push_daily
        ON target_dates.dt = push_daily.dt;
    """
    logging.info("开始处理: dws_push_daily")
    client.query(query).result()
    logging.info("dws_push_daily 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_push_daily_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 dws_push_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_push_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dws_push_daily 完成, 耗时: %.1f 秒", elapsed)
