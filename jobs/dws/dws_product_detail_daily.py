from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_product_detail_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME

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
    return (
        "TIMESTAMP("
        "DATETIME(logAt_timestamp, 'UTC'), "
        "COALESCE(NULLIF(prop_timezone, ''), 'UTC')"
        ")"
    )


def get_dws_product_detail_daily_dates_to_process() -> list[date]:
    """
    按 dws_product_detail_daily 自身水位判定待处理日期。

    规则：
    - 目标表不存在或为空时，全量初始化所有事件日期
    - 目标表已存在时，只处理源表 update_time 晚于目标表最大 update_time 的日期
    """
    event_ts = event_ts_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
    WHERE update_time > (
        SELECT COALESCE(MAX(update_time), TIMESTAMP('1970-01-01'))
        FROM `{target_table_id()}`
    )
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_product_detail_daily(dates: list[date]) -> None:
    """
    产出商详页曝光事实。

    粒度：
    - dt
    """
    if not dates:
        logging.info("没有新的 dws_product_detail_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    event_ts = event_ts_expr()

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        product_detail_exposure_uv,
        product_detail_exposure_pv,
        update_time
    )
    WITH
    -- 生成本批待刷日期，保证无曝光日期也可补零。
    date_list AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 提取商详页曝光事件并统一访客键与事件键。
    product_detail_events AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            COALESCE(NULLIF(prop_user_id, ''), NULLIF(prop_device_id, '')) AS visitor_id,
            COALESCE(raw_event_id, hash_id) AS event_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND event_name = 'v_product_detail'
    ),
    -- 汇总商详页曝光 UV / PV。
    product_detail_metrics AS (
        SELECT
            dt,
            COUNT(DISTINCT IF(visitor_id IS NOT NULL, visitor_id, NULL))
                AS product_detail_exposure_uv,
            COUNT(DISTINCT IF(event_id IS NOT NULL, event_id, NULL))
                AS product_detail_exposure_pv
        FROM product_detail_events
        GROUP BY dt
    )
    SELECT
        d.dt,
        COALESCE(m.product_detail_exposure_uv, 0) AS product_detail_exposure_uv,
        COALESCE(m.product_detail_exposure_pv, 0) AS product_detail_exposure_pv,
        CURRENT_TIMESTAMP() AS update_time
    FROM date_list d
    LEFT JOIN product_detail_metrics m
        USING (dt);
    """
    logging.info("开始处理: dws_product_detail_daily")
    client.query(query).result()
    logging.info(f"dws_product_detail_daily 刷新完成, 处理日期: {dates}")


if __name__ == "__main__":
    log_path = configure_logging()
    start_time = datetime.now(timezone.utc)

    dates = get_dws_product_detail_daily_dates_to_process()
    if not dates:
        logging.info("没有新的 dws_product_detail_daily 数据需要处理")
        logging.info(f"日志文件: {log_path}")
        raise SystemExit(0)

    logging.info(f"dws_product_detail_daily 待处理日期: {dates}")
    run_dws_product_detail_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"dws_product_detail_daily 执行完成, 耗时: {elapsed:.1f} 秒")
    logging.info(f"日志文件: {log_path}")
