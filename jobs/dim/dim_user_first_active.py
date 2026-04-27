from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dim_user_first_active"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dim" / TABLE_NAME

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


def get_dim_user_first_active_dates_to_process() -> list[date]:
    """
    按 dim_user_first_active 自身水位判定待处理日期。

    规则：
    - 目标表不存在或为空时，全量初始化所有注册用户活跃日期
    - 目标表已存在时，只处理源表 update_time 晚于目标表最大 update_time 的日期
    """
    event_ts = event_ts_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE prop_user_id IS NOT NULL
            AND prop_user_id != ''
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
    WHERE prop_user_id IS NOT NULL
        AND prop_user_id != ''
        AND update_time > (
            SELECT COALESCE(MAX(update_time), TIMESTAMP('1970-01-01'))
            FROM `{target_table_id()}`
        )
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dim_user_first_active(dates: list[date]) -> None:
    """
    增量维护用户首活维表。
    只扫描本批日期分区，对新用户插入，对已有用户保留更早首活日期。
    """
    if not dates:
        logging.info("没有新的 dim_user_first_active 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    event_ts = event_ts_expr()

    query = f"""
    MERGE `{target_table_id()}` AS dim
    USING (
        WITH
        -- 提取本批日期内的用户活跃记录。
        base AS (
            SELECT
                prop_user_id,
                DATE({event_ts}, '{TORONTO_TZ}') AS dt
            FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
            WHERE prop_user_id IS NOT NULL
                AND prop_user_id != ''
        ),
        -- 汇总本批用户的候选首活日期。
        candidate_first_active AS (
            SELECT
                prop_user_id,
                MIN(dt) AS first_active_date
            FROM base
            WHERE dt IN ({dates_str})
            GROUP BY prop_user_id
        )
        SELECT
            prop_user_id,
            first_active_date
        FROM candidate_first_active
    ) AS src
    ON dim.prop_user_id = src.prop_user_id
    WHEN MATCHED AND src.first_active_date < dim.first_active_date THEN
        UPDATE SET
            first_active_date = src.first_active_date,
            update_time = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (prop_user_id, first_active_date, update_time)
        VALUES (src.prop_user_id, src.first_active_date, CURRENT_TIMESTAMP());
    """
    logging.info("开始处理: dim_user_first_active")
    client.query(query).result()
    logging.info(f"dim_user_first_active 刷新完成, 处理日期: {dates}")


if __name__ == "__main__":
    log_path = configure_logging()
    start_time = datetime.now(timezone.utc)

    dates = get_dim_user_first_active_dates_to_process()
    if not dates:
        logging.info("没有新的 dim_user_first_active 数据需要处理")
        logging.info(f"日志文件: {log_path}")
        raise SystemExit(0)

    logging.info(f"dim_user_first_active 待处理日期: {dates}")
    run_dim_user_first_active(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"dim_user_first_active 执行完成, 耗时: {elapsed:.1f} 秒")
    logging.info(f"日志文件: {log_path}")
