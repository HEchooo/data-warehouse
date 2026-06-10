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
SHANGHAI_TZ = "Asia/Shanghai"
TABLE_NAME = "dim_makepod_device_first_visit"
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


def makepod_event_date_expr() -> str:
    return f"DATE(logAt_timestamp, '{SHANGHAI_TZ}')"


def get_dim_makepod_device_first_visit_dates_to_process() -> list[date]:
    """
    按 dim_makepod_device_first_visit 自身水位判定待处理日期。

    规则：
    - 目标表不存在或为空时，全量初始化 Makepod 历史访问日期
    - 目标表已存在时，只处理源表 update_time 晚于目标表最大 update_time 的日期
    """
    event_dt = makepod_event_date_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT {event_dt} AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_makepod_event_log`
        WHERE prop_device_id IS NOT NULL
            AND prop_device_id != ''
            AND event_name NOT IN ({PASSIVE_ACTIVITY_EXCLUDED_EVENTS_SQL})
            AND {event_dt} IS NOT NULL
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT {event_dt} AS dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dwd_makepod_event_log`
    WHERE prop_device_id IS NOT NULL
        AND prop_device_id != ''
        AND event_name NOT IN ({PASSIVE_ACTIVITY_EXCLUDED_EVENTS_SQL})
        AND {event_dt} IS NOT NULL
        AND update_time > (
            SELECT COALESCE(MAX(update_time), TIMESTAMP('1970-01-01'))
            FROM `{target_table_id()}`
        )
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dim_makepod_device_first_visit(dates: list[date]) -> None:
    """
    增量维护 Makepod 设备首次到访维表。
    只扫描本批日期分区，对新设备插入，对已有设备保留更早首次到访日期。
    """
    if not dates:
        logging.info("没有新的 dim_makepod_device_first_visit 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    event_dt = makepod_event_date_expr()

    query = f"""
    MERGE `{target_table_id()}` AS dim
    USING (
        WITH
        -- 提取本批日期内的设备访问记录。
        base AS (
            SELECT
                prop_device_id,
                {event_dt} AS dt
            FROM `{PROJECT_ID}.{DATASET_ID}.dwd_makepod_event_log`
            WHERE prop_device_id IS NOT NULL
                AND prop_device_id != ''
                AND event_name NOT IN ({PASSIVE_ACTIVITY_EXCLUDED_EVENTS_SQL})
                AND {event_dt} IN ({dates_str})
        ),
        -- 汇总本批设备的候选首次到访日期。
        candidate_first_visit AS (
            SELECT
                prop_device_id,
                MIN(dt) AS first_visit_date
            FROM base
            GROUP BY prop_device_id
        )
        SELECT
            prop_device_id,
            first_visit_date
        FROM candidate_first_visit
    ) AS src
    ON dim.prop_device_id = src.prop_device_id
    WHEN MATCHED AND src.first_visit_date < dim.first_visit_date THEN
        UPDATE SET
            first_visit_date = src.first_visit_date,
            update_time = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (prop_device_id, first_visit_date, update_time)
        VALUES (src.prop_device_id, src.first_visit_date, CURRENT_TIMESTAMP());
    """
    logging.info("开始处理: dim_makepod_device_first_visit")
    client.query(query).result()
    logging.info("dim_makepod_device_first_visit 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    start_time = datetime.now(timezone.utc)

    dates = get_dim_makepod_device_first_visit_dates_to_process()
    if not dates:
        logging.info("没有新的 dim_makepod_device_first_visit 数据需要处理")
        logging.info("日志文件: %s", log_path)
        raise SystemExit(0)

    logging.info("dim_makepod_device_first_visit 待处理日期: %s", dates)
    run_dim_makepod_device_first_visit(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dim_makepod_device_first_visit 执行完成, 耗时: %.1f 秒", elapsed)
    logging.info("日志文件: %s", log_path)
