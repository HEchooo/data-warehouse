from datetime import date, datetime
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "dws_column_post_monthly"
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


def get_column_post_months_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    专栏月级帖子快照从日快照衍生：
    - 每个月只取该月最后一个快照日
    - 当前月如果未结束，则取该月最新已落地快照日
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT DATE_TRUNC(dt, MONTH) AS month
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_column_post_daily`
        WHERE dt IS NOT NULL
        ORDER BY month
        """
        results = client.query(query).result()
        return [row.month for row in results]

    query = f"""
    WITH
    -- 最近 6 小时写入的日快照分区，对应需要重算的目标月份。
    changed_snapshot_months AS (
        SELECT DISTINCT DATE_TRUNC(dt, MONTH) AS month
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_column_post_daily`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 最近 6 小时更新过的专栏维表键，用于回推已产出月表中的历史月份。
    changed_columns AS (
        SELECT DISTINCT column_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_column`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND column_id IS NOT NULL
    )
    SELECT DISTINCT month
    FROM (
        SELECT month
        FROM changed_snapshot_months

        UNION DISTINCT

        SELECT DISTINCT t.month
        FROM `{target_table_id()}` t
        JOIN changed_columns c
            ON t.column_id = c.column_id
    )
    WHERE month IS NOT NULL
    ORDER BY month
    """
    results = client.query(query).result()
    return [row.month for row in results]


def run_dws_column_post_monthly(months: list[date]) -> None:
    """
    产出专栏维度的月级在线帖子快照。

    粒度：
    - month × module × column_id

    说明：
    - 每个月只取该月最后一个已落地日快照
    - 当前月若未结束，则取该月最新已落地快照
    """
    if not months:
        logging.info("没有新的 dws_column_post_monthly 月份需要处理")
        return

    months_str = dates_to_sql_list(months)

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE month IN ({months_str});

    INSERT INTO `{target_table_id()}`
    (month, module, column_id, post_total_count, update_time)
    WITH
    -- 当前批次要刷新的目标月份列表。
    target_months AS (
        SELECT DATE(month_str) AS month
        FROM UNNEST([{months_str}]) AS month_str
    ),
    -- 专栏维表：统一 column_id -> module 映射，优先保留启用中的专栏。
    column_dim AS (
        SELECT
            column_id,
            module
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_column`
        WHERE column_id IS NOT NULL
            AND module IS NOT NULL
            AND status = 0
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY column_id
            ORDER BY IF(status = 0, 0, 1), module, column_name
        ) = 1
    ),
    -- 找出每个月最后一个已落地的日快照日期。
    month_latest_snapshot_dates AS (
        SELECT
            DATE_TRUNC(dt, MONTH) AS month,
            MAX(dt) AS snapshot_dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_column_post_daily`
        WHERE DATE_TRUNC(dt, MONTH) IN (SELECT month FROM target_months)
        GROUP BY month
    ),
    -- 取该月最后一个快照日上的专栏在线帖子数。
    month_snapshot AS (
        SELECT
            m.month,
            d.column_id,
            d.post_total_count
        FROM month_latest_snapshot_dates m
        JOIN `{PROJECT_ID}.{DATASET_ID}.dws_column_post_daily` d
            ON d.dt = m.snapshot_dt
    )
    SELECT
        s.month,
        c.module,
        s.column_id,
        s.post_total_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM month_snapshot s
    JOIN column_dim c
        ON c.column_id = s.column_id
    """

    logging.info("开始处理: dws_column_post_monthly")
    job = client.query(query)
    job.result()
    logging.info("dws_column_post_monthly 刷新完成, 处理月份: %s", months)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    months = get_column_post_months_to_process()
    if not months:
        logging.info("没有新的 dws_column_post_monthly 月份需要处理")
    else:
        run_dws_column_post_monthly(months)
