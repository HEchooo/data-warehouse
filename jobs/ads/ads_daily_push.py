from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_daily_push"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "ads" / TABLE_NAME
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


def get_ads_daily_push_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    push ADS 只消费 dws_push_daily。

    规则：
    - 目标表不存在或为空时，按 DWS 已落地历史分区全量初始化
    - 目标表已存在时，只读取最近 6 小时内 DWS 变更的 dt
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_push_daily`
        WHERE dt IS NOT NULL
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dws_push_daily`
    WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
        AND dt IS NOT NULL
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_ads_daily_push(dates: list[date]) -> None:
    """
    push ADS。

    当前版本只消费：
    - dws_push_daily

    ADS 只保留报表展示字段和已读率轻派生。
    """
    if not dates:
        logging.info("没有新的 ads_daily_push 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        push_send_count,
        read_count,
        read_rate,
        update_time
    )
    WITH
    -- push 主题 DWS：只取本批目标日期。
    push_fact AS (
        SELECT
            dt,
            push_send_count,
            read_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_push_daily`
        WHERE dt IN ({dates_str})
    )
    SELECT
        dt,
        push_send_count,
        read_count,
        CAST(
            CASE
                WHEN push_send_count = 0 THEN 0
                ELSE ROUND(
                    CAST(read_count AS NUMERIC)
                    / CAST(push_send_count AS NUMERIC),
                    4
                )
            END AS NUMERIC
        ) AS read_rate,
        CURRENT_TIMESTAMP() AS update_time
    FROM push_fact;
    """
    logging.info("开始处理: ads_daily_push")
    client.query(query).result()
    logging.info("ads_daily_push 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_ads_daily_push_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 ads_daily_push 日期需要处理")
        raise SystemExit(0)

    run_ads_daily_push(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("ads_daily_push 完成, 耗时: %.1f 秒", elapsed)
