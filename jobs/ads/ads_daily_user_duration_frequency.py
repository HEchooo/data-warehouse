from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "ads_daily_user_duration_frequency"
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


def get_user_duration_frequency_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    用户时长与访问频次 ADS 只消费 dws_device_activity_daily。

    规则：
    - 目标表不存在或为空时，按 DWS 已落地历史分区全量初始化
    - 目标表已存在时，只读取最近 6 小时内 DWS 变更的 `dt`
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_activity_daily`
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_activity_daily`
    WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_ads_daily_user_duration_frequency(dates: list[date]) -> None:
    """
    用户时长与访问频次 ADS。

    当前版本只消费：
    - dws_device_activity_daily

    ADS 只保留展示层派生指标：
    - avg_duration_min
    - avg_visit_freq
    - next_day_retention_rate / day_7_retention_rate / day_30_retention_rate
    - dau_mau
    """
    if not dates:
        logging.info("没有新的 ads_daily_user_duration_frequency 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        dau_uv,
        total_duration_min,
        avg_duration_min,
        app_launch_count,
        avg_visit_freq,
        next_day_retention_rate,
        day_7_retention_rate,
        day_30_retention_rate,
        mau_uv,
        dau_mau,
        update_time,
        user_segment
    )
    WITH
    -- 用户活动 DWS：只取本批目标日期。
    device_activity_fact AS (
        SELECT
            dt,
            user_segment,
            dau_uv,
            total_duration_min,
            app_launch_count,
            retained_1d_uv,
            retained_7d_uv,
            retained_30d_uv,
            mau_uv
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_activity_daily`
        WHERE dt IN ({dates_str})
    )
    SELECT
        dt,
        dau_uv,
        total_duration_min,
        CAST(
            CASE
                WHEN dau_uv = 0 THEN 0
                ELSE ROUND(total_duration_min / CAST(dau_uv AS NUMERIC), 4)
            END AS NUMERIC
        ) AS avg_duration_min,
        app_launch_count,
        CAST(
            CASE
                WHEN dau_uv = 0 THEN 0
                ELSE ROUND(CAST(app_launch_count AS NUMERIC) / CAST(dau_uv AS NUMERIC), 4)
            END AS NUMERIC
        ) AS avg_visit_freq,
        CAST(
            CASE
                WHEN dau_uv = 0 THEN 0
                WHEN DATE_ADD(dt, INTERVAL 1 DAY) > DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY)
                    THEN NULL
                ELSE ROUND(CAST(retained_1d_uv AS NUMERIC) / CAST(dau_uv AS NUMERIC), 4)
            END AS NUMERIC
        ) AS next_day_retention_rate,
        CAST(
            CASE
                WHEN dau_uv = 0 THEN 0
                WHEN DATE_ADD(dt, INTERVAL 7 DAY) > DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY)
                    THEN NULL
                ELSE ROUND(CAST(retained_7d_uv AS NUMERIC) / CAST(dau_uv AS NUMERIC), 4)
            END AS NUMERIC
        ) AS day_7_retention_rate,
        CAST(
            CASE
                WHEN dau_uv = 0 THEN 0
                WHEN DATE_ADD(dt, INTERVAL 30 DAY) > DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY)
                    THEN NULL
                ELSE ROUND(CAST(retained_30d_uv AS NUMERIC) / CAST(dau_uv AS NUMERIC), 4)
            END AS NUMERIC
        ) AS day_30_retention_rate,
        mau_uv,
        CAST(
            CASE
                WHEN mau_uv = 0 THEN 0
                ELSE ROUND(CAST(dau_uv AS NUMERIC) / CAST(mau_uv AS NUMERIC), 4)
            END AS NUMERIC
        ) AS dau_mau,
        CURRENT_TIMESTAMP() AS update_time,
        user_segment
    FROM device_activity_fact;
    """
    logging.info("开始处理: ads_daily_user_duration_frequency")
    job = client.query(query)
    job.result()
    logging.info("ads_daily_user_duration_frequency 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_user_duration_frequency_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 ads_daily_user_duration_frequency 日期需要处理")
        raise SystemExit(0)

    run_ads_daily_user_duration_frequency(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("ads_daily_user_duration_frequency 完成, 耗时: %.1f 秒", elapsed)
