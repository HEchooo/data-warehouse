from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_daily_home_module_performance"
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


def get_home_module_performance_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    首页模块 ADS 只消费 `dws_home_module_daily`，待处理日期单独判断。

    规则：
    - 目标表不存在或为空时，按 DWS 已落地历史分区全量初始化
    - 目标表已存在时，只读取最近 6 小时内 DWS 变更的 `dt`
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_home_module_daily`
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dws_home_module_daily`
    WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_ads_daily_home_module_performance(dates: list[date]) -> None:
    """
    首页模块表现日报（每日每模块一行）。

    当前版本只消费：
    - dws_home_module_daily

    ADS 只保留终层展示指标：
    - click_rate
    - avg_click_count_per_user
    """
    if not dates:
        logging.info("没有新的 ads_daily_home_module_performance 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        module,
        module_exposure_uv,
        module_click_uv,
        click_rate,
        click_pv,
        avg_click_count_per_user,
        update_time
    )
    WITH
    -- 首页模块核心事实层：只取本批目标日期。
    module_fact AS (
        SELECT
            dt,
            module,
            module_exposure_uv,
            module_click_uv,
            click_pv
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_home_module_daily`
        WHERE dt IN ({dates_str})
    )
    SELECT
        dt,
        module,
        module_exposure_uv,
        module_click_uv,
        CAST(
            CASE
                WHEN module_exposure_uv = 0 THEN 0
                ELSE ROUND(
                    CAST(module_click_uv AS NUMERIC) / CAST(module_exposure_uv AS NUMERIC),
                    4
                )
            END AS NUMERIC
        ) AS click_rate,
        click_pv,
        CAST(
            CASE
                WHEN module_click_uv = 0 THEN 0
                ELSE ROUND(
                    CAST(click_pv AS NUMERIC) / CAST(module_click_uv AS NUMERIC),
                    4
                )
            END AS NUMERIC
        ) AS avg_click_count_per_user,
        CURRENT_TIMESTAMP() AS update_time
    FROM module_fact;
    """
    logging.info("开始处理: ads_daily_home_module_performance")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_home_module_performance 刷新完成, 处理日期: {dates}")


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info(f"日志文件: {log_path}")

    start_time = datetime.now(timezone.utc)
    dates = get_home_module_performance_dates_to_process()
    logging.info(f"待处理日期: {dates if dates else '[]'}")

    if not dates:
        logging.info("没有新的 ads_daily_home_module_performance 日期需要处理")
        raise SystemExit(0)

    run_ads_daily_home_module_performance(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"ads_daily_home_module_performance 完成, 耗时: {elapsed:.1f} 秒")
