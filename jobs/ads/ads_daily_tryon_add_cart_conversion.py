from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_daily_tryon_add_cart_conversion"
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


def get_tryon_add_cart_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    试穿转化 ADS 同时依赖 `dws_tryon_confirm_daily` 与 `dws_tryon_user_daily`。

    规则：
    - 目标表不存在或为空时，按 `dws_tryon_confirm_daily` 的日期骨架全量初始化
    - 目标表已存在时，合并最近 6 小时两张 DWS 变更的 `dt`
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_tryon_confirm_daily`
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    WITH
    -- 最近 6 小时试穿点击汇总变更，用于保留日期骨架的一致性。
    changed_confirm_dates AS (
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_tryon_confirm_daily`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 最近 6 小时试穿转化用户事实变更，对应直接受影响的业务日。
    changed_user_dates AS (
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_tryon_user_daily`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt FROM changed_confirm_dates
        UNION DISTINCT
        SELECT dt FROM changed_user_dates
    )
    WHERE dt IS NOT NULL
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_ads_daily_tryon_add_cart_conversion(dates: list[date]) -> None:
    """
    试穿结果转化日报。

    当前版本只消费：
    - dws_tryon_confirm_daily
    - dws_tryon_user_daily

    ADS 只保留终层展示指标：
    - add_cart_rate
    """
    if not dates:
        logging.info("没有新的 ads_daily_tryon_add_cart_conversion 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        tryon_uv,
        add_cart_uv,
        add_cart_rate,
        update_time
    )
    WITH
    -- 日期骨架层：复用试穿点击汇总 DWS 的业务日，保留 0 值日期。
    date_spine AS (
        SELECT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_tryon_confirm_daily`
        WHERE dt IN ({dates_str})
    ),
    -- 用户事实聚合层：从用户级 DWS 汇总试穿 UV 与加购 UV。
    user_daily AS (
        SELECT
            dt,
            SUM(has_tryon) AS tryon_uv,
            SUM(has_add_cart) AS add_cart_uv
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_tryon_user_daily`
        WHERE dt IN ({dates_str})
        GROUP BY dt
    )
    SELECT
        d.dt,
        COALESCE(u.tryon_uv, 0) AS tryon_uv,
        COALESCE(u.add_cart_uv, 0) AS add_cart_uv,
        CAST(
            CASE
                WHEN COALESCE(u.tryon_uv, 0) = 0 THEN 0
                ELSE ROUND(
                    CAST(COALESCE(u.add_cart_uv, 0) AS NUMERIC)
                    / CAST(COALESCE(u.tryon_uv, 0) AS NUMERIC),
                    4
                )
            END AS NUMERIC
        ) AS add_cart_rate,
        CURRENT_TIMESTAMP() AS update_time
    FROM date_spine d
    LEFT JOIN user_daily u USING (dt);
    """
    logging.info("开始处理: ads_daily_tryon_add_cart_conversion")
    job = client.query(query)
    job.result()
    logging.info("ads_daily_tryon_add_cart_conversion 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_tryon_add_cart_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 ads_daily_tryon_add_cart_conversion 日期需要处理")
        raise SystemExit(0)

    run_ads_daily_tryon_add_cart_conversion(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("ads_daily_tryon_add_cart_conversion 完成, 耗时: %.1f 秒", elapsed)
