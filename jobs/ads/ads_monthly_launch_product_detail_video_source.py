from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
EASTERN_TZ = "America/New_York"
TABLE_NAME = "ads_monthly_launch_product_detail_video_source"
SOURCE_TABLE_NAME = "dws_launch_product_detail_device_daily"
NO_SOURCE_VIDEO_ID = "无法识别来源"
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


def dates_to_array_sql(dates: list[date]) -> str:
    return ", ".join([f"DATE '{d}'" for d in dates])


def target_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


def source_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{SOURCE_TABLE_NAME}"


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


def get_months_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    Launch 商详页来源概览月报只消费 dws_launch_product_detail_device_daily。

    规则：
    - 目标表不存在或为空时，按 DWS 已落地日期范围推算全部历史月份
    - 目标表已存在时，重刷最近 6 小时 DWS 变更日期所在月
    - 同时固定补当前月和上一月
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT DATE_TRUNC(dt, MONTH) AS month
        FROM `{source_table_id()}`
        WHERE dt IS NOT NULL
        ORDER BY month
        """
        results = client.query(query).result()
        return [row.month for row in results]

    query = f"""
    WITH
    changed_months AS (
        SELECT DISTINCT DATE_TRUNC(dt, MONTH) AS month
        FROM `{source_table_id()}`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND dt IS NOT NULL
    ),
    recent_months AS (
        SELECT DATE_TRUNC(
            DATE_SUB(CURRENT_DATE('{EASTERN_TZ}'), INTERVAL 1 MONTH), MONTH
        ) AS month
        UNION DISTINCT
        SELECT DATE_TRUNC(CURRENT_DATE('{EASTERN_TZ}'), MONTH) AS month
    )
    SELECT DISTINCT month
    FROM (
        SELECT month FROM changed_months
        UNION DISTINCT
        SELECT month FROM recent_months
    )
    WHERE month IS NOT NULL
    ORDER BY month
    """
    results = client.query(query).result()
    return [row.month for row in results]


def run(months: list[date]) -> None:
    """
    Launch 商详页来源概览月报。

    粒度：
    - month

    说明：
    - 商品页 PV/UV 为当月全量（不区分来源）
    - 有来源 UV：`video_id != '无法识别来源'` 的设备去重计数
    - 无来源 UV：`video_id = '无法识别来源'` 的设备去重计数
    """
    if not months:
        logging.info("没有新的 %s 月份需要处理", TABLE_NAME)
        return

    months_str = dates_to_sql_list(months)
    months_array_sql = dates_to_array_sql(months)
    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE month IN ({months_str});

    INSERT INTO `{target_table_id()}`
    (
        month,
        product_detail_pv,
        product_detail_uv,
        has_source_uv,
        no_source_uv,
        update_time
    )
    WITH
    target_months AS (
        SELECT
            month,
            DATE_ADD(month, INTERVAL 1 MONTH) AS next_month
        FROM UNNEST([{months_array_sql}]) AS month
    ),
    base AS (
        SELECT
            DATE_TRUNC(s.dt, MONTH) AS month,
            s.video_id,
            s.device_id,
            s.product_detail_pv
        FROM `{source_table_id()}` s
        WHERE s.dt >= (SELECT MIN(month) FROM target_months)
            AND s.dt < (SELECT MAX(next_month) FROM target_months)
            AND DATE_TRUNC(s.dt, MONTH) IN (SELECT month FROM target_months)
    )
    SELECT
        month,
        SUM(product_detail_pv) AS product_detail_pv,
        COUNT(DISTINCT IF(product_detail_pv > 0, device_id, NULL)) AS product_detail_uv,
        COUNT(DISTINCT IF(product_detail_pv > 0 AND video_id != '{NO_SOURCE_VIDEO_ID}', device_id, NULL)) AS has_source_uv,
        COUNT(DISTINCT IF(product_detail_pv > 0 AND video_id = '{NO_SOURCE_VIDEO_ID}', device_id, NULL)) AS no_source_uv,
        CURRENT_TIMESTAMP() AS update_time
    FROM base
    GROUP BY month;
    """
    logging.info("开始处理: %s", TABLE_NAME)
    client.query(query).result()
    logging.info("%s 刷新完成, 处理月份: %s", TABLE_NAME, months)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    months = get_months_to_process()
    logging.info("待处理月份: %s", months if months else "[]")

    if not months:
        logging.info("没有新的 %s 月份需要处理", TABLE_NAME)
        raise SystemExit(0)

    run(months)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
