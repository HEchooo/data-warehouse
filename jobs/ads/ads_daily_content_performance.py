from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "ads_daily_content_performance"
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


def event_ts_expr() -> str:
    """
    将 logAt_timestamp 先按 prop_timezone 解释为真实时间，再转成 UTC TIMESTAMP。
    logAt_timestamp 存的是“本地时间字面值”，不能直接当 UTC 使用。
    """
    return (
        "TIMESTAMP("
        "DATETIME(logAt_timestamp, 'UTC'), "
        "COALESCE(NULLIF(prop_timezone, ''), 'UTC')"
        ")"
    )


def service_ts_expr(field_name: str = "gmt_create") -> str:
    """v3 服务表的 gmt_create 是 UTC DATETIME，这里统一转成 TIMESTAMP。"""
    return f"TIMESTAMP({field_name}, 'UTC')"


def platform_expr() -> str:
    return (
        "CASE "
        "WHEN prop_url IS NOT NULL AND prop_url != '' THEN 'H5' "
        "WHEN LOWER(prop_os) = 'ios' THEN 'iOS' "
        "WHEN LOWER(prop_os) IN ('android', 'harmony') THEN 'Android' "
        "ELSE 'unknown' "
        "END"
    )


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


def get_content_performance_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    平台内容 ADS 只消费 `dws_platform_content_daily`，待处理日期单独判断。

    规则：
    - 目标表不存在或为空时，按 DWS 已落地历史分区全量初始化
    - 目标表已存在时，只读取最近 6 小时内 DWS 变更的 `dt`
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_platform_content_daily`
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dws_platform_content_daily`
    WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_ads_daily_content_performance(dates: list[date]) -> None:
    """
    平台整体内容表现日报（每日每平台一行）。

    当前版本只消费：
    - dws_platform_content_daily

    ADS 只保留终层展示指标：
    - avg_browse_content_count_per_user
    - like_rate
    - product_click_rate
    - follow_rate
    - follow_conversion_rate
    - read_follow_rate
    - tryon_penetration_rate
    """
    if not dates:
        logging.info("没有新的 ads_daily_content_performance 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        platform,
        platform_exposure_uv,
        content_exposure_uv,
        content_exposure_pv,
        product_detail_exposure_uv,
        product_detail_exposure_pv,
        product_click_uv,
        product_click_pv,
        avg_browse_content_count_per_user,
        like_total_count,
        like_uv,
        like_rate,
        read_uv,
        follow_total_count,
        follow_column_count,
        follow_uv,
        dau_uv,
        product_click_rate,
        follow_rate,
        follow_conversion_rate,
        read_follow_rate,
        tryon_total_count,
        tryon_uv,
        column_exposure_pv,
        tryon_penetration_rate,
        update_time
    )
    WITH
    -- 平台内容核心事实层：只取本批目标日期。
    platform_fact AS (
        SELECT
            dt,
            platform,
            platform_exposure_uv,
            content_exposure_uv,
            content_exposure_pv,
            product_detail_exposure_uv,
            product_detail_exposure_pv,
            product_click_uv,
            product_click_pv,
            read_uv,
            like_total_count,
            like_uv,
            follow_total_count,
            follow_column_count,
            follow_uv,
            dau_uv,
            tryon_total_count,
            tryon_uv,
            column_exposure_pv
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_platform_content_daily`
        WHERE dt IN ({dates_str})
    )
    SELECT
        dt,
        platform,
        platform_exposure_uv,
        content_exposure_uv,
        content_exposure_pv,
        product_detail_exposure_uv,
        product_detail_exposure_pv,
        product_click_uv,
        product_click_pv,
        CAST(
            CASE
                WHEN content_exposure_uv = 0 THEN 0
                ELSE ROUND(
                    CAST(content_exposure_pv AS NUMERIC) / CAST(content_exposure_uv AS NUMERIC),
                    4
                )
            END AS NUMERIC
        ) AS avg_browse_content_count_per_user,
        like_total_count,
        like_uv,
        CAST(
            CASE
                WHEN content_exposure_uv = 0 THEN 0
                ELSE ROUND(
                    CAST(like_uv AS NUMERIC) / CAST(content_exposure_uv AS NUMERIC),
                    4
                )
            END AS NUMERIC
        ) AS like_rate,
        read_uv,
        follow_total_count,
        follow_column_count,
        follow_uv,
        dau_uv,
        CAST(
            CASE
                WHEN dau_uv = 0 THEN 0
                ELSE ROUND(
                    CAST(product_click_uv AS NUMERIC) / CAST(dau_uv AS NUMERIC),
                    4
                )
            END AS NUMERIC
        ) AS product_click_rate,
        CAST(
            CASE
                WHEN dau_uv = 0 THEN 0
                ELSE ROUND(
                    CAST(follow_uv AS NUMERIC) / CAST(dau_uv AS NUMERIC),
                    4
                )
            END AS NUMERIC
        ) AS follow_rate,
        CAST(
            CASE
                WHEN like_uv = 0 THEN 0
                ELSE ROUND(
                    CAST(follow_uv AS NUMERIC) / CAST(like_uv AS NUMERIC),
                    4
                )
            END AS NUMERIC
        ) AS follow_conversion_rate,
        CAST(
            CASE
                WHEN column_exposure_pv = 0 THEN 0
                ELSE ROUND(
                    CAST(follow_total_count AS NUMERIC) / CAST(column_exposure_pv AS NUMERIC),
                    4
                )
            END AS NUMERIC
        ) AS read_follow_rate,
        tryon_total_count,
        tryon_uv,
        column_exposure_pv,
        CAST(
            CASE
                WHEN dau_uv = 0 THEN 0
                ELSE ROUND(
                    CAST(tryon_uv AS NUMERIC) / CAST(dau_uv AS NUMERIC),
                    4
                )
            END AS NUMERIC
        ) AS tryon_penetration_rate,
        CURRENT_TIMESTAMP() AS update_time
    FROM platform_fact;
    """
    logging.info("开始处理: ads_daily_content_performance")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_content_performance 刷新完成, 处理日期: {dates}")


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info(f"日志文件: {log_path}")

    start_time = datetime.now(timezone.utc)
    dates = get_content_performance_dates_to_process()
    logging.info(f"待处理日期: {dates if dates else '[]'}")

    if not dates:
        logging.info("没有新的 ads_daily_content_performance 日期需要处理")
        raise SystemExit(0)

    run_ads_daily_content_performance(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"ads_daily_content_performance 完成, 耗时: {elapsed:.1f} 秒")
