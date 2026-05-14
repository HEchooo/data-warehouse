from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_daily_user_tryon_penetration_app_version"
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


def get_user_tryon_penetration_app_version_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    新老用户版本试穿渗透 ADS 只消费 dws_user_activity_app_version_daily。

    规则：
    - 目标表不存在或为空时，按 DWS 已落地历史分区全量初始化
    - 目标表已存在时，只读取最近 6 小时内 DWS 变更的 `dt`
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_activity_app_version_daily`
        WHERE dt IS NOT NULL
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_activity_app_version_daily`
    WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
        AND dt IS NOT NULL
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_ads_daily_user_tryon_penetration_app_version(dates: list[date]) -> None:
    """
    新老用户版本试穿渗透 ADS。

    当前版本只消费：
    - dws_user_activity_app_version_daily

    ADS 只保留报表粒度聚合和试穿率轻派生。
    """
    if not dates:
        logging.info("没有新的 ads_daily_user_tryon_penetration_app_version 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        user_group,
        app_version,
        dau_uv,
        tryon_uv,
        tryon_penetration_rate,
        update_time
    )
    WITH
    -- 用户版本活动事实：只取本批目标日期内、首活日期可判定的已登录用户版本明细。
    user_activity_app_version_fact AS (
        SELECT
            dt,
            user_id,
            app_version,
            first_active_date,
            used_ai_tryon
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_activity_app_version_daily`
        WHERE dt IN ({dates_str})
            AND first_active_date IS NOT NULL
    ),
    -- 新老用户版本日聚合：同一用户同一天命中过多个版本时，会在各版本下分别计入。
    user_group_app_version_daily AS (
        SELECT
            dt,
            CASE
                WHEN first_active_date = dt THEN '新用户'
                WHEN first_active_date < dt THEN '老用户'
                ELSE NULL
            END AS user_group,
            app_version,
            COUNT(DISTINCT user_id) AS dau_uv,
            COUNT(DISTINCT IF(used_ai_tryon = 1, user_id, NULL)) AS tryon_uv
        FROM user_activity_app_version_fact
        GROUP BY dt, user_group, app_version
    )
    SELECT
        dt,
        user_group,
        app_version,
        dau_uv,
        tryon_uv,
        SAFE_DIVIDE(tryon_uv, NULLIF(dau_uv, 0))
            AS tryon_penetration_rate,
        CURRENT_TIMESTAMP() AS update_time
    FROM user_group_app_version_daily
    WHERE user_group IS NOT NULL
    ORDER BY dt, user_group, app_version;
    """
    logging.info("开始处理: ads_daily_user_tryon_penetration_app_version")
    client.query(query).result()
    logging.info(
        "ads_daily_user_tryon_penetration_app_version 刷新完成, 处理日期: %s",
        dates,
    )


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_user_tryon_penetration_app_version_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 ads_daily_user_tryon_penetration_app_version 日期需要处理")
        raise SystemExit(0)

    run_ads_daily_user_tryon_penetration_app_version(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(
        "ads_daily_user_tryon_penetration_app_version 完成, 耗时: %.1f 秒",
        elapsed,
    )
