from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_daily_user_tryon_penetration"
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


def get_user_tryon_penetration_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    新老用户试穿渗透 ADS 只消费 dws_user_activity_daily。

    规则：
    - 目标表不存在或为空时，按 DWS 已落地历史分区全量初始化
    - 目标表已存在时，只读取最近 6 小时内 DWS 变更的 `dt`
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_activity_daily`
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_activity_daily`
    WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_ads_daily_user_tryon_penetration(dates: list[date]) -> None:
    """
    新老用户试穿渗透 ADS。

    当前版本只消费：
    - dws_user_activity_daily

    ADS 只保留报表粒度聚合，不再重复计算首活口径和试穿行为识别。
    """
    if not dates:
        logging.info("没有新的 ads_daily_user_tryon_penetration 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        user_group,
        dau_uv,
        tryon_uv,
        update_time
    )
    WITH
    -- 用户活动 DWS：只取本批目标日期内、首活日期可判定的已登录用户。
    user_activity_fact AS (
        SELECT
            dt,
            user_id,
            first_active_date,
            used_ai_tryon
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_activity_daily`
        WHERE dt IN ({dates_str})
            AND first_active_date IS NOT NULL
    ),
    -- 目标日期骨架：保留本批次内出现过的业务日。
    date_spine AS (
        SELECT DISTINCT dt
        FROM user_activity_fact
    ),
    -- 用户分群骨架：固定输出新用户和老用户两类。
    user_groups AS (
        SELECT '新用户' AS user_group
        UNION ALL
        SELECT '老用户' AS user_group
    ),
    -- 日期 × 用户分群骨架：保证缺某一组时也能产出 0 值行。
    date_group_spine AS (
        SELECT
            d.dt,
            g.user_group
        FROM date_spine d
        CROSS JOIN user_groups g
    ),
    -- 新老用户日聚合：复用首活日期与试穿布尔标记，不在 ADS 重新识别。
    user_group_daily AS (
        SELECT
            dt,
            CASE
                WHEN first_active_date = dt THEN '新用户'
                WHEN first_active_date < dt THEN '老用户'
                ELSE NULL
            END AS user_group,
            COUNT(DISTINCT user_id) AS dau_uv,
            COUNT(DISTINCT IF(used_ai_tryon = 1, user_id, NULL)) AS tryon_uv
        FROM user_activity_fact
        GROUP BY dt, user_group
    )
    SELECT
        s.dt,
        s.user_group,
        COALESCE(f.dau_uv, 0) AS dau_uv,
        COALESCE(f.tryon_uv, 0) AS tryon_uv,
        CURRENT_TIMESTAMP() AS update_time
    FROM date_group_spine s
    LEFT JOIN user_group_daily f
        ON f.dt = s.dt
        AND f.user_group = s.user_group
    ORDER BY s.dt, s.user_group;
    """
    logging.info("开始处理: ads_daily_user_tryon_penetration")
    job = client.query(query)
    job.result()
    logging.info("ads_daily_user_tryon_penetration 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_user_tryon_penetration_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 ads_daily_user_tryon_penetration 日期需要处理")
        raise SystemExit(0)

    run_ads_daily_user_tryon_penetration(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("ads_daily_user_tryon_penetration 完成, 耗时: %.1f 秒", elapsed)
