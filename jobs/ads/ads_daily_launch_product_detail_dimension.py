from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
EASTERN_TZ = "America/New_York"
TABLE_NAME = "ads_daily_launch_product_detail_dimension"
SOURCE_TABLE_NAME = "dws_launch_product_detail_device_daily"
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


def get_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    Launch 商详页多维度日报只消费 dws_launch_product_detail_device_daily。

    规则：
    - 目标表不存在或为空时，从 DWS 已落地最早日期补到最晚日期
    - 目标表已存在时，重刷最近 6 小时 DWS 变更日期
    - 同时固定补当前日和前一日
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        WITH
        source_bounds AS (
            SELECT
                MIN(dt) AS min_dt,
                MAX(dt) AS max_dt
            FROM `{source_table_id()}`
            WHERE dt IS NOT NULL
        )
        SELECT dt
        FROM source_bounds,
        UNNEST(
            IF(
                min_dt IS NULL,
                ARRAY<DATE>[],
                GENERATE_DATE_ARRAY(min_dt, max_dt)
            )
        ) AS dt
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    WITH
    changed_dws_dates AS (
        SELECT DISTINCT dt
        FROM `{source_table_id()}`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND dt IS NOT NULL
    ),
    recent_dates AS (
        SELECT DATE_SUB(CURRENT_DATE('{EASTERN_TZ}'), INTERVAL 1 DAY) AS dt
        UNION DISTINCT
        SELECT CURRENT_DATE('{EASTERN_TZ}') AS dt
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt FROM changed_dws_dates
        UNION DISTINCT
        SELECT dt FROM recent_dates
    )
    WHERE dt IS NOT NULL
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run(dates: list[date]) -> None:
    """
    Launch 商详页多维度日报。

    粒度：
    - dt × video_id × channel × kol_name × country × merchant

    说明：
    - 只统计商品页 PV/UV（product_detail_pv），不含第三方外跳
    - PV 贡献占比 / UV 贡献占比的分母为当日全维度合计，UV 合计按 device_id
      去重（不能对各维度 UV 直接求和，同一设备当日可能落在多个维度组合）
    - 维度组合本身天然稀疏，不做零值行兜底
    """
    if not dates:
        logging.info("没有新的 %s 日期需要处理", TABLE_NAME)
        return

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        video_id,
        channel,
        kol_name,
        country,
        merchant,
        product_detail_pv,
        product_detail_uv,
        pv_contribution_rate,
        uv_contribution_rate,
        avg_visit_per_uv,
        update_time
    )
    WITH
    target_dates AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    base AS (
        SELECT dt, video_id, channel, kol_name, country, merchant, device_id, product_detail_pv
        FROM `{source_table_id()}`
        WHERE dt IN (SELECT dt FROM target_dates)
    ),
    dim_daily AS (
        SELECT
            dt,
            video_id,
            channel,
            kol_name,
            country,
            merchant,
            SUM(product_detail_pv) AS product_detail_pv,
            COUNT(DISTINCT IF(product_detail_pv > 0, device_id, NULL)) AS product_detail_uv
        FROM base
        GROUP BY dt, video_id, channel, kol_name, country, merchant
    ),
    total_daily AS (
        SELECT
            dt,
            SUM(product_detail_pv) AS total_pv,
            COUNT(DISTINCT IF(product_detail_pv > 0, device_id, NULL)) AS total_uv
        FROM base
        GROUP BY dt
    )
    SELECT
        d.dt,
        d.video_id,
        d.channel,
        d.kol_name,
        d.country,
        d.merchant,
        d.product_detail_pv,
        d.product_detail_uv,
        SAFE_DIVIDE(d.product_detail_pv, t.total_pv) AS pv_contribution_rate,
        SAFE_DIVIDE(d.product_detail_uv, t.total_uv) AS uv_contribution_rate,
        SAFE_DIVIDE(d.product_detail_pv, d.product_detail_uv) AS avg_visit_per_uv,
        CURRENT_TIMESTAMP() AS update_time
    FROM dim_daily d
    JOIN total_daily t
        ON d.dt = t.dt;
    """
    logging.info("开始处理: %s", TABLE_NAME)
    client.query(query).result()
    logging.info("%s 刷新完成, 处理日期: %s", TABLE_NAME, dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 %s 日期需要处理", TABLE_NAME)
        raise SystemExit(0)

    run(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
