from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
EASTERN_TZ = "America/New_York"
TABLE_NAME = "ads_daily_lta_spu"
SOURCE_TABLE = "dws_lta_thirdparty_event_daily"
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
    return f"{PROJECT_ID}.{DATASET_ID}.{SOURCE_TABLE}"


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
    ADS 只消费 dws_lta_thirdparty_event_daily。

    规则：
    - 目标表不存在或为空时，从 DWS 已落地最早日期补到最晚日期
    - 目标表已存在时，重刷最近 6 小时 DWS 变更日期，并固定补最近两天
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        WITH source_bounds AS (
            SELECT MIN(dt) AS min_dt, MAX(dt) AS max_dt
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


def run_ads_daily_lta_spu(dates: list[date]) -> None:
    """
    LTA 看板商品维度表。

    粒度：
    - dt × 商品（spu + spu_name）

    说明：
    - 仅统计带商品的两个事件（商品曝光、商品点击），无页面曝光
    - 带货红人数 = 曝光该商品的不同红人数（COUNT DISTINCT kol_id，仅商品曝光事件）
    - spu_name 取商品曝光事件上的名称；商品点击事件无 spu_name
    - 商品点击转化率 = 商品点击 / 商品曝光（PV、UV 各一列）
    """
    if not dates:
        logging.info("没有新的 ads_daily_lta_spu 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        spu,
        spu_name,
        kol_count,
        goods_exposure_pv,
        goods_exposure_uv,
        goods_click_pv,
        goods_click_uv,
        goods_click_cvr_pv,
        goods_click_cvr_uv,
        update_time
    )
    WITH
    spu_metric AS (
        SELECT
            dt,
            spu,
            MAX(IF(event_name = 'v_third_party_goods', spu_name, NULL)) AS spu_name,
            COUNT(DISTINCT IF(event_name = 'v_third_party_goods', kol_id, NULL)) AS kol_count,
            COUNT(DISTINCT IF(event_name = 'v_third_party_goods', raw_event_id, NULL)) AS goods_exposure_pv,
            COUNT(DISTINCT IF(event_name = 'v_third_party_goods', device_id, NULL)) AS goods_exposure_uv,
            COUNT(DISTINCT IF(event_name = 'c_third_party_goods', raw_event_id, NULL)) AS goods_click_pv,
            COUNT(DISTINCT IF(event_name = 'c_third_party_goods', device_id, NULL)) AS goods_click_uv
        FROM `{source_table_id()}`
        WHERE dt IN ({dates_str})
            AND spu IS NOT NULL
        GROUP BY dt, spu
    )
    SELECT
        dt,
        spu,
        spu_name,
        kol_count,
        goods_exposure_pv,
        goods_exposure_uv,
        goods_click_pv,
        goods_click_uv,
        SAFE_DIVIDE(goods_click_pv, goods_exposure_pv) AS goods_click_cvr_pv,
        SAFE_DIVIDE(goods_click_uv, goods_exposure_uv) AS goods_click_cvr_uv,
        CURRENT_TIMESTAMP() AS update_time
    FROM spu_metric;
    """
    logging.info("开始处理: ads_daily_lta_spu")
    client.query(query).result()
    logging.info("ads_daily_lta_spu 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 ads_daily_lta_spu 日期需要处理")
        raise SystemExit(0)

    run_ads_daily_lta_spu(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("ads_daily_lta_spu 完成, 耗时: %.1f 秒", elapsed)
