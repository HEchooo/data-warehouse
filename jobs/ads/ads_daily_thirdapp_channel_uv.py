from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
EASTERN_TZ = "America/New_York"
TABLE_NAME = "ads_daily_thirdapp_channel_uv"
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


def get_ads_daily_thirdapp_channel_uv_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    thirdapp 渠道曝光 ADS 只消费 dws_thirdapp_channel_device_daily。

    规则：
    - 目标表不存在或为空时，从 DWS 已落地最早日期补到最晚日期
    - 目标表已存在时，重刷最近 6 小时 DWS 变更日期
    - 同时固定补当前日和前一日，保证零曝光日能产出 overall / 各渠道 0 值行
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        WITH
        -- DWS 已落地日期边界。
        source_bounds AS (
            SELECT
                MIN(dt) AS min_dt,
                MAX(dt) AS max_dt
            FROM `{PROJECT_ID}.{DATASET_ID}.dws_thirdapp_channel_device_daily`
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
    -- 最近 6 小时发生变化的 DWS 业务日。
    changed_dws_dates AS (
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_thirdapp_channel_device_daily`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND dt IS NOT NULL
    ),
    -- 固定补最近两天，保证零曝光日也有落盘结果。
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


def run_ads_daily_thirdapp_channel_uv(dates: list[date]) -> None:
    """
    thirdapp 渠道曝光 UV ADS。

    当前版本只消费：
    - dws_thirdapp_channel_device_daily

    粒度：
    - dt × channel

    说明：
    - `channel = 'overall'` 表示全渠道按设备全局去重后的 UV
    """
    if not dates:
        logging.info("没有新的 ads_daily_thirdapp_channel_uv 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        channel,
        exposure_uv,
        update_time
    )
    WITH
    -- 本批待产出日期清单。
    target_dates AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 历史已出现过的渠道维度，用于给目标日期补齐 0 值行。
    channel_dim AS (
        SELECT DISTINCT channel
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_thirdapp_channel_device_daily`
        WHERE channel IS NOT NULL
            AND channel != ''
    ),
    -- 渠道粒度 UV。
    channel_metric_daily AS (
        SELECT
            dt,
            channel,
            COUNT(DISTINCT device_id) AS exposure_uv
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_thirdapp_channel_device_daily`
        WHERE dt IN (
            SELECT dt
            FROM target_dates
        )
        GROUP BY dt, channel
    ),
    -- overall 粒度 UV，按设备全局去重。
    overall_metric_daily AS (
        SELECT
            dt,
            COUNT(DISTINCT device_id) AS exposure_uv
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_thirdapp_channel_device_daily`
        WHERE dt IN (
            SELECT dt
            FROM target_dates
        )
        GROUP BY dt
    ),
    -- 目标日期与历史渠道组合，保证缺失渠道也能产出 0 值。
    channel_keys AS (
        SELECT
            d.dt,
            c.channel
        FROM target_dates d
        CROSS JOIN channel_dim c
    ),
    -- 渠道粒度结果。
    channel_level AS (
        SELECT
            k.dt,
            k.channel,
            COALESCE(m.exposure_uv, 0) AS exposure_uv
        FROM channel_keys k
        LEFT JOIN channel_metric_daily m
            ON k.dt = m.dt
            AND k.channel = m.channel
    ),
    -- overall 粒度结果。
    overall_level AS (
        SELECT
            d.dt,
            'overall' AS channel,
            COALESCE(m.exposure_uv, 0) AS exposure_uv
        FROM target_dates d
        LEFT JOIN overall_metric_daily m
            ON d.dt = m.dt
    )
    SELECT
        dt,
        channel,
        exposure_uv,
        CURRENT_TIMESTAMP() AS update_time
    FROM channel_level
    UNION ALL
    SELECT
        dt,
        channel,
        exposure_uv,
        CURRENT_TIMESTAMP() AS update_time
    FROM overall_level;
    """
    logging.info("开始处理: ads_daily_thirdapp_channel_uv")
    client.query(query).result()
    logging.info("ads_daily_thirdapp_channel_uv 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_ads_daily_thirdapp_channel_uv_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 ads_daily_thirdapp_channel_uv 日期需要处理")
        raise SystemExit(0)

    run_ads_daily_thirdapp_channel_uv(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("ads_daily_thirdapp_channel_uv 完成, 耗时: %.1f 秒", elapsed)
