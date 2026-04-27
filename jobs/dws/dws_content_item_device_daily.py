from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_content_item_device_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
CONTENT_ITEM_EXPOSURE_EVENTS = (
    "'v_home_feeds',"
    "'v_shop_feeds',"
    "'v_star_post_feeds',"
    "'v_brand_post_feeds',"
    "'v_kol_post_feeds'"
)
CONTENT_ITEM_CLICK_EVENTS = (
    "'v_product_detail',"
    "'v_star_post_detail',"
    "'v_magazine_post_detail',"
    "'v_brand_post_detail',"
    "'v_kol_post_detail'"
)

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


def event_ts_expr() -> str:
    return (
        "TIMESTAMP("
        "DATETIME(logAt_timestamp, 'UTC'), "
        "COALESCE(NULLIF(prop_timezone, ''), 'UTC')"
        ")"
    )


def get_dws_content_item_device_daily_dates_to_process() -> list[date]:
    """
    按 dws_content_item_device_daily 自身水位判定待处理日期。

    规则：
    - 目标表不存在或为空时，全量初始化所有事件日期
    - 目标表已存在时，只处理源表 update_time 晚于目标表最大 update_time 的日期
    """
    event_ts = event_ts_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE prop_device_id IS NOT NULL
            AND prop_device_id != ''
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
    WHERE prop_device_id IS NOT NULL
        AND prop_device_id != ''
        AND update_time > (
            SELECT COALESCE(MAX(update_time), TIMESTAMP('1970-01-01'))
            FROM `{target_table_id()}`
        )
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_content_item_device_daily(dates: list[date]) -> None:
    """
    产出设备级内容 item 曝光 / 点击事实。

    粒度：
    - dt × prop_device_id
    """
    if not dates:
        logging.info("没有新的 dws_content_item_device_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    event_ts = event_ts_expr()

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        prop_device_id,
        exposure_item_count,
        click_item_count,
        update_time
    )
    WITH
    -- 提取本批日期内的设备级内容 item 事件。
    base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            prop_device_id,
            event_name,
            raw_event_id,
            COALESCE(
                IF(
                    product_code IS NULL OR product_code = '',
                    NULL,
                    CONCAT('product:', product_code)
                ),
                IF(
                    post_code IS NULL OR post_code = '',
                    NULL,
                    CONCAT('post:', post_code)
                )
            ) AS item_key
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND prop_device_id IS NOT NULL
            AND prop_device_id != ''
    ),
    -- 提取 feeds 曝光 item，并按 raw_event_id + item_key 去重。
    exposure_events AS (
        SELECT
            dt,
            prop_device_id,
            raw_event_id,
            item_key
        FROM base
        WHERE event_name IN ({CONTENT_ITEM_EXPOSURE_EVENTS})
            AND raw_event_id IS NOT NULL
            AND item_key IS NOT NULL
        GROUP BY dt, prop_device_id, raw_event_id, item_key
    ),
    -- 提取详情点击 item，并按 raw_event_id + item_key 去重。
    click_events AS (
        SELECT
            dt,
            prop_device_id,
            raw_event_id,
            item_key
        FROM base
        WHERE event_name IN ({CONTENT_ITEM_CLICK_EVENTS})
            AND raw_event_id IS NOT NULL
            AND item_key IS NOT NULL
        GROUP BY dt, prop_device_id, raw_event_id, item_key
    ),
    -- 汇总设备级曝光 item 数。
    exposure_agg AS (
        SELECT
            dt,
            prop_device_id,
            COUNT(*) AS exposure_item_count
        FROM exposure_events
        GROUP BY dt, prop_device_id
    ),
    -- 汇总设备级点击 item 数。
    click_agg AS (
        SELECT
            dt,
            prop_device_id,
            COUNT(*) AS click_item_count
        FROM click_events
        GROUP BY dt, prop_device_id
    ),
    -- 收敛所有设备键，避免单侧指标漏行。
    metric_keys AS (
        SELECT dt, prop_device_id FROM exposure_agg
        UNION DISTINCT
        SELECT dt, prop_device_id FROM click_agg
    )
    SELECT
        k.dt,
        k.prop_device_id,
        COALESCE(e.exposure_item_count, 0) AS exposure_item_count,
        COALESCE(c.click_item_count, 0) AS click_item_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM metric_keys k
    LEFT JOIN exposure_agg e
        ON k.dt = e.dt
        AND k.prop_device_id = e.prop_device_id
    LEFT JOIN click_agg c
        ON k.dt = c.dt
        AND k.prop_device_id = c.prop_device_id;
    """
    logging.info("开始处理: dws_content_item_device_daily")
    client.query(query).result()
    logging.info(f"dws_content_item_device_daily 刷新完成, 处理日期: {dates}")


if __name__ == "__main__":
    log_path = configure_logging()
    start_time = datetime.now(timezone.utc)

    dates = get_dws_content_item_device_daily_dates_to_process()
    if not dates:
        logging.info("没有新的 dws_content_item_device_daily 数据需要处理")
        logging.info(f"日志文件: {log_path}")
        raise SystemExit(0)

    logging.info(f"dws_content_item_device_daily 待处理日期: {dates}")
    run_dws_content_item_device_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"dws_content_item_device_daily 执行完成, 耗时: {elapsed:.1f} 秒")
    logging.info(f"日志文件: {log_path}")
