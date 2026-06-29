from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from jobs.event_activity_rules import PASSIVE_ACTIVITY_EXCLUDED_EVENTS_SQL


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_registered_app_device_daily"
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
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


def get_registered_app_device_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    已注册 app 设备事实同时依赖：
    - dws_device_daily 的 app 设备活跃事实
    - dwd_event_log 的 app 已注册用户事件

    规则：
    - 目标表不存在或为空时，按 dws_device_daily 的 app 日期全量初始化
    - 目标表已存在时，合并最近 6 小时内设备事实和注册事件变化涉及的 dt
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
        WHERE platform != 'h5'
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    WITH
    -- 最近变更的 app 设备事实日期。
    changed_device_dates AS (
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
        WHERE platform != 'h5'
            AND update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 最近变更的 app 已注册用户事件日期。
    changed_registered_event_dates AS (
        SELECT DISTINCT
            DATE(
                TIMESTAMP(
                    DATETIME(logAt_timestamp, 'UTC'),
                    COALESCE(NULLIF(prop_timezone, ''), 'UTC')
                ),
                '{TORONTO_TZ}'
            ) AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND (prop_url IS NULL OR prop_url = '')
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
            AND prop_device_id IS NOT NULL
            AND prop_device_id != ''
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt FROM changed_device_dates
        UNION DISTINCT
        SELECT dt FROM changed_registered_event_dates
    )
    WHERE dt IS NOT NULL
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_registered_app_device_daily(dates: list[date]) -> None:
    """
    产出 app 设备日事实。

    粒度：
    - dt × prop_device_id

    语义：
    - 所有 app 活跃设备都保留一行，用于投资人报表的新增 / 活跃设备数
    - 已注册用户事件的设备级时长 / 内容曝光 / 内容点击下沉到同一张 DWS
    """
    if not dates:
        logging.info("没有新的 dws_registered_app_device_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    max_date = max(dates)

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        prop_device_id,
        first_app_active_dt,
        is_new_app_device,
        has_registered_user_activity,
        registered_duration_sec,
        registered_content_exposure_count,
        registered_content_click_count,
        update_time
    )
    WITH
    -- 本批待处理日期列表。
    target_dates AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 全量 app 设备的首活日期。
    app_device_first_active AS (
        SELECT
            prop_device_id,
            MIN(dt) AS first_app_active_dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
        WHERE dt <= DATE '{max_date}'
            AND platform != 'h5'
            AND prop_device_id IS NOT NULL
            AND prop_device_id != ''
        GROUP BY prop_device_id
    ),
    -- 目标日期内所有 app 活跃设备。
    app_active_devices AS (
        SELECT
            d.dt,
            d.prop_device_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily` d
        INNER JOIN target_dates t
            ON d.dt = t.dt
        WHERE d.platform != 'h5'
            AND d.prop_device_id IS NOT NULL
            AND d.prop_device_id != ''
        GROUP BY d.dt, d.prop_device_id
    ),
    -- 目标日期内 app 已注册用户事件基表。
    registered_event_base AS (
        SELECT
            DATE(
                TIMESTAMP(
                    DATETIME(logAt_timestamp, 'UTC'),
                    COALESCE(NULLIF(prop_timezone, ''), 'UTC')
                ),
                '{TORONTO_TZ}'
            ) AS dt,
            prop_device_id,
            hash_id,
            raw_event_id,
            event_name,
            args_session_duration,
            product_code,
            post_code
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE(
                TIMESTAMP(
                    DATETIME(logAt_timestamp, 'UTC'),
                    COALESCE(NULLIF(prop_timezone, ''), 'UTC')
                ),
                '{TORONTO_TZ}'
            ) IN ({dates_str})
            AND (prop_url IS NULL OR prop_url = '')
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
            AND prop_device_id IS NOT NULL
            AND prop_device_id != ''
            AND event_name NOT IN ({PASSIVE_ACTIVITY_EXCLUDED_EVENTS_SQL})
    ),
    -- 设备级注册用户停留时长。
    registered_duration_metrics AS (
        SELECT
            dt,
            prop_device_id,
            COALESCE(SUM(duration_ms), 0) / 1000.0 AS registered_duration_sec
        FROM (
            SELECT
                dt,
                prop_device_id,
                COALESCE(raw_event_id, hash_id) AS duration_event_id,
                MAX(CAST(args_session_duration AS FLOAT64)) AS duration_ms
            FROM registered_event_base
            WHERE event_name = 'app_launch'
                AND args_session_duration IS NOT NULL
                AND COALESCE(raw_event_id, hash_id) IS NOT NULL
            GROUP BY dt, prop_device_id, duration_event_id
        )
        GROUP BY dt, prop_device_id
    ),
    -- 设备级内容 item 曝光 / 点击量。
    registered_content_metrics AS (
        SELECT
            dt,
            prop_device_id,
            COUNTIF(event_group = 'exposure') AS registered_content_exposure_count,
            COUNTIF(event_group = 'click') AS registered_content_click_count
        FROM (
            SELECT
                dt,
                prop_device_id,
                event_group,
                raw_event_id,
                item_key
            FROM (
                SELECT
                    dt,
                    prop_device_id,
                    'exposure' AS event_group,
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
                FROM registered_event_base
                WHERE event_name IN (
                    'v_home_feeds',
                    'v_shop_feeds',
                    'v_star_post_feeds',
                    'v_brand_post_feeds',
                    'v_kol_post_feeds',
                    'v_magazine_post_feeds'
                )
                    AND raw_event_id IS NOT NULL

                UNION ALL

                SELECT
                    dt,
                    prop_device_id,
                    'click' AS event_group,
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
                FROM registered_event_base
                WHERE event_name IN (
                    'v_product_detail',
                    'v_star_post_detail',
                    'v_magazine_post_detail',
                    'v_brand_post_detail',
                    'v_kol_post_detail'
                )
                    AND raw_event_id IS NOT NULL
            )
            WHERE item_key IS NOT NULL
            GROUP BY dt, prop_device_id, event_group, raw_event_id, item_key
        )
        GROUP BY dt, prop_device_id
    ),
    -- 标记哪些 app 设备当天存在已注册用户活动。
    registered_device_presence AS (
        SELECT
            dt,
            prop_device_id
        FROM registered_event_base
        GROUP BY dt, prop_device_id
    )
    SELECT
        d.dt,
        d.prop_device_id,
        f.first_app_active_dt,
        f.first_app_active_dt = d.dt AS is_new_app_device,
        p.prop_device_id IS NOT NULL AS has_registered_user_activity,
        COALESCE(u.registered_duration_sec, 0) AS registered_duration_sec,
        COALESCE(c.registered_content_exposure_count, 0)
            AS registered_content_exposure_count,
        COALESCE(c.registered_content_click_count, 0)
            AS registered_content_click_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM app_active_devices d
    LEFT JOIN app_device_first_active f
        ON d.prop_device_id = f.prop_device_id
    LEFT JOIN registered_device_presence p
        ON d.dt = p.dt
        AND d.prop_device_id = p.prop_device_id
    LEFT JOIN registered_duration_metrics u
        ON d.dt = u.dt
        AND d.prop_device_id = u.prop_device_id
    LEFT JOIN registered_content_metrics c
        ON d.dt = c.dt
        AND d.prop_device_id = c.prop_device_id
    """

    logging.info("开始处理: %s", TABLE_NAME)
    job = client.query(query)
    job.result()
    logging.info("%s 刷新完成, 处理日期: %s", TABLE_NAME, dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_registered_app_device_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 dws_registered_app_device_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_registered_app_device_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
