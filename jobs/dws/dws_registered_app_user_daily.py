from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_registered_app_user_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
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


def get_registered_app_user_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    已注册 app 用户事实同时依赖：
    - dws_user_daily 的 app 活跃用户与留存 cohort
    - dwd_event_log 的商详页事件

    规则：
    - 目标表不存在或为空时，按 dws_user_daily 的 app 日期全量初始化
    - 目标表已存在时，合并最近 6 小时内用户事实与商详事件涉及的 dt
    - 用户日活变更会额外影响 `dt-1` / `dt-7` 的留存结果，需要一并回刷
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        WHERE platform != 'h5'
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    WITH
    -- 最近变更的 app 活跃用户日期。
    changed_user_dates AS (
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        WHERE platform != 'h5'
            AND update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 留存观察日成熟会反向影响前 1 天和前 7 天的 cohort。
    affected_retention_dates AS (
        SELECT DATE_SUB(dt, INTERVAL 1 DAY) AS dt
        FROM changed_user_dates

        UNION DISTINCT

        SELECT DATE_SUB(dt, INTERVAL 7 DAY) AS dt
        FROM changed_user_dates
    ),
    -- 最近变更的 app 已注册用户商详页事件日期。
    changed_product_detail_dates AS (
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
            AND event_name = 'v_product_detail'
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt FROM changed_user_dates
        UNION DISTINCT
        SELECT dt FROM affected_retention_dates
        UNION DISTINCT
        SELECT dt FROM changed_product_detail_dates
    )
    WHERE dt IS NOT NULL
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_registered_app_user_daily(dates: list[date]) -> None:
    """
    产出 app 已注册用户日事实。

    粒度：
    - dt × prop_user_id

    语义：
    - app 活跃注册用户 cohort
    - 1 日 / 7 日留存标记
    - 商详页 PV 明细
    """
    if not dates:
        logging.info("没有新的 dws_registered_app_user_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    min_date = min(dates)
    max_date = max(dates)

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        prop_user_id,
        first_app_active_dt,
        is_new_app_user,
        is_next_day_observation_started,
        is_day_7_observation_started,
        is_next_day_retained,
        is_day_7_retained,
        product_detail_exposure_pv,
        update_time
    )
    WITH
    -- 本批待处理日期列表。
    target_dates AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- app 注册用户的首活日期。
    app_user_first_active AS (
        SELECT
            prop_user_id,
            MIN(dt) AS first_app_active_dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        WHERE dt <= DATE '{max_date}'
            AND platform != 'h5'
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
        GROUP BY prop_user_id
    ),
    -- 为 1 日 / 7 日留存准备带观察窗口的活跃用户全集。
    app_active_users AS (
        SELECT
            dt,
            prop_user_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        WHERE dt BETWEEN DATE '{min_date}' AND LEAST(
            DATE_ADD(DATE '{max_date}', INTERVAL 7 DAY),
            CURRENT_DATE('{TORONTO_TZ}')
        )
            AND platform != 'h5'
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
        GROUP BY dt, prop_user_id
    ),
    -- 当前批次真正需要产出的 app 活跃注册用户 cohort。
    target_active_users AS (
        SELECT
            u.dt,
            u.prop_user_id
        FROM app_active_users u
        INNER JOIN target_dates t
            ON u.dt = t.dt
    ),
    -- app 已注册用户商详页 PV。
    product_detail_metrics AS (
        SELECT
            DATE(
                TIMESTAMP(
                    DATETIME(logAt_timestamp, 'UTC'),
                    COALESCE(NULLIF(prop_timezone, ''), 'UTC')
                ),
                '{TORONTO_TZ}'
            ) AS dt,
            prop_user_id,
            COUNT(DISTINCT COALESCE(raw_event_id, hash_id)) AS product_detail_exposure_pv
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
            AND event_name = 'v_product_detail'
            AND COALESCE(raw_event_id, hash_id) IS NOT NULL
        GROUP BY dt, prop_user_id
    )
    SELECT
        t.dt,
        t.prop_user_id,
        f.first_app_active_dt,
        f.first_app_active_dt = t.dt AS is_new_app_user,
        DATE_ADD(t.dt, INTERVAL 1 DAY) <= CURRENT_DATE('{TORONTO_TZ}')
            AS is_next_day_observation_started,
        DATE_ADD(t.dt, INTERVAL 7 DAY) <= CURRENT_DATE('{TORONTO_TZ}')
            AS is_day_7_observation_started,
        n1.prop_user_id IS NOT NULL AS is_next_day_retained,
        n7.prop_user_id IS NOT NULL AS is_day_7_retained,
        COALESCE(p.product_detail_exposure_pv, 0) AS product_detail_exposure_pv,
        CURRENT_TIMESTAMP() AS update_time
    FROM target_active_users t
    LEFT JOIN app_user_first_active f
        ON t.prop_user_id = f.prop_user_id
    LEFT JOIN app_active_users n1
        ON t.prop_user_id = n1.prop_user_id
        AND n1.dt = DATE_ADD(t.dt, INTERVAL 1 DAY)
    LEFT JOIN app_active_users n7
        ON t.prop_user_id = n7.prop_user_id
        AND n7.dt = DATE_ADD(t.dt, INTERVAL 7 DAY)
    LEFT JOIN product_detail_metrics p
        ON t.dt = p.dt
        AND t.prop_user_id = p.prop_user_id
    """

    logging.info("开始处理: %s", TABLE_NAME)
    job = client.query(query)
    job.result()
    logging.info("%s 刷新完成, 处理日期: %s", TABLE_NAME, dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_registered_app_user_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 dws_registered_app_user_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_registered_app_user_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
