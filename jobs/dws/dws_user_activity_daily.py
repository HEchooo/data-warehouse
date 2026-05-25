from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
V3_DATASET_ID = "v3_decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_user_activity_daily"
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


def service_change_ts_expr() -> str:
    """
    Datastream 源表优先用 source_timestamp 判断最近变更；
    缺失时回退到 gmt_update / gmt_create，避免漏掉最近同步的新行。
    """
    return (
        "COALESCE("
        "TIMESTAMP_MILLIS(datastream_metadata.source_timestamp), "
        "TIMESTAMP(COALESCE(gmt_update, gmt_create), 'UTC')"
        ")"
    )


def get_user_activity_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    留存用户活动 DWS 同时依赖用户活跃、事件行为和服务端行为。

    规则：
    - 目标表不存在或为空时，按 dws_user_daily 历史业务日全量初始化
    - 目标表已存在时：
      - 当天用户活跃或行为变化重刷当天分区
      - dws_user_daily 的未来活跃变化回刷 `dt-1 / dt-7`
    """
    event_ts = event_ts_expr()
    service_ts = service_ts_expr()
    service_change_ts = service_change_ts_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        WHERE dt IS NOT NULL
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    WITH
    -- 最近 6 小时更新的已登录活跃日期，直接影响 cohort 用户集合。
    changed_user_dates AS (
        SELECT DISTINCT dt AS source_dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND dt IS NOT NULL
    ),
    -- 最近 6 小时更新的登录事件业务日，直接影响时长、搜索、商详、衣橱曝光等行为特征。
    changed_event_dates AS (
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS source_dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
            AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
    ),
    -- 最近 6 小时试穿服务端行为业务日。
    changed_tryon_dates AS (
        SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS source_dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE {service_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND task_type IN ('TRY_ON', 'OUTFIT')
    ),
    -- 最近 6 小时创建模特服务端行为业务日。
    changed_model_dates AS (
        SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS source_dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE {service_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND task_type = 'MODEL'
    ),
    -- 最近 6 小时关注服务端行为业务日。
    changed_follow_dates AS (
        SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS source_dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_star_follow`
        WHERE {service_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 最近 6 小时衣橱服务端行为业务日，影响衣橱渗透特征。
    changed_closet_service_dates AS (
        SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS source_dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_fashion_closet`
        WHERE {service_change_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND user_id IS NOT NULL
            AND user_id != ''
            AND type IN ('SINGLE', 'OUTFIT')
    ),
    -- 当天行为变化直接重刷当天分区。
    direct_dates AS (
        SELECT source_dt AS dt
        FROM changed_user_dates
        UNION DISTINCT
        SELECT source_dt AS dt
        FROM changed_event_dates
        UNION DISTINCT
        SELECT source_dt AS dt
        FROM changed_tryon_dates
        UNION DISTINCT
        SELECT source_dt AS dt
        FROM changed_model_dates
        UNION DISTINCT
        SELECT source_dt AS dt
        FROM changed_follow_dates
        UNION DISTINCT
        SELECT source_dt AS dt
        FROM changed_closet_service_dates
    ),
    -- 次留 / 7 留依赖未来活跃变化，需要把未来活跃回推到更早 cohort 日期。
    retention_dates AS (
        SELECT DATE_SUB(source_dt, INTERVAL 1 DAY) AS dt
        FROM changed_user_dates
        UNION DISTINCT
        SELECT DATE_SUB(source_dt, INTERVAL 7 DAY) AS dt
        FROM changed_user_dates
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt FROM direct_dates
        UNION DISTINCT
        SELECT dt FROM retention_dates
    )
    WHERE dt IS NOT NULL
        AND dt <= CURRENT_DATE('{TORONTO_TZ}')
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_user_activity_daily(dates: list[date]) -> None:
    """
    产出已登录用户留存活动事实层。

    粒度：
    - dt × user_id

    指标：
    - is_new_user：是否首活日
    - is_next_day_retained / is_day_7_retained：未来观察日回访标记
    - duration_sec：当日停留时长
    - followed_account_count：当日关注的去重账号数
    - ai_tryon_count：当日 task_type IN ('TRY_ON','OUTFIT') 的服务端任务行数
    - viewed_product_detail / used_search / used_ai_tryon /
      created_model / followed_account / closet_penetrated：行为特征布尔值
    - exposed_home_{feeds|star|magazine|brand} /
      clicked_home_{feeds|star|magazine|brand}：首页 4 个模块曝光与点击布尔值
    """
    if not dates:
        logging.info("没有新的 dws_user_activity_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    min_date = min(dates)
    max_date = max(dates)
    event_ts = event_ts_expr()
    service_ts = service_ts_expr()

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        user_id,
        first_active_date,
        is_new_user,
        is_next_day_observation_started,
        is_day_7_observation_started,
        is_next_day_retained,
        is_day_7_retained,
        duration_sec,
        followed_account_count,
        ai_tryon_count,
        viewed_product_detail,
        used_search,
        used_ai_tryon,
        created_model,
        followed_account,
        closet_penetrated,
        exposed_home_feeds,
        clicked_home_feeds,
        exposed_home_star,
        clicked_home_star,
        exposed_home_magazine,
        clicked_home_magazine,
        exposed_home_brand,
        clicked_home_brand,
        update_time
    )
    WITH
    -- 当前批次目标日期内的活跃登录用户 cohort。
    cohort_users AS (
        SELECT
            dt,
            prop_user_id AS user_id,
            MIN(first_active_date) AS first_active_date
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        WHERE dt IN ({dates_str})
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
        GROUP BY dt, user_id
    ),
    -- 目标日期向后 7 天内的活跃登录用户集合，用于留存判定。
    future_active_users AS (
        SELECT DISTINCT
            dt,
            prop_user_id AS user_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        WHERE dt BETWEEN DATE_ADD(DATE '{min_date}', INTERVAL 1 DAY)
            AND LEAST(
                DATE_ADD(DATE '{max_date}', INTERVAL 7 DAY),
                CURRENT_DATE('{TORONTO_TZ}')
            )
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
    ),
    -- 目标日期内的已登录事件底表，提供停留时长、商详和搜索行为。
    logged_in_event_base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            prop_user_id AS user_id,
            event_name,
            COALESCE(raw_event_id, hash_id) AS event_id,
            CAST(args_session_duration AS FLOAT64) AS duration_ms
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE(logAt_timestamp) BETWEEN DATE_SUB(DATE '{min_date}', INTERVAL 1 DAY)
            AND DATE_ADD(DATE '{max_date}', INTERVAL 1 DAY)
            AND DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
    ),
    -- app_launch 停留时长去重层：同一原始事件只记一次。
    user_duration_daily AS (
        SELECT
            dt,
            user_id,
            CAST(ROUND(COALESCE(SUM(duration_ms), 0) / 1000.0) AS INT64) AS duration_sec
        FROM (
            SELECT
                dt,
                user_id,
                event_id,
                MAX(duration_ms) AS duration_ms
            FROM logged_in_event_base
            WHERE event_name = 'app_launch'
                AND event_id IS NOT NULL
                AND duration_ms IS NOT NULL
            GROUP BY dt, user_id, event_id
        )
        GROUP BY dt, user_id
    ),
    -- 事件侧行为聚合层：统计商详、搜索、衣橱曝光及首页 4 个模块曝光/点击是否发生过。
    event_behavior_daily AS (
        SELECT
            dt,
            user_id,
            MAX(IF(event_name = 'v_product_detail', 1, 0)) AS viewed_product_detail,
            MAX(IF(event_name IN ('c_home_search', 'c_shop_search'), 1, 0)) AS used_search,
            MAX(IF(event_name = 'v_closet', 1, 0)) AS viewed_closet,
            MAX(IF(event_name = 'v_home_feeds', 1, 0)) AS exposed_home_feeds,
            MAX(IF(event_name = 'c_home_feeds', 1, 0)) AS clicked_home_feeds,
            MAX(IF(event_name = 'v_home_star', 1, 0)) AS exposed_home_star,
            MAX(IF(event_name = 'c_home_star', 1, 0)) AS clicked_home_star,
            MAX(IF(event_name = 'v_home_magazine', 1, 0)) AS exposed_home_magazine,
            MAX(IF(event_name = 'c_home_magazine', 1, 0)) AS clicked_home_magazine,
            MAX(IF(event_name = 'v_home_brand', 1, 0)) AS exposed_home_brand,
            MAX(IF(event_name = 'c_home_brand', 1, 0)) AS clicked_home_brand
        FROM logged_in_event_base
        GROUP BY dt, user_id
    ),
    -- 服务端试穿行为聚合层：按天去重到用户，并统计当日试穿任务次数。
    tryon_behavior_daily AS (
        SELECT
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            COALESCE(
                NULLIF(JSON_VALUE(request_param, '$.user_id'), ''),
                NULLIF(JSON_VALUE(request_param, '$.userId'), ''),
                NULLIF(CAST(user_id AS STRING), '')
            ) AS user_id,
            COUNT(1) AS ai_tryon_count
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE DATE({service_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND task_type IN ('TRY_ON', 'OUTFIT')
        GROUP BY dt, user_id
    ),
    -- 服务端创建模特行为聚合层：按天去重到用户。
    model_behavior_daily AS (
        SELECT
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            COALESCE(
                NULLIF(JSON_VALUE(request_param, '$.user_id'), ''),
                NULLIF(JSON_VALUE(request_param, '$.userId'), ''),
                NULLIF(CAST(user_id AS STRING), '')
            ) AS user_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE DATE({service_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND task_type = 'MODEL'
        GROUP BY dt, user_id
),
    -- 服务端关注行为聚合层：按天去重到用户。
    follow_behavior_daily AS (
        SELECT
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            CAST(user_id AS STRING) AS user_id,
            COUNT(DISTINCT CAST(star_user_id AS STRING)) AS followed_account_count
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_star_follow`
        WHERE DATE({service_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND user_id IS NOT NULL
            AND CAST(user_id AS STRING) != ''
            AND star_user_id IS NOT NULL
            AND CAST(star_user_id AS STRING) != ''
        GROUP BY dt, user_id
    ),
    -- 服务端衣橱使用行为聚合层：按天去重到用户。
    closet_use_behavior_daily AS (
        SELECT
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            CAST(user_id AS STRING) AS user_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_fashion_closet`
        WHERE DATE({service_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND user_id IS NOT NULL
            AND user_id != ''
            AND type IN ('SINGLE', 'OUTFIT')
        GROUP BY dt, user_id
    )
    SELECT
        c.dt,
        c.user_id,
        c.first_active_date,
        c.first_active_date = c.dt AS is_new_user,
        DATE_ADD(c.dt, INTERVAL 1 DAY) <= CURRENT_DATE('{TORONTO_TZ}')
            AS is_next_day_observation_started,
        DATE_ADD(c.dt, INTERVAL 7 DAY) <= CURRENT_DATE('{TORONTO_TZ}')
            AS is_day_7_observation_started,
        n1.user_id IS NOT NULL AS is_next_day_retained,
        n7.user_id IS NOT NULL AS is_day_7_retained,
        COALESCE(d.duration_sec, 0) AS duration_sec,
        COALESCE(f.followed_account_count, 0) AS followed_account_count,
        COALESCE(t.ai_tryon_count, 0) AS ai_tryon_count,
        COALESCE(e.viewed_product_detail, 0) AS viewed_product_detail,
        COALESCE(e.used_search, 0) AS used_search,
        IF(t.user_id IS NOT NULL, 1, 0) AS used_ai_tryon,
        IF(m.user_id IS NOT NULL, 1, 0) AS created_model,
        IF(COALESCE(f.followed_account_count, 0) > 0, 1, 0) AS followed_account,
        IF(
            COALESCE(e.viewed_closet, 0) = 1
            AND u.user_id IS NOT NULL,
            1,
            0
        ) AS closet_penetrated,
        COALESCE(e.exposed_home_feeds, 0) AS exposed_home_feeds,
        COALESCE(e.clicked_home_feeds, 0) AS clicked_home_feeds,
        COALESCE(e.exposed_home_star, 0) AS exposed_home_star,
        COALESCE(e.clicked_home_star, 0) AS clicked_home_star,
        COALESCE(e.exposed_home_magazine, 0) AS exposed_home_magazine,
        COALESCE(e.clicked_home_magazine, 0) AS clicked_home_magazine,
        COALESCE(e.exposed_home_brand, 0) AS exposed_home_brand,
        COALESCE(e.clicked_home_brand, 0) AS clicked_home_brand,
        CURRENT_TIMESTAMP() AS update_time
    FROM cohort_users c
    LEFT JOIN future_active_users n1
        ON n1.user_id = c.user_id
        AND n1.dt = DATE_ADD(c.dt, INTERVAL 1 DAY)
    LEFT JOIN future_active_users n7
        ON n7.user_id = c.user_id
        AND n7.dt = DATE_ADD(c.dt, INTERVAL 7 DAY)
    LEFT JOIN user_duration_daily d
        ON d.dt = c.dt
        AND d.user_id = c.user_id
    LEFT JOIN event_behavior_daily e
        ON e.dt = c.dt
        AND e.user_id = c.user_id
    LEFT JOIN tryon_behavior_daily t
        ON t.dt = c.dt
        AND t.user_id = c.user_id
    LEFT JOIN model_behavior_daily m
        ON m.dt = c.dt
        AND m.user_id = c.user_id
    LEFT JOIN follow_behavior_daily f
        ON f.dt = c.dt
        AND f.user_id = c.user_id
    LEFT JOIN closet_use_behavior_daily u
        ON u.dt = c.dt
        AND u.user_id = c.user_id;
    """
    logging.info("开始处理: dws_user_activity_daily")
    job = client.query(query)
    job.result()
    logging.info("dws_user_activity_daily 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_user_activity_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 dws_user_activity_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_user_activity_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dws_user_activity_daily 完成, 耗时: %.1f 秒", elapsed)
