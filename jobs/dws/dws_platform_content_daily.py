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
V3_DATASET_ID = "v3_decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_platform_content_daily"
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6

HOME_EXPOSURE_EVENT_NAMES = (
    "v_home_star",
    "v_home_magazine",
    "v_home_brand",
    "v_home_feeds",
)
POST_CONTENT_EXPOSURE_EVENT_NAMES = (
    "v_star_post_detail",
    "v_magazine_post_detail",
    "v_brand_post_detail",
    "v_kol_post_detail",
)
READ_EVENT_NAMES = (
    "r_star_post_detail",
    "r_magazine_post_detail",
    "r_brand_post_detail",
    "r_kol_post_detail",
)
PRODUCT_CLICK_EVENT_NAMES = (
    "c_go_product_detail",
    "c_open_sku_popup",
    "c_third_party_goods",
)
COLUMN_EXPOSURE_EVENT_NAMES = (
    "v_star_post_detail",
    "v_magazine_post_detail",
    "v_brand_post_detail",
    "v_kol_post_detail",
    "v_star_post_feeds",
    "v_brand_post_feeds",
)
TRYON_TASK_TYPES = (
    "QUICK_TRY_ON",
    "TRY_ON",
    "OUTFIT",
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


def string_values_to_sql_list(values: tuple[str, ...]) -> str:
    return ", ".join([f"'{value}'" for value in values])


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


def platform_expr() -> str:
    return (
        "CASE "
        "WHEN prop_url IS NOT NULL AND prop_url != '' THEN 'H5' "
        "WHEN LOWER(prop_os) = 'ios' THEN 'iOS' "
        "WHEN LOWER(prop_os) IN ('android', 'harmony') THEN 'Android' "
        "ELSE 'unknown' "
        "END"
    )


def get_platform_content_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    平台内容 DWS 直接依赖事件表和服务端事实表，不复用共享水位。

    规则：
    - 目标表不存在或为空时，全量初始化所有可能产生平台日报的业务日
    - 目标表已存在时，只取最近 6 小时上游变更映射出的受影响业务日
    """
    event_ts = event_ts_expr()
    service_ts = service_ts_expr()
    tryon_task_types_sql = string_values_to_sql_list(TRYON_TASK_TYPES)
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        WITH
        -- 事件层存在任意数据的业务日；用于支撑平台曝光、内容曝光和 DAU。
        event_dates AS (
            SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
            FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        ),
        -- 点赞事实业务日。
        like_dates AS (
            SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS dt
            FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_post_like`
        ),
        -- 关注事实业务日。
        follow_dates AS (
            SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS dt
            FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_star_follow`
        ),
        -- 平台级试穿任务业务日。
        tryon_dates AS (
            SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS dt
            FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
            WHERE task_type IN ({tryon_task_types_sql})
        )
        SELECT DISTINCT dt
        FROM (
            SELECT dt FROM event_dates
            UNION DISTINCT
            SELECT dt FROM like_dates
            UNION DISTINCT
            SELECT dt FROM follow_dates
            UNION DISTINCT
            SELECT dt FROM tryon_dates
        )
        WHERE dt IS NOT NULL
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    WITH
    -- 最近 6 小时事件层变更映射出的业务日。
    changed_event_dates AS (
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 最近 6 小时服务端点赞变更映射出的业务日。
    changed_like_dates AS (
        SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_post_like`
        WHERE {service_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 最近 6 小时服务端关注变更映射出的业务日。
    changed_follow_dates AS (
        SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_star_follow`
        WHERE {service_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 最近 6 小时平台级试穿任务变更映射出的业务日。
    changed_tryon_dates AS (
        SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE {service_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND task_type IN ({tryon_task_types_sql})
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt FROM changed_event_dates
        UNION DISTINCT
        SELECT dt FROM changed_like_dates
        UNION DISTINCT
        SELECT dt FROM changed_follow_dates
        UNION DISTINCT
        SELECT dt FROM changed_tryon_dates
    )
    WHERE dt IS NOT NULL
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_platform_content_daily(dates: list[date]) -> None:
    """
    产出平台内容主题核心事实层。

    粒度：
    - dt × platform

    指标：
    - 平台曝光、内容曝光、商详曝光、商品点击、阅读、DAU
    - 点赞 / 关注 / 试穿的服务端成功事实
    """
    if not dates:
        logging.info("没有新的 dws_platform_content_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    event_ts = event_ts_expr()
    service_ts = service_ts_expr()
    platform_case = platform_expr()
    home_exposure_events_sql = string_values_to_sql_list(HOME_EXPOSURE_EVENT_NAMES)
    post_content_exposure_events_sql = string_values_to_sql_list(
        POST_CONTENT_EXPOSURE_EVENT_NAMES
    )
    read_events_sql = string_values_to_sql_list(READ_EVENT_NAMES)
    product_click_events_sql = string_values_to_sql_list(PRODUCT_CLICK_EVENT_NAMES)
    column_exposure_events_sql = string_values_to_sql_list(COLUMN_EXPOSURE_EVENT_NAMES)
    tryon_task_types_sql = string_values_to_sql_list(TRYON_TASK_TYPES)

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
        read_uv,
        like_total_count,
        like_uv,
        follow_total_count,
        follow_column_count,
        follow_uv,
        dau_uv,
        tryon_total_count,
        tryon_uv,
        column_exposure_pv,
        update_time
    )
    WITH
    -- 基础事件明细：统一多伦多日期、平台和原始事件字段。
    base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            {event_ts} AS event_ts,
            {platform_case} AS platform,
            event_name,
            prop_user_id,
            raw_event_id,
            hash_id,
            args_star,
            args_magazine,
            args_brand,
            args_blogger
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND event_name NOT IN ({PASSIVE_ACTIVITY_EXCLUDED_EVENTS_SQL})
    ),
    -- 已登录事件标准层：统一用户键、事件去重键和专栏维度。
    events AS (
        SELECT
            dt,
            platform,
            event_name,
            NULLIF(prop_user_id, '') AS user_id,
            COALESCE(raw_event_id, hash_id) AS event_id,
            hash_id,
            COALESCE(
                IF(NULLIF(args_star, '') IS NULL, NULL, CONCAT('star:', NULLIF(args_star, ''))),
                IF(
                    NULLIF(args_magazine, '') IS NULL,
                    NULL,
                    CONCAT('magazine:', NULLIF(args_magazine, ''))
                ),
                IF(NULLIF(args_brand, '') IS NULL, NULL, CONCAT('brand:', NULLIF(args_brand, ''))),
                IF(
                    NULLIF(args_blogger, '') IS NULL,
                    NULL,
                    CONCAT('kol:', NULLIF(args_blogger, ''))
                )
            ) AS column_id
        FROM base
        WHERE NULLIF(prop_user_id, '') IS NOT NULL
    ),
    -- 登录用户平台事件：用于把服务端行为唯一归因到同日平台。
    logged_in_platform_events AS (
        SELECT DISTINCT
            dt,
            event_ts,
            platform,
            NULLIF(prop_user_id, '') AS user_id
        FROM base
        WHERE NULLIF(prop_user_id, '') IS NOT NULL
    ),
    -- 日活：按任意已登录事件去重用户统计 DAU。
    dau_daily AS (
        SELECT
            dt,
            platform,
            COUNT(DISTINCT user_id) AS dau_uv
        FROM events
        GROUP BY dt, platform
    ),
    -- 平台曝光：统计首页入口曝光 UV。
    platform_daily AS (
        SELECT
            dt,
            platform,
            COUNT(
                DISTINCT IF(event_name IN ({home_exposure_events_sql}), user_id, NULL)
            ) AS platform_exposure_uv
        FROM events
        GROUP BY dt, platform
    ),
    -- 内容与商品行为日聚合：统一产出曝光、商品点击和专栏曝光等指标。
    exposure_daily AS (
        SELECT
            dt,
            platform,
            COUNT(
                DISTINCT IF(
                    event_name IN ({post_content_exposure_events_sql}),
                    user_id,
                    NULL
                )
            ) AS content_exposure_uv,
            COUNT(
                DISTINCT IF(
                    event_name IN ({post_content_exposure_events_sql}),
                    hash_id,
                    NULL
                )
            ) AS content_exposure_pv,
            COUNT(
                DISTINCT IF(
                    event_name = 'v_product_detail',
                    user_id,
                    NULL
                )
            ) AS product_detail_exposure_uv,
            COUNT(
                DISTINCT IF(
                    event_name = 'v_product_detail' AND event_id IS NOT NULL,
                    event_id,
                    NULL
                )
            ) AS product_detail_exposure_pv,
            COUNT(
                DISTINCT IF(
                    event_name IN ({product_click_events_sql}),
                    user_id,
                    NULL
                )
            ) AS product_click_uv,
            COUNT(
                DISTINCT IF(
                    event_name IN ({product_click_events_sql}) AND event_id IS NOT NULL,
                    event_id,
                    NULL
                )
            ) AS product_click_pv,
            COUNT(
                DISTINCT IF(
                    event_name IN ({column_exposure_events_sql}) AND column_id IS NOT NULL,
                    hash_id,
                    NULL
                )
            ) AS column_exposure_pv
        FROM events
        GROUP BY dt, platform
    ),
    -- 服务端点赞原始记录：按创建时间统计成功点赞事实。
    like_service_base AS (
        SELECT
            GENERATE_UUID() AS action_id,
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            {service_ts} AS action_ts,
            CAST(post_id AS STRING) AS post_id,
            CAST(user_id AS STRING) AS user_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_post_like`
        WHERE DATE({service_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND post_id IS NOT NULL
            AND user_id IS NOT NULL
            AND CAST(user_id AS STRING) != ''
    ),
    -- 服务端点赞记录按同日最近登录事件归因平台；未命中则记为 unknown。
    like_service_with_platform AS (
        SELECT
            l.dt,
            COALESCE(p.platform, 'unknown') AS platform,
            l.user_id
        FROM like_service_base l
        LEFT JOIN logged_in_platform_events p
            ON p.dt = l.dt
            AND p.user_id = l.user_id
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY l.action_id
            ORDER BY
                IF(p.event_ts IS NULL, 1, 0),
                ABS(TIMESTAMP_DIFF(p.event_ts, l.action_ts, SECOND)),
                IF(p.event_ts <= l.action_ts, 0, 1),
                p.event_ts DESC
        ) = 1
    ),
    -- 服务端点赞日聚合：产出点赞成功次数与点赞用户数。
    like_daily AS (
        SELECT
            dt,
            platform,
            COUNT(*) AS like_total_count,
            COUNT(DISTINCT user_id) AS like_uv
        FROM like_service_with_platform
        GROUP BY dt, platform
    ),
    -- 服务端关注原始记录：按创建时间统计新增关注事实。
    follow_service_base AS (
        SELECT
            GENERATE_UUID() AS action_id,
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            {service_ts} AS action_ts,
            CAST(star_user_id AS STRING) AS column_id,
            CAST(user_id AS STRING) AS user_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_star_follow`
        WHERE DATE({service_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND star_user_id IS NOT NULL
            AND CAST(star_user_id AS STRING) != ''
            AND user_id IS NOT NULL
            AND CAST(user_id AS STRING) != ''
    ),
    -- 服务端关注记录按同日最近登录事件归因平台；未命中则记为 unknown。
    follow_service_with_platform AS (
        SELECT
            f.dt,
            COALESCE(p.platform, 'unknown') AS platform,
            f.column_id,
            f.user_id
        FROM follow_service_base f
        LEFT JOIN logged_in_platform_events p
            ON p.dt = f.dt
            AND p.user_id = f.user_id
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY f.action_id
            ORDER BY
                IF(p.event_ts IS NULL, 1, 0),
                ABS(TIMESTAMP_DIFF(p.event_ts, f.action_ts, SECOND)),
                IF(p.event_ts <= f.action_ts, 0, 1),
                p.event_ts DESC
        ) = 1
    ),
    -- 服务端关注日聚合：产出新增关注次数、专栏数和关注用户数。
    follow_daily AS (
        SELECT
            dt,
            platform,
            COUNT(*) AS follow_total_count,
            COUNT(DISTINCT column_id) AS follow_column_count,
            COUNT(DISTINCT user_id) AS follow_uv
        FROM follow_service_with_platform
        GROUP BY dt, platform
    ),
    -- 服务端试穿原始记录：统计平台级 QUICK_TRY_ON / TRY_ON / OUTFIT 任务。
    tryon_task_base AS (
        SELECT
            GENERATE_UUID() AS action_id,
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            {service_ts} AS action_ts,
            COALESCE(
                NULLIF(JSON_VALUE(request_param, '$.user_id'), ''),
                NULLIF(JSON_VALUE(request_param, '$.userId'), ''),
                NULLIF(CAST(user_id AS STRING), '')
            ) AS user_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE DATE({service_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND task_type IN ({tryon_task_types_sql})
    ),
    -- 服务端试穿记录按同日最近登录事件归因平台；未命中则记为 unknown。
    tryon_task_with_platform AS (
        SELECT
            t.dt,
            COALESCE(p.platform, 'unknown') AS platform,
            t.user_id
        FROM tryon_task_base t
        LEFT JOIN logged_in_platform_events p
            ON p.dt = t.dt
            AND p.user_id = t.user_id
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY t.action_id
            ORDER BY
                IF(p.event_ts IS NULL, 1, 0),
                ABS(TIMESTAMP_DIFF(p.event_ts, t.action_ts, SECOND)),
                IF(p.event_ts <= t.action_ts, 0, 1),
                p.event_ts DESC
        ) = 1
    ),
    -- 服务端试穿日聚合：产出平台级试穿次数与试穿用户数。
    tryon_daily AS (
        SELECT
            dt,
            platform,
            COUNT(*) AS tryon_total_count,
            COUNT(DISTINCT IF(user_id IS NOT NULL, user_id, NULL)) AS tryon_uv
        FROM tryon_task_with_platform
        GROUP BY dt, platform
    ),
    -- 阅读日聚合：保留已登录用户阅读 UV 供终层展示。
    read_daily AS (
        SELECT
            dt,
            platform,
            COUNT(
                DISTINCT IF(
                    event_name IN ({read_events_sql}),
                    user_id,
                    NULL
                )
            ) AS read_uv
        FROM events
        GROUP BY dt, platform
    ),
    -- 日期平台骨架：合并所有指标来源，确保纯服务端平台行也能保留下来。
    date_platform_list AS (
        SELECT dt, platform FROM dau_daily
        UNION DISTINCT
        SELECT dt, platform FROM platform_daily
        UNION DISTINCT
        SELECT dt, platform FROM exposure_daily
        UNION DISTINCT
        SELECT dt, platform FROM like_daily
        UNION DISTINCT
        SELECT dt, platform FROM follow_daily
        UNION DISTINCT
        SELECT dt, platform FROM tryon_daily
        UNION DISTINCT
        SELECT dt, platform FROM read_daily
    )
    SELECT
        d.dt,
        d.platform,
        COALESCE(p.platform_exposure_uv, 0) AS platform_exposure_uv,
        COALESCE(e.content_exposure_uv, 0) AS content_exposure_uv,
        COALESCE(e.content_exposure_pv, 0) AS content_exposure_pv,
        COALESCE(e.product_detail_exposure_uv, 0) AS product_detail_exposure_uv,
        COALESCE(e.product_detail_exposure_pv, 0) AS product_detail_exposure_pv,
        COALESCE(e.product_click_uv, 0) AS product_click_uv,
        COALESCE(e.product_click_pv, 0) AS product_click_pv,
        COALESCE(r.read_uv, 0) AS read_uv,
        COALESCE(l.like_total_count, 0) AS like_total_count,
        COALESCE(l.like_uv, 0) AS like_uv,
        COALESCE(f.follow_total_count, 0) AS follow_total_count,
        COALESCE(f.follow_column_count, 0) AS follow_column_count,
        COALESCE(f.follow_uv, 0) AS follow_uv,
        COALESCE(u.dau_uv, 0) AS dau_uv,
        COALESCE(t.tryon_total_count, 0) AS tryon_total_count,
        COALESCE(t.tryon_uv, 0) AS tryon_uv,
        COALESCE(e.column_exposure_pv, 0) AS column_exposure_pv,
        CURRENT_TIMESTAMP() AS update_time
    FROM date_platform_list d
    LEFT JOIN dau_daily u USING (dt, platform)
    LEFT JOIN platform_daily p USING (dt, platform)
    LEFT JOIN exposure_daily e USING (dt, platform)
    LEFT JOIN like_daily l USING (dt, platform)
    LEFT JOIN follow_daily f USING (dt, platform)
    LEFT JOIN tryon_daily t USING (dt, platform)
    LEFT JOIN read_daily r USING (dt, platform);
    """
    logging.info("开始处理: dws_platform_content_daily")
    job = client.query(query)
    job.result()
    logging.info(f"dws_platform_content_daily 刷新完成, 处理日期: {dates}")


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info(f"日志文件: {log_path}")

    start_time = datetime.now(timezone.utc)
    dates = get_platform_content_dates_to_process()
    logging.info(f"待处理日期: {dates if dates else '[]'}")

    if not dates:
        logging.info("没有新的 dws_platform_content_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_platform_content_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"dws_platform_content_daily 完成, 耗时: {elapsed:.1f} 秒")
