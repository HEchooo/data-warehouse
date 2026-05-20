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
TABLE_NAME = "dws_post_interaction_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6

POST_EXPOSURE_EVENT_NAMES = (
    "v_star_post_detail",
    "v_magazine_post_detail",
    "v_brand_post_detail",
    "v_star_post_feeds",
    "v_magazine_post_feeds",
    "v_brand_post_feeds",
)
POST_DETAIL_EXPOSURE_EVENT_NAMES = (
    "v_star_post_detail",
    "v_magazine_post_detail",
    "v_brand_post_detail",
)
READ_EVENT_NAMES = (
    "r_star_post_detail",
    "r_magazine_post_detail",
    "r_brand_post_detail",
    "r_kol_post_detail",
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


def get_post_interaction_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    帖子主题同时依赖事件表、服务端事实表和维表映射，不复用共享水位。
    默认只看最近 6 小时变更，再映射出受影响业务日：
    - 事件表按 dwd_event_log.update_time 找受影响 dt
    - 服务端表按 gmt_create 找受影响 dt
    - 维表修正按最近 6 小时 update_time 找受影响业务键，再回推目标表已有历史 dt

    注意：
    - 这不是全历史强一致方案
    - 如果维表修正需要影响更早历史分区，仍要单独确认回填
    """
    event_ts = event_ts_expr()
    service_ts = service_ts_expr()
    target_exists = target_table_exists()

    target_dim_impact_sql = ""
    if target_exists:
        target_dim_impact_sql = f"""
        UNION DISTINCT
        SELECT DISTINCT t.dt
        FROM `{target_table_id()}` t
        JOIN changed_posts p
            ON t.post_code = p.post_code

        UNION DISTINCT
        SELECT DISTINCT t.dt
        FROM `{target_table_id()}` t
        JOIN changed_columns c
            ON t.column_id = c.column_id
        """

    query = f"""
    WITH
    -- 最近 6 小时变更的事件分区，对应帖子曝光 / 阅读 / 关注归因依赖的业务日。
    changed_event_dates AS (
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 最近 6 小时新增/更新的帖子维度键，用于回推受影响历史分区。
    changed_posts AS (
        SELECT DISTINCT post_code
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_post`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND post_code IS NOT NULL
    ),
    -- 最近 6 小时新增/更新的专栏维度键，用于回推受影响历史分区。
    changed_columns AS (
        SELECT DISTINCT column_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_column`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND column_id IS NOT NULL
    ),
    -- 最近 6 小时服务端点赞变更，对应帖子主题当天业务日。
    changed_like_dates AS (
        SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_post_like`
        WHERE {service_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 最近 6 小时服务端关注变更，对应帖子主题当天业务日。
    changed_follow_dates AS (
        SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_star_follow`
        WHERE {service_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 最近 6 小时帖子试穿服务端变更，对应帖子主题当天业务日。
    changed_tryon_dates AS (
        SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE {service_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND task_type IN ('TRY_ON', 'STYLE_ME')
            AND NULLIF(JSON_VALUE(request_param, '$.postId'), '') IS NOT NULL
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
        {target_dim_impact_sql}
    )
    WHERE dt IS NOT NULL
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


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


def clean_id_expr(field_name: str) -> str:
    """
    dwd_event_log 的 args_* 有时来自 JSON_EXTRACT，可能携带首尾双引号：
    例如：'"2602276189664"'。这里统一去掉首尾双引号并把空串归一为 NULL。
    """
    return f"NULLIF(NULLIF(TRIM(TRIM({field_name}), '\"'), ''), 'null')"


def module_expr(event_name_field: str = "event_name") -> str:
    star_id = clean_id_expr("args_star")
    magazine_id = clean_id_expr("args_magazine")
    brand_id = clean_id_expr("args_brand")
    return f"""
    CASE
        WHEN {event_name_field} IN ('v_star_post_detail', 'v_star_post_feeds') THEN 'star'
        WHEN {event_name_field} IN ('v_magazine_post_detail', 'v_magazine_post_feeds')
            THEN 'magazine'
        WHEN {event_name_field} IN ('v_brand_post_detail', 'v_brand_post_feeds') THEN 'brand'
        WHEN {star_id} IS NOT NULL THEN 'star'
        WHEN {magazine_id} IS NOT NULL THEN 'magazine'
        WHEN {brand_id} IS NOT NULL THEN 'brand'
        ELSE NULL
    END
    """


def column_id_expr(event_name_field: str = "event_name") -> str:
    star_id = clean_id_expr("args_star")
    magazine_id = clean_id_expr("args_magazine")
    brand_id = clean_id_expr("args_brand")
    return f"""
    CASE
        WHEN {event_name_field} IN ('v_star_post_detail', 'v_star_post_feeds') THEN {star_id}
        WHEN {event_name_field} IN ('v_magazine_post_detail', 'v_magazine_post_feeds')
            THEN {magazine_id}
        WHEN {event_name_field} IN ('v_brand_post_detail', 'v_brand_post_feeds') THEN {brand_id}
        ELSE COALESCE({star_id}, {magazine_id}, {brand_id})
    END
    """


def run_dws_post_interaction_daily(dates: list[date]) -> None:
    """
    产出帖子主题核心事实层。

    粒度：
    - dt × post_code × module × column_id × platform
    - 其中 platform 同时产出具体平台和 `all` 总计口径

    指标：
    - post_exposure_uv：帖子曝光 UV（详情 + feeds），仅统计已登录用户
    - read_uv：帖子阅读 UV，仅统计已登录用户
    - like_total_count / like_uv：服务端点赞次数 / 用户数
    - follow_total_count / follow_uv：服务端关注按帖子详情曝光 last-touch 归因后的次数 / 用户数
    - tryon_total_count：帖子相关试穿次数
    """
    if not dates:
        logging.info("没有新的 dws_post_interaction_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    event_ts = event_ts_expr()
    service_ts = service_ts_expr()
    platform_case = platform_expr()
    module_sql = module_expr()
    column_id_sql = column_id_expr()
    post_exposure_events_sql = string_values_to_sql_list(POST_EXPOSURE_EVENT_NAMES)
    post_detail_exposure_events_sql = string_values_to_sql_list(
        POST_DETAIL_EXPOSURE_EVENT_NAMES
    )
    read_events_sql = string_values_to_sql_list(READ_EVENT_NAMES)

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.dws_post_interaction_daily`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.dws_post_interaction_daily`
    (
        dt,
        post_code,
        module,
        column_id,
        platform,
        post_exposure_uv,
        read_uv,
        like_total_count,
        like_uv,
        follow_total_count,
        follow_uv,
        tryon_total_count,
        update_time
    )
    WITH
    -- 事件日志基础层：统一多伦多日期、平台和帖子主题相关原始字段。
    base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            {event_ts} AS event_ts,
            {platform_case} AS platform,
            event_name,
            NULLIF(prop_user_id, '') AS user_id,
            raw_event_id,
            hash_id,
            post_code,
            args_star,
            args_magazine,
            args_brand
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
    ),
    -- 已登录平台事件：给服务端点赞 / 试穿补平台，避免直接连 dws_user_daily 产生重复分摊。
    logged_in_platform_events AS (
        SELECT
            dt,
            event_ts,
            user_id,
            platform,
            hash_id
        FROM base
        WHERE user_id IS NOT NULL
    ),
    -- 帖子维表：统一 post_id -> post_code / column_id 映射，供服务端行为回帖子维度。
    post_dim AS (
        SELECT
            CAST(post_id AS STRING) AS post_id,
            post_code,
            column_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_post`
        WHERE post_id IS NOT NULL
            AND post_code IS NOT NULL
            AND column_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY CAST(post_id AS STRING)
            ORDER BY post_code, column_id
        ) = 1
    ),
    -- 专栏维表：统一 column_id -> module 映射，优先保留启用中的专栏。
    column_dim AS (
        SELECT
            column_id,
            module
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_column`
        WHERE column_id IS NOT NULL
            AND module IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY column_id
            ORDER BY IF(status = 0, 0, 1), module
        ) = 1
    ),
    -- 帖子曝光明细：保留帖子、专栏和平台维度，作为帖子级率指标分母。
    post_exposure_events AS (
        SELECT
            dt,
            event_ts,
            event_name,
            user_id,
            post_code,
            {module_sql} AS module,
            {column_id_sql} AS column_id,
            platform,
            hash_id
        FROM base
        WHERE event_name IN ({post_exposure_events_sql})
            AND user_id IS NOT NULL
            AND post_code IS NOT NULL
    ),
    -- 关注归因曝光明细：仍只保留帖子详情曝光，避免连带改动关注 last-touch 口径。
    detail_exposure_events AS (
        SELECT
            dt,
            event_ts,
            user_id,
            post_code,
            module,
            column_id,
            platform,
            hash_id
        FROM post_exposure_events
        WHERE event_name IN ({post_detail_exposure_events_sql})
    ),
    -- 帖子曝光平台聚合：按平台统计已登录曝光 UV。
    exposure_daily_platform AS (
        SELECT
            dt,
            post_code,
            module,
            column_id,
            platform,
            COUNT(DISTINCT user_id) AS post_exposure_uv
        FROM post_exposure_events
        WHERE module IS NOT NULL
            AND column_id IS NOT NULL
        GROUP BY dt, post_code, module, column_id, platform
    ),
    -- 帖子曝光总计聚合：忽略平台重算全平台 UV，避免跨平台重复相加。
    exposure_daily_all AS (
        SELECT
            dt,
            post_code,
            module,
            column_id,
            'all' AS platform,
            COUNT(DISTINCT user_id) AS post_exposure_uv
        FROM post_exposure_events
        WHERE module IS NOT NULL
            AND column_id IS NOT NULL
        GROUP BY dt, post_code, module, column_id
    ),
    -- 帖子曝光统一输出：同时保留平台口径和全平台总计口径。
    exposure_daily AS (
        SELECT * FROM exposure_daily_platform
        UNION ALL
        SELECT * FROM exposure_daily_all
    ),
    -- 阅读事件原始记录：保留事件内自带的帖子、专栏和平台维度。
    read_events_raw AS (
        SELECT
            dt,
            platform,
            raw_event_id,
            user_id,
            post_code,
            {module_sql} AS module,
            {column_id_sql} AS column_id
        FROM base
        WHERE event_name IN ({read_events_sql})
            AND raw_event_id IS NOT NULL
            AND post_code IS NOT NULL
            AND user_id IS NOT NULL
    ),
    -- 阅读事件去重：按 raw_event_id + post_code 合并拆行后的明细。
    read_events_dedup AS (
        SELECT
            dt,
            platform,
            raw_event_id,
            post_code,
            ANY_VALUE(user_id) AS user_id,
            ANY_VALUE(module) AS module,
            ANY_VALUE(column_id) AS column_id
        FROM read_events_raw
        GROUP BY dt, platform, raw_event_id, post_code
    ),
    -- 阅读平台聚合：按平台统计已登录阅读 UV。
    read_daily_platform AS (
        SELECT
            dt,
            post_code,
            module,
            column_id,
            platform,
            COUNT(DISTINCT user_id) AS read_uv
        FROM read_events_dedup
        WHERE module IS NOT NULL
            AND column_id IS NOT NULL
        GROUP BY dt, post_code, module, column_id, platform
    ),
    -- 阅读总计聚合：忽略平台重算全平台阅读 UV。
    read_daily_all AS (
        SELECT
            dt,
            post_code,
            module,
            column_id,
            'all' AS platform,
            COUNT(DISTINCT user_id) AS read_uv
        FROM read_events_dedup
        WHERE module IS NOT NULL
            AND column_id IS NOT NULL
        GROUP BY dt, post_code, module, column_id
    ),
    -- 阅读统一输出：同时保留平台口径和全平台总计口径。
    read_daily AS (
        SELECT * FROM read_daily_platform
        UNION ALL
        SELECT * FROM read_daily_all
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
    -- 服务端点赞补齐帖子、专栏与平台维度；平台按同日最近登录事件归因，未命中回落 unknown。
    like_service_enriched AS (
        SELECT
            l.dt,
            p.post_code,
            c.module,
            p.column_id,
            COALESCE(e.platform, 'unknown') AS platform,
            l.user_id
        FROM like_service_base l
        JOIN post_dim p
            ON l.post_id = p.post_id
        JOIN column_dim c
            ON p.column_id = c.column_id
        LEFT JOIN logged_in_platform_events e
            ON e.dt = l.dt
            AND e.user_id = l.user_id
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY l.action_id
            ORDER BY
                IF(e.event_ts IS NULL, 1, 0),
                ABS(TIMESTAMP_DIFF(e.event_ts, l.action_ts, SECOND)),
                IF(e.event_ts <= l.action_ts, 0, 1),
                e.event_ts DESC,
                e.hash_id DESC
        ) = 1
    ),
    -- 服务端点赞平台聚合：按平台产出点赞次数与点赞 UV。
    like_daily_platform AS (
        SELECT
            dt,
            post_code,
            module,
            column_id,
            platform,
            COUNT(*) AS like_total_count,
            COUNT(DISTINCT user_id) AS like_uv
        FROM like_service_enriched
        GROUP BY dt, post_code, module, column_id, platform
    ),
    -- 服务端点赞总计聚合：忽略平台重算全平台点赞次数与点赞 UV。
    like_daily_all AS (
        SELECT
            dt,
            post_code,
            module,
            column_id,
            'all' AS platform,
            COUNT(*) AS like_total_count,
            COUNT(DISTINCT user_id) AS like_uv
        FROM like_service_enriched
        GROUP BY dt, post_code, module, column_id
    ),
    -- 服务端点赞统一输出：同时保留平台口径和全平台总计口径。
    like_daily AS (
        SELECT * FROM like_daily_platform
        UNION ALL
        SELECT * FROM like_daily_all
    ),
    -- 服务端试穿原始记录：只保留带 postId 的内容场景试穿任务。
    tryon_task_base AS (
        SELECT
            GENERATE_UUID() AS action_id,
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            {service_ts} AS action_ts,
            NULLIF(JSON_VALUE(request_param, '$.postId'), '') AS post_id,
            COALESCE(
                NULLIF(JSON_VALUE(request_param, '$.user_id'), ''),
                NULLIF(JSON_VALUE(request_param, '$.userId'), ''),
                NULLIF(CAST(user_id AS STRING), '')
            ) AS user_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE DATE({service_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND task_type IN ('TRY_ON', 'STYLE_ME')
            AND NULLIF(JSON_VALUE(request_param, '$.postId'), '') IS NOT NULL
    ),
    -- 服务端试穿补齐帖子、专栏与平台维度；平台按同日最近登录事件归因，未命中回落 unknown。
    tryon_task_enriched AS (
        SELECT
            t.dt,
            p.post_code,
            c.module,
            p.column_id,
            COALESCE(e.platform, 'unknown') AS platform
        FROM tryon_task_base t
        JOIN post_dim p
            ON t.post_id = p.post_id
        JOIN column_dim c
            ON p.column_id = c.column_id
        LEFT JOIN logged_in_platform_events e
            ON e.dt = t.dt
            AND e.user_id = t.user_id
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY t.action_id
            ORDER BY
                IF(e.event_ts IS NULL, 1, 0),
                ABS(TIMESTAMP_DIFF(e.event_ts, t.action_ts, SECOND)),
                IF(e.event_ts <= t.action_ts, 0, 1),
                e.event_ts DESC,
                e.hash_id DESC
        ) = 1
    ),
    -- 服务端试穿平台聚合：按平台产出帖子相关试穿次数。
    tryon_daily_platform AS (
        SELECT
            dt,
            post_code,
            module,
            column_id,
            platform,
            COUNT(*) AS tryon_total_count
        FROM tryon_task_enriched
        GROUP BY dt, post_code, module, column_id, platform
    ),
    -- 服务端试穿总计聚合：忽略平台重算全平台试穿次数。
    tryon_daily_all AS (
        SELECT
            dt,
            post_code,
            module,
            column_id,
            'all' AS platform,
            COUNT(*) AS tryon_total_count
        FROM tryon_task_enriched
        GROUP BY dt, post_code, module, column_id
    ),
    -- 服务端试穿统一输出：同时保留平台口径和全平台总计口径。
    tryon_daily AS (
        SELECT * FROM tryon_daily_platform
        UNION ALL
        SELECT * FROM tryon_daily_all
    ),
    -- 服务端关注原始记录：按创建时间统计新增关注事实，并带上专栏维度。
    follow_service_base AS (
        SELECT
            GENERATE_UUID() AS action_id,
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            {service_ts} AS action_ts,
            CAST(f.user_id AS STRING) AS user_id,
            c.module,
            c.column_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_star_follow` f
        JOIN column_dim c
            ON CAST(f.star_user_id AS STRING) = c.column_id
        WHERE DATE({service_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND f.star_user_id IS NOT NULL
            AND CAST(f.star_user_id AS STRING) != ''
            AND f.user_id IS NOT NULL
            AND CAST(f.user_id AS STRING) != ''
    ),
    -- 服务端关注归因到帖子：只在同日同专栏下，命中关注前最后一次帖子详情曝光，并沿用曝光平台。
    follow_attributed AS (
        SELECT
            f.dt,
            f.user_id,
            e.post_code,
            e.module,
            e.column_id,
            e.platform
        FROM follow_service_base f
        JOIN detail_exposure_events e
            ON e.dt = f.dt
            AND e.user_id = f.user_id
            AND e.module = f.module
            AND e.column_id = f.column_id
            AND e.event_ts <= f.action_ts
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY f.action_id
            ORDER BY e.event_ts DESC, e.hash_id DESC
        ) = 1
    ),
    -- 服务端关注平台聚合：按平台产出帖子级关注次数与关注 UV。
    follow_daily_platform AS (
        SELECT
            dt,
            post_code,
            module,
            column_id,
            platform,
            COUNT(*) AS follow_total_count,
            COUNT(DISTINCT user_id) AS follow_uv
        FROM follow_attributed
        GROUP BY dt, post_code, module, column_id, platform
    ),
    -- 服务端关注总计聚合：忽略平台重算全平台关注次数与关注 UV。
    follow_daily_all AS (
        SELECT
            dt,
            post_code,
            module,
            column_id,
            'all' AS platform,
            COUNT(*) AS follow_total_count,
            COUNT(DISTINCT user_id) AS follow_uv
        FROM follow_attributed
        GROUP BY dt, post_code, module, column_id
    ),
    -- 服务端关注统一输出：同时保留平台口径和全平台总计口径。
    follow_daily AS (
        SELECT * FROM follow_daily_platform
        UNION ALL
        SELECT * FROM follow_daily_all
    ),
    -- 指标主键骨架：合并所有可能产出指标的帖子维度键。
    keys AS (
        SELECT DISTINCT dt, post_code, module, column_id, platform
        FROM exposure_daily
        UNION DISTINCT
        SELECT DISTINCT dt, post_code, module, column_id, platform
        FROM read_daily
        UNION DISTINCT
        SELECT DISTINCT dt, post_code, module, column_id, platform
        FROM like_daily
        UNION DISTINCT
        SELECT DISTINCT dt, post_code, module, column_id, platform
        FROM follow_daily
        UNION DISTINCT
        SELECT DISTINCT dt, post_code, module, column_id, platform
        FROM tryon_daily
    )
    SELECT
        k.dt,
        k.post_code,
        k.module,
        k.column_id,
        k.platform,
        COALESCE(e.post_exposure_uv, 0) AS post_exposure_uv,
        COALESCE(r.read_uv, 0) AS read_uv,
        COALESCE(l.like_total_count, 0) AS like_total_count,
        COALESCE(l.like_uv, 0) AS like_uv,
        COALESCE(f.follow_total_count, 0) AS follow_total_count,
        COALESCE(f.follow_uv, 0) AS follow_uv,
        COALESCE(t.tryon_total_count, 0) AS tryon_total_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM keys k
    LEFT JOIN exposure_daily e USING (dt, post_code, module, column_id, platform)
    LEFT JOIN read_daily r USING (dt, post_code, module, column_id, platform)
    LEFT JOIN like_daily l USING (dt, post_code, module, column_id, platform)
    LEFT JOIN follow_daily f USING (dt, post_code, module, column_id, platform)
    LEFT JOIN tryon_daily t USING (dt, post_code, module, column_id, platform);
    """

    logging.info("开始处理: dws_post_interaction_daily")
    job = client.query(query)
    job.result()
    logging.info(f"dws_post_interaction_daily 刷新完成, 处理日期: {dates}")


if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    dates = get_post_interaction_dates_to_process()
    if not dates:
        logging.info("没有新的 dws_post_interaction_daily 日期需要处理")
        raise SystemExit(0)

    logging.info("帖子主题待处理日期: %s", dates)
    run_dws_post_interaction_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dws_post_interaction_daily 执行完成, 耗时: %.1f 秒", elapsed)
