import logging

from google.cloud import bigquery

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
V3_DATASET_ID = "v3_decom"
TORONTO_TZ = "America/Toronto"

COLUMN_EXPOSURE_EVENTS = (
    "'v_star_post_detail',"
    "'v_magazine_post_detail',"
    "'v_brand_post_detail',"
    "'v_star_post_feeds',"
    "'v_brand_post_feeds'"
)

client = bigquery.Client(project=PROJECT_ID)


def dates_to_sql_list(dates):
    return ", ".join([f"'{d}'" for d in dates])


def event_ts_expr():
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


def module_expr(event_name_field="event_name"):
    star_id = clean_id_expr("args_star")
    magazine_id = clean_id_expr("args_magazine")
    brand_id = clean_id_expr("args_brand")
    return f"""
    CASE
        WHEN {event_name_field} IN ('v_star_post_detail', 'v_star_post_feeds') THEN 'star'
        WHEN {event_name_field} = 'v_magazine_post_detail' THEN 'magazine'
        WHEN {event_name_field} IN ('v_brand_post_detail', 'v_brand_post_feeds') THEN 'brand'
        WHEN {star_id} IS NOT NULL THEN 'star'
        WHEN {magazine_id} IS NOT NULL THEN 'magazine'
        WHEN {brand_id} IS NOT NULL THEN 'brand'
        ELSE NULL
    END
    """


def clean_id_expr(field_name: str) -> str:
    """
    dwd_event_log 的 args_* 有时来自 JSON_EXTRACT，可能携带首尾双引号：
    例如：'"2602276189664"'。这里统一去掉首尾双引号并把空串归一为 NULL。
    """
    # 线上数据里也可能出现 'null' / '""' 这样的字符串，占位但不代表有效 ID
    # 这里统一把它们当作 NULL，避免后续分组/映射异常。
    return f"NULLIF(NULLIF(TRIM(TRIM({field_name}), '\"'), ''), 'null')"


def column_id_expr(event_name_field="event_name"):
    star_id = clean_id_expr("args_star")
    magazine_id = clean_id_expr("args_magazine")
    brand_id = clean_id_expr("args_brand")
    return f"""
    CASE
        WHEN {event_name_field} IN ('v_star_post_detail', 'v_star_post_feeds')
            THEN {star_id}
        WHEN {event_name_field} = 'v_magazine_post_detail' THEN {magazine_id}
        WHEN {event_name_field} IN ('v_brand_post_detail', 'v_brand_post_feeds')
            THEN {brand_id}
        ELSE COALESCE({star_id}, {magazine_id}, {brand_id})
    END
    """


def run_ads_daily_column_performance(dates):
    """
    专栏追踪明细（日×创建者×专栏）:
    粒度：dt × creator × module × column_id

    Filters（用于 BI）：
    - dt（日期范围）
    - creator：帖子创建者（映射 v3_decom.community_post.creator 原值）
    - module：star / brand / magazine
    - column_id：专栏ID（对应 star_id/brand_id/magazine_id）
    - column_name：专栏名称（映射 v3_decom.kol_rel.nickname）

    指标口径：
    - column_exposure_uv：专栏曝光UV（feed+detail 曝光去重用户数）
      - star：v_star_post_feeds + v_star_post_detail
      - brand：v_brand_post_feeds + v_brand_post_detail
      - magazine：仅 v_magazine_post_detail（无 feeds 埋点）
    - follow_total_count：关注点击次数（c_follow，按 raw_event_id 去重计数）
    - follow_rate：关注转化率（follow_uv / column_exposure_uv；同一用户同天同创建者同专栏只要关注过一次即 100%）
    - read_post_count：去重后的帖子数（同一用户同天同创建者同专栏同一 post 多次曝光只算 1）
    - avg_read_post_count_per_user：人均阅读帖子数（read_post_count / column_exposure_uv）
    - c_follow 无 creator，需要归因到同一天内该访客最近一次专栏曝光事件，再取对应 post_code 的 creator

    使用多伦多时间（America/Toronto）。
    """
    dates_str = dates_to_sql_list(dates)
    event_ts = event_ts_expr()
    module_sql = module_expr()
    column_id_sql = column_id_expr()

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_column_performance`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ads_daily_column_performance`
    (dt, creator, module, column_id, column_name,
     column_exposure_uv, follow_total_count, follow_rate,
     read_post_count, avg_read_post_count_per_user, update_time)
    WITH
    base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            {event_ts} AS event_ts,
            event_name,
            COALESCE(NULLIF(prop_user_id, ''), NULLIF(prop_device_id, '')) AS visitor_id,
            raw_event_id,
            hash_id,
            post_code,
            args_star,
            args_magazine,
            args_brand
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
    ),
    post_map AS (
        SELECT
            CAST(post_code AS STRING) AS post_code,
            ANY_VALUE(CAST(creator AS STRING)) AS creator
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_post`
        WHERE post_code IS NOT NULL
        GROUP BY post_code
    ),
    exposure_events AS (
        SELECT
            b.dt,
            b.event_ts,
            {module_sql} AS module,
            {column_id_sql} AS column_id,
            pm.creator,
            b.visitor_id,
            b.hash_id,
            b.post_code
        FROM base b
        LEFT JOIN post_map pm
            ON CAST(b.post_code AS STRING) = pm.post_code
        WHERE event_name IN ({COLUMN_EXPOSURE_EVENTS})
    ),
    exposure_daily AS (
        SELECT
            dt,
            creator,
            module,
            column_id,
            COUNT(DISTINCT IF(visitor_id IS NOT NULL, visitor_id, NULL)) AS column_exposure_uv,
            COUNT(
                DISTINCT IF(
                    visitor_id IS NOT NULL AND post_code IS NOT NULL,
                    CONCAT(visitor_id, '-', CAST(post_code AS STRING)),
                    NULL
                )
            ) AS read_post_count
        FROM exposure_events
        WHERE module IS NOT NULL AND column_id IS NOT NULL
        GROUP BY dt, creator, module, column_id
    ),
    follow_events_dedup AS (
        SELECT
            dt,
            raw_event_id,
            ANY_VALUE(event_ts) AS event_ts,
            ANY_VALUE(visitor_id) AS visitor_id,
            ANY_VALUE({module_sql}) AS module,
            ANY_VALUE({column_id_sql}) AS column_id
        FROM base
        WHERE event_name = 'c_follow'
            AND raw_event_id IS NOT NULL
        GROUP BY dt, raw_event_id
    ),
    follow_attributed AS (
        SELECT
            f.dt,
            f.raw_event_id,
            f.visitor_id,
            e.creator,
            f.module AS module,
            f.column_id AS column_id
        FROM follow_events_dedup f
        LEFT JOIN exposure_events e
            ON e.dt = f.dt
            AND e.visitor_id = f.visitor_id
            AND e.event_ts <= f.event_ts
            AND e.module = f.module
            AND e.column_id = f.column_id
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY f.dt, f.raw_event_id
            ORDER BY e.event_ts DESC, e.hash_id DESC
        ) = 1
    ),
    follow_daily AS (
        SELECT
            dt,
            creator,
            module,
            column_id,
            COUNT(*) AS follow_total_count,
            COUNT(DISTINCT IF(visitor_id IS NOT NULL, visitor_id, NULL)) AS follow_uv
        FROM follow_attributed
        WHERE module IS NOT NULL AND column_id IS NOT NULL
        GROUP BY dt, creator, module, column_id
    ),
    keys AS (
        SELECT DISTINCT dt, creator, module, column_id FROM exposure_daily
        UNION DISTINCT
        SELECT DISTINCT dt, creator, module, column_id FROM follow_daily
    ),
    column_map AS (
        SELECT
            CASE
                WHEN kol_type = 2 THEN 'star'
                WHEN kol_type = 3 THEN 'magazine'
                WHEN kol_type = 4 THEN 'brand'
                ELSE NULL
            END AS module,
            CAST(user_id AS STRING) AS column_id,
            ANY_VALUE(nickname) AS column_name
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.kol_rel`
        WHERE status = 0 AND kol_type IN (2, 3, 4)
        GROUP BY module, column_id
    ),
    daily AS (
        SELECT
            k.dt,
            k.creator,
            k.module,
            k.column_id,
            COALESCE(cm.column_name, '') AS column_name,
            COALESCE(e.column_exposure_uv, 0) AS column_exposure_uv,
            COALESCE(f.follow_total_count, 0) AS follow_total_count,
            COALESCE(f.follow_uv, 0) AS follow_uv,
            COALESCE(e.read_post_count, 0) AS read_post_count
        FROM keys k
        LEFT JOIN exposure_daily e
            ON k.dt = e.dt
            AND k.creator IS NOT DISTINCT FROM e.creator
            AND k.module = e.module
            AND k.column_id = e.column_id
        LEFT JOIN follow_daily f
            ON k.dt = f.dt
            AND k.creator IS NOT DISTINCT FROM f.creator
            AND k.module = f.module
            AND k.column_id = f.column_id
        LEFT JOIN column_map cm
            ON cm.module = k.module
            AND cm.column_id = k.column_id
    )
    SELECT
        dt,
        creator,
        module,
        column_id,
        column_name,
        column_exposure_uv,
        follow_total_count,
        CAST(CASE
            WHEN column_exposure_uv = 0 THEN 0
            ELSE ROUND(
                CAST(follow_uv AS NUMERIC) / CAST(column_exposure_uv AS NUMERIC),
                4
            )
        END AS NUMERIC) AS follow_rate,
        read_post_count,
        CAST(CASE
            WHEN column_exposure_uv = 0 THEN 0
            ELSE ROUND(
                CAST(read_post_count AS NUMERIC) / CAST(column_exposure_uv AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_read_post_count_per_user,
        CURRENT_TIMESTAMP() AS update_time
    FROM daily;
    """

    logging.info("开始处理: ads_daily_column_performance")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_column_performance 刷新完成, 处理日期: {dates}")
