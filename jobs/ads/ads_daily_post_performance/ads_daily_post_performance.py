import logging

from google.cloud import bigquery

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
V3_DATASET_ID = "v3_decom"
TORONTO_TZ = "America/Toronto"

POST_DETAIL_EXPOSURE_EVENTS = (
    "'v_star_post_detail',"
    "'v_magazine_post_detail',"
    "'v_brand_post_detail'"
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
        WHEN {event_name_field} = 'v_star_post_detail' THEN 'star'
        WHEN {event_name_field} = 'v_magazine_post_detail' THEN 'magazine'
        WHEN {event_name_field} = 'v_brand_post_detail' THEN 'brand'
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
        WHEN {event_name_field} = 'v_star_post_detail' THEN {star_id}
        WHEN {event_name_field} = 'v_magazine_post_detail' THEN {magazine_id}
        WHEN {event_name_field} = 'v_brand_post_detail' THEN {brand_id}
        ELSE COALESCE({star_id}, {magazine_id}, {brand_id})
    END
    """


def run_ads_daily_post_performance(dates):
    """
    内容维度日报（日×帖子）:
    粒度：dt × post × module × column

    指标：
    - post_exposure_uv: 帖子曝光UV（进入帖子详情页的去重用户数）
    - like_total_count: 帖子点赞数（点赞成功次数）
    - like_rate: 帖子点赞率（点赞UV / 帖子曝光UV；同一用户同一天同帖只要点赞过即计为1）
    - follow_total_count: 帖子相关关注数（关注成功次数，按 last-touch 归因到帖子详情曝光）
    - follow_rate: 帖子关注转化率（归因关注UV / 帖子曝光UV）
    - tryon_total_count: 从该帖子发起的试穿次数（try on 成功次数）

    说明：
    - post_code -> post_id/post_name：v3_decom.community_post (id, post_code, title)
    - post_code -> creator：v3_decom.community_post (post_code, creator)
    - column_name：v3_decom.kol_rel (status=0, user_id, nickname, kol_type=2/3/4)
    - 关注事件 c_follow 无 post_code，需用同一天内“此事件之前最后一次帖子详情曝光”归因
    - dwd_event_log 会展开 args_post，PV 类指标统一按 raw_event_id 去重
    使用多伦多时间（America/Toronto）。
    """
    dates_str = dates_to_sql_list(dates)
    event_ts = event_ts_expr()
    module_sql = module_expr()
    column_id_sql = column_id_expr()

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_post_performance`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ads_daily_post_performance`
    (dt, post_id, post_code, post_name, creator, module, column_id, column_name,
     post_exposure_uv, like_total_count, like_rate,
     follow_total_count, follow_rate, tryon_total_count, update_time)
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
            ANY_VALUE(SAFE_CAST(id AS INT64)) AS post_id,
            ANY_VALUE(title) AS post_name,
            ANY_VALUE(CAST(creator AS STRING)) AS creator
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_post`
        WHERE post_code IS NOT NULL
        GROUP BY post_code
    ),
    column_map AS (
        SELECT
            CAST(user_id AS STRING) AS column_id,
            kol_type,
            ANY_VALUE(nickname) AS column_name
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.kol_rel`
        WHERE status = 0
        GROUP BY column_id, kol_type
    ),
    exposure_events AS (
        SELECT
            dt,
            event_ts,
            visitor_id,
            post_code,
            {module_sql} AS module,
            {column_id_sql} AS column_id,
            hash_id
        FROM base
        WHERE event_name IN ({POST_DETAIL_EXPOSURE_EVENTS})
            AND visitor_id IS NOT NULL
            AND post_code IS NOT NULL
    ),
    exposure_daily AS (
        SELECT
            dt,
            post_code,
            module,
            column_id,
            COUNT(DISTINCT visitor_id) AS post_exposure_uv
        FROM exposure_events
        WHERE module IS NOT NULL AND column_id IS NOT NULL
        GROUP BY dt, post_code, module, column_id
    ),
    like_events_raw AS (
        SELECT
            dt,
            event_ts,
            raw_event_id,
            visitor_id,
            post_code,
            {module_sql} AS module,
            {column_id_sql} AS column_id
        FROM base
        WHERE event_name = 'c_like'
            AND raw_event_id IS NOT NULL
            AND post_code IS NOT NULL
            AND visitor_id IS NOT NULL
    ),
    like_events_dedup AS (
        SELECT
            dt,
            raw_event_id,
            post_code,
            ANY_VALUE(visitor_id) AS visitor_id,
            ANY_VALUE(event_ts) AS event_ts,
            ANY_VALUE(module) AS module,
            ANY_VALUE(column_id) AS column_id
        FROM like_events_raw
        GROUP BY dt, raw_event_id, post_code
    ),
    like_events_enriched AS (
        SELECT
            l.dt,
            l.raw_event_id,
            l.post_code,
            l.visitor_id,
            COALESCE(l.module, e.module) AS module,
            COALESCE(l.column_id, e.column_id) AS column_id
        FROM like_events_dedup l
        LEFT JOIN exposure_events e
            ON e.dt = l.dt
            AND e.visitor_id = l.visitor_id
            AND e.post_code = l.post_code
            AND e.event_ts <= l.event_ts
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY l.dt, l.raw_event_id, l.post_code
            ORDER BY e.event_ts DESC, e.hash_id DESC
        ) = 1
    ),
    like_daily AS (
        SELECT
            dt,
            post_code,
            module,
            column_id,
            COUNT(*) AS like_total_count,
            COUNT(DISTINCT visitor_id) AS like_uv
        FROM like_events_enriched
        WHERE module IS NOT NULL AND column_id IS NOT NULL
        GROUP BY dt, post_code, module, column_id
    ),
    tryon_events_raw AS (
        SELECT
            dt,
            event_ts,
            raw_event_id,
            visitor_id,
            post_code,
            {module_sql} AS module,
            {column_id_sql} AS column_id
        FROM base
        WHERE event_name = 'c_tryon'
            AND raw_event_id IS NOT NULL
            AND post_code IS NOT NULL
            AND visitor_id IS NOT NULL
    ),
    tryon_events_dedup AS (
        SELECT
            dt,
            raw_event_id,
            post_code,
            ANY_VALUE(visitor_id) AS visitor_id,
            ANY_VALUE(event_ts) AS event_ts,
            ANY_VALUE(module) AS module,
            ANY_VALUE(column_id) AS column_id
        FROM tryon_events_raw
        GROUP BY dt, raw_event_id, post_code
    ),
    tryon_events_enriched AS (
        SELECT
            t.dt,
            t.raw_event_id,
            t.post_code,
            t.visitor_id,
            COALESCE(t.module, e.module) AS module,
            COALESCE(t.column_id, e.column_id) AS column_id
        FROM tryon_events_dedup t
        LEFT JOIN exposure_events e
            ON e.dt = t.dt
            AND e.visitor_id = t.visitor_id
            AND e.post_code = t.post_code
            AND e.event_ts <= t.event_ts
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY t.dt, t.raw_event_id, t.post_code
            ORDER BY e.event_ts DESC, e.hash_id DESC
        ) = 1
    ),
    tryon_daily AS (
        SELECT
            dt,
            post_code,
            module,
            column_id,
            COUNT(*) AS tryon_total_count
        FROM tryon_events_enriched
        WHERE module IS NOT NULL AND column_id IS NOT NULL
        GROUP BY dt, post_code, module, column_id
    ),
    follow_events_dedup AS (
        SELECT
            dt,
            raw_event_id,
            ANY_VALUE(event_ts) AS event_ts,
            ANY_VALUE(visitor_id) AS visitor_id
        FROM base
        WHERE event_name = 'c_follow'
            AND raw_event_id IS NOT NULL
            AND visitor_id IS NOT NULL
        GROUP BY dt, raw_event_id
    ),
    follow_attributed AS (
        SELECT
            f.dt,
            f.raw_event_id,
            f.visitor_id,
            e.post_code,
            e.module,
            e.column_id
        FROM follow_events_dedup f
        JOIN exposure_events e
            ON e.dt = f.dt
            AND e.visitor_id = f.visitor_id
            AND e.event_ts <= f.event_ts
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY f.dt, f.raw_event_id
            ORDER BY e.event_ts DESC, e.hash_id DESC
        ) = 1
    ),
    follow_daily AS (
        SELECT
            dt,
            post_code,
            module,
            column_id,
            COUNT(*) AS follow_total_count,
            COUNT(DISTINCT visitor_id) AS follow_uv
        FROM follow_attributed
        WHERE module IS NOT NULL AND column_id IS NOT NULL
        GROUP BY dt, post_code, module, column_id
    ),
    keys AS (
        SELECT DISTINCT dt, post_code, module, column_id
        FROM exposure_daily
        UNION DISTINCT
        SELECT DISTINCT dt, post_code, module, column_id
        FROM like_daily
        UNION DISTINCT
        SELECT DISTINCT dt, post_code, module, column_id
        FROM tryon_daily
        UNION DISTINCT
        SELECT DISTINCT dt, post_code, module, column_id
        FROM follow_daily
        WHERE module IS NOT NULL AND column_id IS NOT NULL
    ),
    final AS (
        SELECT
            k.dt,
            pm.post_id,
            k.post_code,
            pm.post_name,
            pm.creator,
            k.module,
            k.column_id,
            cm.column_name,
            COALESCE(e.post_exposure_uv, 0) AS post_exposure_uv,
            COALESCE(l.like_total_count, 0) AS like_total_count,
            COALESCE(l.like_uv, 0) AS like_uv,
            COALESCE(f.follow_total_count, 0) AS follow_total_count,
            COALESCE(f.follow_uv, 0) AS follow_uv,
            COALESCE(t.tryon_total_count, 0) AS tryon_total_count
        FROM keys k
        LEFT JOIN exposure_daily e USING (dt, post_code, module, column_id)
        LEFT JOIN like_daily l USING (dt, post_code, module, column_id)
        LEFT JOIN follow_daily f USING (dt, post_code, module, column_id)
        LEFT JOIN tryon_daily t USING (dt, post_code, module, column_id)
        LEFT JOIN post_map pm USING (post_code)
        LEFT JOIN column_map cm
            ON cm.column_id = k.column_id
            AND cm.kol_type = CASE
                WHEN k.module = 'star' THEN 2
                WHEN k.module = 'magazine' THEN 3
                WHEN k.module = 'brand' THEN 4
                ELSE NULL
            END
    )
    SELECT
        dt,
        post_id,
        post_code,
        post_name,
        creator,
        module,
        column_id,
        column_name,
        post_exposure_uv,
        like_total_count,
        CAST(CASE
            WHEN post_exposure_uv = 0 THEN 0
            ELSE ROUND(CAST(like_uv AS NUMERIC) / CAST(post_exposure_uv AS NUMERIC), 4)
        END AS NUMERIC) AS like_rate,
        follow_total_count,
        CAST(CASE
            WHEN post_exposure_uv = 0 THEN 0
            ELSE ROUND(CAST(follow_uv AS NUMERIC) / CAST(post_exposure_uv AS NUMERIC), 4)
        END AS NUMERIC) AS follow_rate,
        tryon_total_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM final;
    """

    logging.info("开始处理: ads_daily_post_performance")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_post_performance 刷新完成, 处理日期: {dates}")
