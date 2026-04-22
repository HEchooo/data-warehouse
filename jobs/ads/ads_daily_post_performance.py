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
READ_EVENTS = (
    "'r_star_post_detail',"
    "'r_magazine_post_detail',"
    "'r_brand_post_detail',"
    "'r_kol_post_detail'"
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


def service_ts_expr(field_name: str = "gmt_create") -> str:
    """v3 服务表的 gmt_create 是 UTC DATETIME，这里统一转成 TIMESTAMP。"""
    return f"TIMESTAMP({field_name}, 'UTC')"


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
    - post_exposure_uv: 帖子曝光已登录用户数（进入帖子详情页的去重已登录用户数）
    - like_total_count: 帖子点赞数（服务端点赞成功次数）
    - like_rate: 帖子点赞率（点赞已登录用户数 / 帖子曝光已登录用户数；同一用户同一天同帖只要点赞过即计为1）
    - read_rate: 帖子完读率（阅读已登录用户数 / 帖子曝光已登录用户数；同一用户同一天同帖只要阅读过即计为1）
    - follow_total_count: 帖子相关关注数（服务端关注成功次数，按 last-touch 归因到帖子详情曝光）
    - follow_rate: 帖子关注转化率（归因关注已登录用户数 / 帖子曝光已登录用户数）
    - tryon_total_count: 从该帖子发起的试穿次数（服务端 try on 成功次数）

    说明：
    - post_code -> post_id/post_name：v3_decom.community_post (id, post_code, title)
    - post_code -> creator：v3_decom.community_post (post_code, creator)
    - 点赞取服务端表：v3_decom.community_post_like
    - 关注取服务端表：v3_decom.community_star_follow，仍需按同一天内“此事件之前最后一次帖子详情曝光”归因到帖子
    - 试穿取服务端表：v3_decom.common_ext_ai_task，限定 task_type IN ('TRY_ON', 'STYLE_ME') 且 request_param.postId 非空
    - column_name：v3_decom.kol_rel (user_id, nickname, kol_type=2/3/4；优先取 status=0)
    使用多伦多时间（America/Toronto）。
    """
    dates_str = dates_to_sql_list(dates)
    event_ts = event_ts_expr()
    service_ts = service_ts_expr()
    module_sql = module_expr()
    column_id_sql = column_id_expr()

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_post_performance`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ads_daily_post_performance`
    (dt, post_id, post_code, post_name, creator, module, column_id, column_name,
     post_exposure_uv, like_total_count, like_rate, read_rate,
     follow_total_count, follow_rate, tryon_total_count, update_time)
    WITH
    -- 事件日志基础层：统一多伦多日期，并保留已登录用户口径所需字段。
    base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            {event_ts} AS event_ts,
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
    -- 帖子维表明细：统一帖子主键、编码、作者和所属专栏。
    post_dim AS (
        SELECT
            CAST(id AS STRING) AS post_id,
            SAFE_CAST(id AS INT64) AS post_id_int64,
            CAST(post_code AS STRING) AS post_code,
            title AS post_name,
            CAST(creator AS STRING) AS creator,
            CAST(poster_user_id AS STRING) AS poster_column_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_post`
        WHERE id IS NOT NULL
            AND post_code IS NOT NULL
    ),
    -- 帖子编码映射：供最终结果补齐 post_id / post_name / creator。
    post_map AS (
        SELECT
            post_code,
            ANY_VALUE(post_id_int64) AS post_id,
            ANY_VALUE(post_name) AS post_name,
            ANY_VALUE(creator) AS creator
        FROM post_dim
        GROUP BY post_code
    ),
    -- 专栏映射：统一 column_id -> module / column_name，优先保留启用中的映射。
    column_map AS (
        SELECT
            CASE
                WHEN kol_type = 2 THEN 'star'
                WHEN kol_type = 3 THEN 'magazine'
                WHEN kol_type = 4 THEN 'brand'
                ELSE NULL
            END AS module,
            CAST(user_id AS STRING) AS column_id,
            nickname AS column_name
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.kol_rel`
        WHERE user_id IS NOT NULL
            AND kol_type IN (2, 3, 4)
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY CAST(user_id AS STRING)
            ORDER BY IF(status = 0, 0, 1), kol_type
        ) = 1
    ),
    -- 帖子详情曝光明细：保留帖子、专栏和已登录用户归因所需信息。
    exposure_events AS (
        SELECT
            dt,
            event_ts,
            user_id,
            post_code,
            {module_sql} AS module,
            {column_id_sql} AS column_id,
            hash_id
        FROM base
        WHERE event_name IN ({POST_DETAIL_EXPOSURE_EVENTS})
            AND user_id IS NOT NULL
            AND post_code IS NOT NULL
    ),
    -- 帖子详情曝光日聚合：按已登录用户去重，作为帖子级率指标分母。
    exposure_daily AS (
        SELECT
            dt,
            post_code,
            module,
            column_id,
            COUNT(DISTINCT user_id) AS post_exposure_uv
        FROM exposure_events
        WHERE module IS NOT NULL AND column_id IS NOT NULL
        GROUP BY dt, post_code, module, column_id
    ),
    -- 服务端点赞原始记录：按创建时间统计成功点赞事实。
    like_service_base AS (
        SELECT
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            CAST(post_id AS STRING) AS post_id,
            CAST(user_id AS STRING) AS user_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_post_like`
        WHERE DATE({service_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND post_id IS NOT NULL
            AND user_id IS NOT NULL
            AND CAST(user_id AS STRING) != ''
    ),
    -- 服务端点赞补齐帖子与专栏维度。
    like_service_enriched AS (
        SELECT
            l.dt,
            p.post_code,
            c.module,
            c.column_id,
            l.user_id
        FROM like_service_base l
        JOIN post_dim p
            ON l.post_id = p.post_id
        JOIN column_map c
            ON p.poster_column_id = c.column_id
    ),
    -- 服务端点赞日聚合：产出点赞次数与点赞 UV。
    like_daily AS (
        SELECT
            dt,
            post_code,
            module,
            column_id,
            COUNT(*) AS like_total_count,
            COUNT(DISTINCT user_id) AS like_uv
        FROM like_service_enriched
        GROUP BY dt, post_code, module, column_id
    ),
    -- 阅读事件原始记录：保留事件内自带的帖子、专栏和已登录用户维度。
    read_events_raw AS (
        SELECT
            dt,
            raw_event_id,
            user_id,
            post_code,
            {module_sql} AS module,
            {column_id_sql} AS column_id
        FROM base
        WHERE event_name IN ({READ_EVENTS})
            AND raw_event_id IS NOT NULL
            AND post_code IS NOT NULL
            AND user_id IS NOT NULL
    ),
    -- 阅读事件去重：按 raw_event_id + post_code 合并拆行后的明细。
    read_events_dedup AS (
        SELECT
            dt,
            raw_event_id,
            post_code,
            ANY_VALUE(user_id) AS user_id,
            ANY_VALUE(module) AS module,
            ANY_VALUE(column_id) AS column_id
        FROM read_events_raw
        GROUP BY dt, raw_event_id, post_code
    ),
    -- 阅读日聚合：按已登录用户去重，产出阅读 UV。
    read_daily AS (
        SELECT
            dt,
            post_code,
            module,
            column_id,
            COUNT(DISTINCT user_id) AS read_uv
        FROM read_events_dedup
        WHERE module IS NOT NULL AND column_id IS NOT NULL
        GROUP BY dt, post_code, module, column_id
    ),
    -- 服务端试穿原始记录：只保留带 postId 的内容场景试穿任务。
    tryon_task_base AS (
        SELECT
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            NULLIF(JSON_VALUE(request_param, '$.postId'), '') AS post_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE DATE({service_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND task_type IN ('TRY_ON', 'STYLE_ME')
            AND NULLIF(JSON_VALUE(request_param, '$.postId'), '') IS NOT NULL
    ),
    -- 服务端试穿补齐帖子与专栏维度。
    tryon_task_enriched AS (
        SELECT
            t.dt,
            p.post_code,
            c.module,
            c.column_id
        FROM tryon_task_base t
        JOIN post_dim p
            ON t.post_id = p.post_id
        JOIN column_map c
            ON p.poster_column_id = c.column_id
    ),
    -- 服务端试穿日聚合：产出从该帖子发起的试穿次数。
    tryon_daily AS (
        SELECT
            dt,
            post_code,
            module,
            column_id,
            COUNT(*) AS tryon_total_count
        FROM tryon_task_enriched
        GROUP BY dt, post_code, module, column_id
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
        JOIN column_map c
            ON CAST(f.star_user_id AS STRING) = c.column_id
        WHERE DATE({service_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND f.star_user_id IS NOT NULL
            AND CAST(f.star_user_id AS STRING) != ''
            AND f.user_id IS NOT NULL
            AND CAST(f.user_id AS STRING) != ''
    ),
    -- 服务端关注归因到帖子：只在同日同专栏下，命中关注前最后一次帖子详情曝光。
    follow_attributed AS (
        SELECT
            f.dt,
            f.action_id,
            f.user_id,
            e.post_code,
            e.module,
            e.column_id
        FROM follow_service_base f
        JOIN exposure_events e
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
    -- 服务端关注日聚合：产出帖子级关注次数与关注 UV。
    follow_daily AS (
        SELECT
            dt,
            post_code,
            module,
            column_id,
            COUNT(*) AS follow_total_count,
            COUNT(DISTINCT user_id) AS follow_uv
        FROM follow_attributed
        GROUP BY dt, post_code, module, column_id
    ),
    -- 指标主键骨架：合并所有可能产出指标的帖子维度键。
    keys AS (
        SELECT DISTINCT dt, post_code, module, column_id
        FROM exposure_daily
        UNION DISTINCT
        SELECT DISTINCT dt, post_code, module, column_id
        FROM like_daily
        UNION DISTINCT
        SELECT DISTINCT dt, post_code, module, column_id
        FROM read_daily
        UNION DISTINCT
        SELECT DISTINCT dt, post_code, module, column_id
        FROM tryon_daily
        UNION DISTINCT
        SELECT DISTINCT dt, post_code, module, column_id
        FROM follow_daily
    ),
    -- 最终指标拼接层：补齐帖子和专栏名称，并保留率指标分子分母。
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
            COALESCE(r.read_uv, 0) AS read_uv,
            COALESCE(f.follow_total_count, 0) AS follow_total_count,
            COALESCE(f.follow_uv, 0) AS follow_uv,
            COALESCE(t.tryon_total_count, 0) AS tryon_total_count
        FROM keys k
        LEFT JOIN exposure_daily e USING (dt, post_code, module, column_id)
        LEFT JOIN like_daily l USING (dt, post_code, module, column_id)
        LEFT JOIN read_daily r USING (dt, post_code, module, column_id)
        LEFT JOIN follow_daily f USING (dt, post_code, module, column_id)
        LEFT JOIN tryon_daily t USING (dt, post_code, module, column_id)
        LEFT JOIN post_map pm USING (post_code)
        LEFT JOIN column_map cm
            ON cm.module = k.module
            AND cm.column_id = k.column_id
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
        CAST(CASE
            WHEN post_exposure_uv = 0 THEN 0
            ELSE ROUND(CAST(read_uv AS NUMERIC) / CAST(post_exposure_uv AS NUMERIC), 4)
        END AS NUMERIC) AS read_rate,
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
