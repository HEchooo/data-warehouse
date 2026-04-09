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


def service_ts_expr(field_name: str = "gmt_create") -> str:
    """v3 服务表的 gmt_create 是 UTC DATETIME，这里统一转成 TIMESTAMP。"""
    return f"TIMESTAMP({field_name}, 'UTC')"


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
    专栏追踪明细（日×专栏）:
    粒度：dt × module × column_id

    指标口径：
    - column_exposure_uv：专栏曝光 UV（同日同设备若后续登录则归并到 user_id）
    - follow_total_count：关注点击次数（c_follow，按 raw_event_id 去重）
    - follow_rate：关注点击 UV / 专栏曝光 UV
    - read_post_count：同一人同天同专栏曝光到的去重帖子数
    - avg_read_post_count_per_user：read_post_count / column_exposure_uv
    - post_total_count：服务端当天创建且可映射到专栏的帖子总数
    - tryon_post_count：当天发生 TRY_ON / STYLE_ME 的去重帖子数
    - tryon_click_uv：当天发生 TRY_ON / STYLE_ME 的去重用户数
    - tryon_uv_rate：tryon_click_uv / column_exposure_uv
    - ai_tryon_penetration_rate：tryon_post_count / post_total_count
    - like_post_count：同一专栏中，当天获得过点赞的去重帖子数
    - like_rate：点赞人数 / 专栏曝光人数
    - new_follow_count：当天新增关注该专栏的去重用户数
    - follow_conversion_rate_7d：近7日点赞用户中，今日发生关注行为的用户数 / 近7日点赞用户数

    说明：
    - 去掉 creator 粒度后，c_follow 不再做 creator 归因，只保留专栏级统计
    - 若同日同 device 只关联 1 个 user_id，则游客态并到该 user_id
    - 若同日同 device 关联多个 user_id，则人数口径按 distinct user_id 统计，游客态不再额外计 UV
    """
    dates_str = dates_to_sql_list(dates)
    event_ts = event_ts_expr()
    service_ts = service_ts_expr()
    module_sql = module_expr()
    column_id_sql = column_id_expr()

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_column_performance`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ads_daily_column_performance`
    (dt, module, column_id, column_name,
     column_exposure_uv, follow_total_count, follow_rate,
     read_post_count, avg_read_post_count_per_user,
     post_total_count, tryon_post_count, tryon_click_uv,
     tryon_uv_rate, ai_tryon_penetration_rate,
     like_post_count, like_rate, new_follow_count,
     follow_conversion_rate_7d, update_time)
    WITH
    -- 当前批次要刷新的目标日期列表。
    target_dates AS (
        SELECT DATE(dt_str) AS dt
        FROM UNNEST([{dates_str}]) AS dt_str
    ),
    -- 目标日期窗口边界，用于拉取近7日点赞数据。
    target_date_bounds AS (
        SELECT
            MIN(dt) AS min_dt,
            MAX(dt) AS max_dt
        FROM target_dates
    ),
    -- 事件日志原始层：先把 user/device 分开保留，后续再统一 UV 归并。
    base_raw AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            {event_ts} AS event_ts,
            event_name,
            NULLIF(prop_user_id, '') AS user_id,
            NULLIF(prop_device_id, '') AS device_id,
            raw_event_id,
            hash_id,
            post_code,
            args_star,
            args_magazine,
            args_brand
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{TORONTO_TZ}') IN (SELECT dt FROM target_dates)
    ),
    -- 每天每个 device 命中的登录用户集合，用于修正游客后续登录的 UV 归并口径。
    device_user_daily AS (
        SELECT
            dt,
            device_id,
            COUNT(DISTINCT user_id) AS user_id_count,
            ANY_VALUE(user_id) AS single_user_id
        FROM base_raw
        WHERE device_id IS NOT NULL
            AND user_id IS NOT NULL
        GROUP BY dt, device_id
    ),
    -- 应用统一访客口径后的事件层。
    base AS (
        SELECT
            b.dt,
            b.event_ts,
            b.event_name,
            CASE
                WHEN b.user_id IS NOT NULL THEN CONCAT('u:', b.user_id)
                WHEN b.device_id IS NULL THEN NULL
                WHEN COALESCE(d.user_id_count, 0) = 0 THEN CONCAT('d:', b.device_id)
                WHEN d.user_id_count = 1 THEN CONCAT('u:', d.single_user_id)
                ELSE NULL
            END AS visitor_id,
            b.raw_event_id,
            b.hash_id,
            b.post_code,
            b.args_star,
            b.args_magazine,
            b.args_brand
        FROM base_raw b
        LEFT JOIN device_user_daily d
            ON b.dt = d.dt
            AND b.device_id = d.device_id
    ),
    -- 专栏维表映射：把 kol_type 统一成报表使用的 module。
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
        WHERE status = 0
            AND kol_type IN (2, 3, 4)
            AND user_id IS NOT NULL
        GROUP BY module, column_id
    ),
    -- 专栏曝光事件明细。
    exposure_events AS (
        SELECT
            dt,
            visitor_id,
            post_code,
            {module_sql} AS module,
            {column_id_sql} AS column_id
        FROM base
        WHERE event_name IN ({COLUMN_EXPOSURE_EVENTS})
    ),
    -- 按专栏聚合曝光 UV 与去重阅读帖子数。
    exposure_daily AS (
        SELECT
            dt,
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
        WHERE module IS NOT NULL
            AND column_id IS NOT NULL
        GROUP BY dt, module, column_id
    ),
    -- c_follow 事件先按 raw_event_id 去重，保留专栏级点击事实。
    follow_events_dedup AS (
        SELECT
            dt,
            raw_event_id,
            ANY_VALUE(visitor_id) AS visitor_id,
            ANY_VALUE({module_sql}) AS module,
            ANY_VALUE({column_id_sql}) AS column_id
        FROM base
        WHERE event_name = 'c_follow'
            AND raw_event_id IS NOT NULL
        GROUP BY dt, raw_event_id
    ),
    -- 按专栏聚合关注点击次数与关注点击 UV。
    follow_daily AS (
        SELECT
            dt,
            module,
            column_id,
            COUNT(*) AS follow_total_count,
            COUNT(DISTINCT IF(visitor_id IS NOT NULL, visitor_id, NULL)) AS follow_uv
        FROM follow_events_dedup
        WHERE module IS NOT NULL
            AND column_id IS NOT NULL
        GROUP BY dt, module, column_id
    ),
    -- 读取已固化的专栏在线帖子日快照，避免每次 ADS 重扫帖子存量。
    post_snapshot_daily AS (
        SELECT
            dt,
            module,
            column_id,
            post_total_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_column_post_daily`
        WHERE dt IN (SELECT dt FROM target_dates)
    ),
    -- 当天试穿任务原始记录；试穿是日发生指标，不走快照。
    tryon_task_base AS (
        SELECT
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            NULLIF(JSON_VALUE(request_param, '$.postId'), '') AS post_id,
            COALESCE(
                NULLIF(JSON_VALUE(request_param, '$.user_id'), ''),
                NULLIF(JSON_VALUE(request_param, '$.userId'), ''),
                NULLIF(user_id, '')
            ) AS tryon_user_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE DATE({service_ts}, '{TORONTO_TZ}') IN (SELECT dt FROM target_dates)
            AND task_type IN ('TRY_ON', 'STYLE_ME')
    ),
    -- 把试穿任务映射到帖子所属专栏。
    tryon_task_enriched AS (
        SELECT
            t.dt,
            cm.module,
            CAST(p.poster_user_id AS STRING) AS column_id,
            t.post_id,
            t.tryon_user_id
        FROM tryon_task_base t
        JOIN `{PROJECT_ID}.{V3_DATASET_ID}.community_post` p
            ON t.post_id = CAST(p.id AS STRING)
        JOIN column_map cm
            ON cm.column_id = CAST(p.poster_user_id AS STRING)
        WHERE t.post_id IS NOT NULL
            AND p.poster_user_id IS NOT NULL
            AND CAST(p.poster_user_id AS STRING) != ''
    ),
    -- 按专栏聚合试穿帖子数与试穿点击人数。
    tryon_daily AS (
        SELECT
            dt,
            module,
            column_id,
            COUNT(DISTINCT post_id) AS tryon_post_count,
            COUNT(DISTINCT IF(tryon_user_id IS NOT NULL, tryon_user_id, NULL)) AS tryon_click_uv
        FROM tryon_task_enriched
        GROUP BY dt, module, column_id
    ),
    -- 点赞服务表原始记录（覆盖目标日期最早日往前 6 天到目标日期最晚日）。
    like_service_base_7d AS (
        SELECT
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            CAST(post_id AS STRING) AS post_id,
            CAST(user_id AS STRING) AS user_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_post_like`
        WHERE DATE({service_ts}, '{TORONTO_TZ}') BETWEEN (
                SELECT DATE_SUB(min_dt, INTERVAL 6 DAY)
                FROM target_date_bounds
            ) AND (
                SELECT max_dt
                FROM target_date_bounds
            )
            AND post_id IS NOT NULL
            AND user_id IS NOT NULL
            AND CAST(user_id AS STRING) != ''
    ),
    -- 把点赞记录挂到帖子所属专栏。
    like_service_enriched_7d AS (
        SELECT
            l.dt,
            cm.module,
            CAST(p.poster_user_id AS STRING) AS column_id,
            l.post_id,
            l.user_id
        FROM like_service_base_7d l
        JOIN `{PROJECT_ID}.{V3_DATASET_ID}.community_post` p
            ON l.post_id = CAST(p.id AS STRING)
        JOIN column_map cm
            ON cm.column_id = CAST(p.poster_user_id AS STRING)
        WHERE p.poster_user_id IS NOT NULL
            AND CAST(p.poster_user_id AS STRING) != ''
    ),
    -- 按专栏聚合“获得点赞的帖子数”和点赞人数。
    like_daily AS (
        SELECT
            dt,
            module,
            column_id,
            COUNT(DISTINCT post_id) AS like_post_count,
            COUNT(DISTINCT user_id) AS like_uv
        FROM like_service_enriched_7d
        WHERE dt IN (SELECT dt FROM target_dates)
        GROUP BY dt, module, column_id
    ),
    -- 服务端新增关注记录。
    follow_service_base AS (
        SELECT
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            cm.module,
            CAST(star_user_id AS STRING) AS column_id,
            CAST(user_id AS STRING) AS user_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_star_follow` f
        JOIN column_map cm
            ON cm.column_id = CAST(f.star_user_id AS STRING)
        WHERE DATE({service_ts}, '{TORONTO_TZ}') IN (SELECT dt FROM target_dates)
            AND star_user_id IS NOT NULL
            AND CAST(star_user_id AS STRING) != ''
            AND user_id IS NOT NULL
            AND CAST(user_id AS STRING) != ''
    ),
    -- 按专栏聚合新增关注用户数。
    follow_service_daily AS (
        SELECT
            dt,
            module,
            column_id,
            COUNT(DISTINCT user_id) AS new_follow_count
        FROM follow_service_base
        GROUP BY dt, module, column_id
    ),
    -- 每个目标日期构建近7日点赞用户集合（含当日，向前 6 天）。
    like_user_window_7d AS (
        SELECT DISTINCT
            t.dt,
            l.module,
            l.column_id,
            l.user_id
        FROM target_dates t
        JOIN like_service_enriched_7d l
            ON l.dt BETWEEN DATE_SUB(t.dt, INTERVAL 6 DAY) AND t.dt
    ),
    -- 近7日点赞去重用户数（作为近7日关注转化率分母）。
    like_user_7d_daily AS (
        SELECT
            dt,
            module,
            column_id,
            COUNT(DISTINCT user_id) AS like_uv_7d
        FROM like_user_window_7d
        GROUP BY dt, module, column_id
    ),
    -- 先拉平今日关注用户集合，供近7日关注转化率求交集使用。
    follow_user_daily AS (
        SELECT DISTINCT
            dt,
            module,
            column_id,
            user_id
        FROM follow_service_base
    ),
    -- 计算同日同专栏“今日关注且近7日内点过赞”的去重用户数（7日口径）。
    follow_like_7d_daily AS (
        SELECT
            f.dt,
            f.module,
            f.column_id,
            COUNT(DISTINCT f.user_id) AS follow_like_uv_7d
        FROM follow_user_daily f
        JOIN like_user_window_7d l
            ON f.dt = l.dt
            AND f.module = l.module
            AND f.column_id = l.column_id
            AND f.user_id = l.user_id
        GROUP BY f.dt, f.module, f.column_id
    ),
    -- 汇总所有可能产出指标的专栏主键。
    keys AS (
        SELECT DISTINCT dt, module, column_id
        FROM exposure_daily
        UNION DISTINCT
        SELECT DISTINCT dt, module, column_id
        FROM follow_daily
        UNION DISTINCT
        SELECT DISTINCT dt, module, column_id
        FROM post_snapshot_daily
        UNION DISTINCT
        SELECT DISTINCT dt, module, column_id
        FROM tryon_daily
        UNION DISTINCT
        SELECT DISTINCT dt, module, column_id
        FROM like_daily
        UNION DISTINCT
        SELECT DISTINCT dt, module, column_id
        FROM follow_service_daily
        UNION DISTINCT
        SELECT DISTINCT dt, module, column_id
        FROM like_user_7d_daily
        UNION DISTINCT
        SELECT DISTINCT dt, module, column_id
        FROM follow_like_7d_daily
    ),
    -- 汇总到最终报表所需的分子分母。
    daily AS (
        SELECT
            k.dt,
            k.module,
            k.column_id,
            COALESCE(cm.column_name, '') AS column_name,
            COALESCE(e.column_exposure_uv, 0) AS column_exposure_uv,
            COALESCE(f.follow_total_count, 0) AS follow_total_count,
            COALESCE(f.follow_uv, 0) AS follow_uv,
            COALESCE(e.read_post_count, 0) AS read_post_count,
            COALESCE(s.post_total_count, 0) AS post_total_count,
            COALESCE(t.tryon_post_count, 0) AS tryon_post_count,
            COALESCE(t.tryon_click_uv, 0) AS tryon_click_uv,
            COALESCE(l.like_post_count, 0) AS like_post_count,
            COALESCE(l.like_uv, 0) AS like_uv,
            COALESCE(l7.like_uv_7d, 0) AS like_uv_7d,
            COALESCE(fs.new_follow_count, 0) AS new_follow_count,
            COALESCE(fl7.follow_like_uv_7d, 0) AS follow_like_uv_7d
        FROM keys k
        LEFT JOIN exposure_daily e
            ON k.dt = e.dt
            AND k.module = e.module
            AND k.column_id = e.column_id
        LEFT JOIN follow_daily f
            ON k.dt = f.dt
            AND k.module = f.module
            AND k.column_id = f.column_id
        LEFT JOIN post_snapshot_daily s
            ON k.dt = s.dt
            AND k.module = s.module
            AND k.column_id = s.column_id
        LEFT JOIN tryon_daily t
            ON k.dt = t.dt
            AND k.module = t.module
            AND k.column_id = t.column_id
        LEFT JOIN like_daily l
            ON k.dt = l.dt
            AND k.module = l.module
            AND k.column_id = l.column_id
        LEFT JOIN like_user_7d_daily l7
            ON k.dt = l7.dt
            AND k.module = l7.module
            AND k.column_id = l7.column_id
        LEFT JOIN follow_service_daily fs
            ON k.dt = fs.dt
            AND k.module = fs.module
            AND k.column_id = fs.column_id
        LEFT JOIN follow_like_7d_daily fl7
            ON k.dt = fl7.dt
            AND k.module = fl7.module
            AND k.column_id = fl7.column_id
        LEFT JOIN column_map cm
            ON k.module = cm.module
            AND k.column_id = cm.column_id
    )
    SELECT
        dt,
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
        post_total_count,
        tryon_post_count,
        tryon_click_uv,
        CAST(CASE
            WHEN column_exposure_uv = 0 THEN 0
            ELSE ROUND(
                CAST(tryon_click_uv AS NUMERIC) / CAST(column_exposure_uv AS NUMERIC),
                4
            )
        END AS NUMERIC) AS tryon_uv_rate,
        CAST(CASE
            WHEN post_total_count = 0 THEN 0
            ELSE ROUND(
                CAST(tryon_post_count AS NUMERIC) / CAST(post_total_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS ai_tryon_penetration_rate,
        like_post_count,
        CAST(CASE
            WHEN column_exposure_uv = 0 THEN 0
            ELSE ROUND(
                CAST(like_uv AS NUMERIC) / CAST(column_exposure_uv AS NUMERIC),
                4
            )
        END AS NUMERIC) AS like_rate,
        new_follow_count,
        CAST(CASE
            WHEN like_uv_7d = 0 THEN 0
            ELSE ROUND(
                CAST(follow_like_uv_7d AS NUMERIC) / CAST(like_uv_7d AS NUMERIC),
                4
            )
        END AS NUMERIC) AS follow_conversion_rate_7d,
        CURRENT_TIMESTAMP() AS update_time
    FROM daily;
    """

    logging.info("开始处理: ads_daily_column_performance")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_column_performance 刷新完成, 处理日期: {dates}")
