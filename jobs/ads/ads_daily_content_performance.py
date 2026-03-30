import logging

from google.cloud import bigquery

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"

HOME_EXPOSURE_EVENTS = (
    "'v_home_star'," "'v_home_magazine'," "'v_home_brand'," "'v_home_feeds'"
)
POST_CONTENT_EXPOSURE_EVENTS = (
    "'v_star_post_detail',"
    "'v_magazine_post_detail',"
    "'v_brand_post_detail',"
    "'v_kol_post_detail'"
)
READ_EVENTS = (
    "'r_star_post_detail',"
    "'r_magazine_post_detail',"
    "'r_brand_post_detail',"
    "'r_kol_post_detail'"
)
COLUMN_EXPOSURE_EVENTS = (
    "'v_star_post_detail',"
    "'v_magazine_post_detail',"
    "'v_brand_post_detail',"
    "'v_kol_post_detail',"
    "'v_star_post_feeds',"
    "'v_brand_post_feeds'"
)
COLUMN_FOLLOW_EXPOSURE_EVENTS = (
    "'v_kol_post_feeds',"
    "'v_kol_post_detail',"
    "'v_star_post_feeds',"
    "'v_star_post_detail',"
    "'v_magazine_post_detail',"
    "'v_brand_post_feeds',"
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


def platform_expr():
    return (
        "CASE "
        "WHEN prop_url IS NOT NULL AND prop_url != '' THEN 'H5' "
        "WHEN LOWER(prop_os) = 'ios' THEN 'iOS' "
        "WHEN LOWER(prop_os) IN ('android', 'harmony') THEN 'Android' "
        "ELSE 'unknown' "
        "END"
    )


def build_ads_daily_content_performance_select_query(dates):
    dates_str = dates_to_sql_list(dates)
    event_ts = event_ts_expr()
    platform_case = platform_expr()

    return f"""
    WITH
    base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            {platform_case} AS platform,
            event_name,
            prop_user_id,
            prop_device_id,
            raw_event_id,
            hash_id,
            args_star,
            args_magazine,
            args_brand,
            args_blogger
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
    ),
    events AS (
        SELECT
            dt,
            platform,
            event_name,
            COALESCE(NULLIF(prop_user_id, ''), NULLIF(prop_device_id, '')) AS visitor_id,
            raw_event_id,
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
    ),
    date_platform_list AS (
        SELECT DISTINCT dt, platform
        FROM base
    ),
    dau_daily AS (
        SELECT
            dt,
            platform,
            COUNT(DISTINCT IF(visitor_id IS NOT NULL, visitor_id, NULL)) AS dau_uv
        FROM events
        GROUP BY dt, platform
    ),
    platform_daily AS (
        SELECT
            dt,
            platform,
            COUNT(DISTINCT IF(event_name IN ({HOME_EXPOSURE_EVENTS}), visitor_id, NULL)) AS platform_exposure_uv
        FROM events
        GROUP BY dt, platform
    ),
    exposure_daily AS (
        SELECT
            dt,
            platform,
            COUNT(DISTINCT IF(event_name IN ({POST_CONTENT_EXPOSURE_EVENTS}), visitor_id, NULL))
                AS content_exposure_uv,
            COUNT(DISTINCT IF(event_name IN ({POST_CONTENT_EXPOSURE_EVENTS}), hash_id, NULL))
                AS content_exposure_pv,
            COUNT(
                DISTINCT IF(
                    event_name IN ({COLUMN_EXPOSURE_EVENTS})
                    AND column_id IS NOT NULL,
                    hash_id,
                    NULL
                )
            ) AS column_exposure_pv,
            COUNT(
                DISTINCT IF(
                    event_name IN ({COLUMN_FOLLOW_EXPOSURE_EVENTS})
                    AND visitor_id IS NOT NULL
                    AND column_id IS NOT NULL,
                    visitor_id,
                    NULL
                )
            ) AS column_follow_exposure_uv
        FROM events
        GROUP BY dt, platform
    ),
    action_events AS (
        SELECT
            dt,
            platform,
            event_name,
            raw_event_id,
            ANY_VALUE(visitor_id) AS visitor_id,
            ANY_VALUE(column_id) AS column_id
        FROM events
        WHERE raw_event_id IS NOT NULL
            AND event_name IN ('c_like', 'c_follow', 'c_tryon')
        GROUP BY dt, platform, event_name, raw_event_id
    ),
    action_daily AS (
        SELECT
            dt,
            platform,
            COUNTIF(event_name = 'c_like') AS like_total_count,
            COUNTIF(event_name = 'c_follow' AND column_id IS NOT NULL) AS follow_total_count,
            COUNT(
                DISTINCT IF(
                    event_name = 'c_follow' AND column_id IS NOT NULL,
                    column_id,
                    NULL
                )
            ) AS follow_column_count,
            COUNTIF(event_name = 'c_tryon') AS tryon_total_count,
            COUNT(DISTINCT IF(event_name = 'c_like' AND visitor_id IS NOT NULL, visitor_id, NULL))
                AS like_uv,
            COUNT(
                DISTINCT IF(
                    event_name = 'c_follow'
                    AND visitor_id IS NOT NULL
                    AND column_id IS NOT NULL,
                    visitor_id,
                    NULL
                )
            ) AS follow_uv
        FROM action_events
        GROUP BY dt, platform
    ),
    read_daily AS (
        SELECT
            dt,
            platform,
            COUNT(DISTINCT IF(event_name IN ({READ_EVENTS}) AND visitor_id IS NOT NULL, visitor_id, NULL)) AS read_uv
        FROM events
        GROUP BY dt, platform
    ),
    daily AS (
        SELECT
            d.dt,
            d.platform,
            COALESCE(p.platform_exposure_uv, 0) AS platform_exposure_uv,
            COALESCE(e.content_exposure_uv, 0) AS content_exposure_uv,
            COALESCE(e.content_exposure_pv, 0) AS content_exposure_pv,
            COALESCE(a.like_total_count, 0) AS like_total_count,
            COALESCE(a.like_uv, 0) AS like_uv,
            COALESCE(r.read_uv, 0) AS read_uv,
            COALESCE(a.follow_total_count, 0) AS follow_total_count,
            COALESCE(a.follow_column_count, 0) AS follow_column_count,
            COALESCE(a.follow_uv, 0) AS follow_uv,
            COALESCE(u.dau_uv, 0) AS dau_uv,
            COALESCE(a.tryon_total_count, 0) AS tryon_total_count,
            COALESCE(e.column_exposure_pv, 0) AS column_exposure_pv,
            COALESCE(e.column_follow_exposure_uv, 0) AS column_follow_exposure_uv
        FROM date_platform_list d
        LEFT JOIN dau_daily u USING (dt, platform)
        LEFT JOIN platform_daily p USING (dt, platform)
        LEFT JOIN exposure_daily e USING (dt, platform)
        LEFT JOIN action_daily a USING (dt, platform)
        LEFT JOIN read_daily r USING (dt, platform)
    )
    SELECT
        dt,
        platform,
        platform_exposure_uv,
        content_exposure_uv,
        content_exposure_pv,
        CAST(CASE
            WHEN content_exposure_uv = 0 THEN 0
            ELSE ROUND(
                CAST(content_exposure_pv AS NUMERIC) / CAST(content_exposure_uv AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_browse_content_count_per_user,
        like_total_count,
        like_uv,
        CAST(CASE
            WHEN content_exposure_uv = 0 THEN 0
            ELSE ROUND(
                CAST(like_uv AS NUMERIC) / CAST(content_exposure_uv AS NUMERIC),
                4
            )
        END AS NUMERIC) AS like_rate,
        read_uv,
        CAST(CASE
            WHEN content_exposure_uv = 0 THEN 0
            ELSE ROUND(
                CAST(read_uv AS NUMERIC) / CAST(content_exposure_uv AS NUMERIC),
                4
            )
        END AS NUMERIC) AS read_rate,
        follow_total_count,
        follow_column_count,
        follow_uv,
        dau_uv,
        CAST(CASE
            WHEN dau_uv = 0 THEN 0
            ELSE ROUND(
                CAST(follow_uv AS NUMERIC) / CAST(dau_uv AS NUMERIC),
                4
            )
        END AS NUMERIC) AS follow_rate,
        CAST(CASE
            WHEN column_follow_exposure_uv = 0 THEN 0
            ELSE ROUND(
                CAST(follow_uv AS NUMERIC) / CAST(column_follow_exposure_uv AS NUMERIC),
                4
            )
        END AS NUMERIC) AS column_follow_rate,
        column_follow_exposure_uv,
        CAST(CASE
            WHEN column_exposure_pv = 0 THEN 0
            ELSE ROUND(
                CAST(follow_total_count AS NUMERIC) / CAST(column_exposure_pv AS NUMERIC),
                4
            )
        END AS NUMERIC) AS read_follow_rate,
        tryon_total_count,
        column_exposure_pv,
        CAST(CASE
            WHEN column_exposure_pv = 0 THEN 0
            ELSE ROUND(
                CAST(tryon_total_count AS NUMERIC) / CAST(column_exposure_pv AS NUMERIC),
                4
            )
        END AS NUMERIC) AS read_tryon_rate,
        CURRENT_TIMESTAMP() AS update_time
    FROM daily
    """


def run_ads_daily_content_performance(dates):
    """
    平台整体内容表现日报（每日每平台一行）:
    - platform: 设备类型（iOS / Android / H5 / unknown）
    - platform_exposure_uv: 平台曝光 UV（Home 曝光 UV）
    - content_exposure_uv: 帖子内容曝光 UV
    - content_exposure_pv: 帖子内容曝光 PV（历史仍按 hash_id 统计）
    - avg_browse_content_count_per_user: 人均帖子内容曝光数（进入帖子详情，历史字段名保留）
    - like_total_count: 点赞总数（点赞成功次数）
    - like_uv: 点赞 UV
    - like_rate: 点赞率（点赞 UV / 帖子内容曝光 UV）
    - read_uv: 完读 UV
    - read_rate: 完读率（阅读 UV / 帖子内容曝光 UV）
    - follow_total_count: 关注总数（关注专栏点击次数）
    - follow_column_count: 关注专栏个数（当天被关注过的去重专栏数）
    - follow_uv: 关注 UV（c_follow 去重访客数）
    - dau_uv: 日活 UV（任意事件活跃 visitor_id）
    - follow_rate: 关注率（关注 UV / 日活 UV）
    - column_follow_rate: 专栏关注率（关注 UV / 指定专栏内容曝光 UV）
    - column_follow_exposure_uv: 用于重算专栏关注率的专栏内容曝光 UV
    - read_follow_rate: 曝光关注率（关注专栏次数 / 专栏曝光次数，历史字段名保留）
    - tryon_total_count: 上身试穿总次数（开始试穿 PV）
    - column_exposure_pv: 用于重算曝光试穿率的专栏曝光 PV
    - read_tryon_rate: 曝光试穿率（试穿 PV / 专栏曝光 PV，历史字段名保留）
    使用多伦多时间（America/Toronto）。
    """
    dates_str = dates_to_sql_list(dates)
    select_query = build_ads_daily_content_performance_select_query(dates)

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_content_performance`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ads_daily_content_performance`
    (dt, platform, platform_exposure_uv, content_exposure_uv, content_exposure_pv,
     avg_browse_content_count_per_user, like_total_count, like_uv, like_rate,
     read_uv, read_rate, follow_total_count, follow_column_count, follow_uv,
     dau_uv, follow_rate, column_follow_rate, column_follow_exposure_uv,
     read_follow_rate, tryon_total_count, column_exposure_pv,
     read_tryon_rate, update_time)
    {select_query};
    """
    logging.info("开始处理: ads_daily_content_performance")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_content_performance 刷新完成, 处理日期: {dates}")
