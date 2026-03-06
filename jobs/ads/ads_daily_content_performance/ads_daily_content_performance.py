import logging

from google.cloud import bigquery

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"

HOME_EXPOSURE_EVENTS = (
    "'v_home_star',"
    "'v_home_magazine',"
    "'v_home_brand',"
    "'v_home_feeds'"
)
CONTENT_READ_EVENTS = (
    "'v_product_detail',"
    "'v_star_post_detail',"
    "'v_magazine_post_detail',"
    "'v_brand_post_detail',"
    "'v_kol_post_detail'"
)
COLUMN_READ_EVENTS = (
    "'v_star_post_detail',"
    "'v_magazine_post_detail',"
    "'v_brand_post_detail',"
    "'v_kol_post_detail'"
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


def run_ads_daily_content_performance(dates):
    """
    平台整体内容表现日报（每日一行）:
    - platform_exposure_uv: 平台曝光 UV（Home 曝光 UV）
    - avg_browse_content_count_per_user: 人均浏览内容数（进入详情）
    - like_total_count: 点赞总数（点赞成功次数）
    - like_rate: 点赞率（点赞 UV / 内容曝光 PV）
    - follow_total_count: 关注总数（关注成功次数）
    - read_follow_rate: 阅读关注率（关注 UV / 专栏阅读 UV）
    - tryon_total_count: 上身试穿总次数（开始试穿 PV）
    - read_tryon_rate: 阅读试穿率（试穿 PV / 专栏阅读 PV）
    使用多伦多时间（America/Toronto）。
    """
    dates_str = dates_to_sql_list(dates)
    event_ts = event_ts_expr()

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_content_performance`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ads_daily_content_performance`
    (dt, platform_exposure_uv, avg_browse_content_count_per_user,
     like_total_count, like_rate, follow_total_count, read_follow_rate,
     tryon_total_count, read_tryon_rate, update_time)
    WITH
    base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            event_name,
            prop_user_id,
            prop_device_id,
            raw_event_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
    ),
    events AS (
        SELECT
            dt,
            event_name,
            COALESCE(NULLIF(prop_user_id, ''), NULLIF(prop_device_id, '')) AS visitor_id,
            raw_event_id
        FROM base
    ),
    date_list AS (
        SELECT DISTINCT dt
        FROM base
    ),
    platform_daily AS (
        SELECT
            dt,
            COUNT(DISTINCT IF(event_name IN ({HOME_EXPOSURE_EVENTS}), visitor_id, NULL)) AS platform_exposure_uv
        FROM events
        GROUP BY dt
    ),
    raw_events AS (
        SELECT
            dt,
            event_name,
            raw_event_id,
            ANY_VALUE(visitor_id) AS visitor_id
        FROM events
        WHERE raw_event_id IS NOT NULL
            AND event_name IN (
                {CONTENT_READ_EVENTS},
                'c_like',
                'c_follow',
                'c_tryon'
            )
        GROUP BY dt, event_name, raw_event_id
    ),
    uv_daily AS (
        SELECT
            dt,
            COUNT(DISTINCT IF(event_name IN ({CONTENT_READ_EVENTS}), visitor_id, NULL)) AS content_read_uv,
            COUNT(DISTINCT IF(event_name = 'c_like', visitor_id, NULL)) AS like_uv,
            COUNT(DISTINCT IF(event_name = 'c_follow', visitor_id, NULL)) AS follow_uv,
            COUNT(DISTINCT IF(event_name IN ({COLUMN_READ_EVENTS}), visitor_id, NULL)) AS column_read_uv
        FROM events
        GROUP BY dt
    ),
    raw_daily AS (
        SELECT
            dt,
            COUNTIF(event_name IN ({CONTENT_READ_EVENTS})) AS content_read_pv,
            COUNTIF(event_name = 'c_like') AS like_total_count,
            COUNTIF(event_name = 'c_follow') AS follow_total_count,
            COUNTIF(event_name = 'c_tryon') AS tryon_total_count,
            COUNTIF(event_name IN ({COLUMN_READ_EVENTS})) AS column_read_pv
        FROM raw_events
        GROUP BY dt
    ),
    daily AS (
        SELECT
            d.dt,
            COALESCE(p.platform_exposure_uv, 0) AS platform_exposure_uv,
            COALESCE(r.content_read_pv, 0) AS content_read_pv,
            COALESCE(u.content_read_uv, 0) AS content_read_uv,
            COALESCE(r.like_total_count, 0) AS like_total_count,
            COALESCE(u.like_uv, 0) AS like_uv,
            COALESCE(r.follow_total_count, 0) AS follow_total_count,
            COALESCE(u.follow_uv, 0) AS follow_uv,
            COALESCE(r.tryon_total_count, 0) AS tryon_total_count,
            COALESCE(u.column_read_uv, 0) AS column_read_uv,
            COALESCE(r.column_read_pv, 0) AS column_read_pv
        FROM date_list d
        LEFT JOIN platform_daily p USING (dt)
        LEFT JOIN uv_daily u USING (dt)
        LEFT JOIN raw_daily r USING (dt)
    )
    SELECT
        dt,
        platform_exposure_uv,
        CAST(CASE
            WHEN content_read_uv = 0 THEN 0
            ELSE ROUND(CAST(content_read_pv AS NUMERIC) / CAST(content_read_uv AS NUMERIC), 4)
        END AS NUMERIC) AS avg_browse_content_count_per_user,
        like_total_count,
        CAST(CASE
            WHEN content_read_pv = 0 THEN 0
            ELSE ROUND(CAST(like_uv AS NUMERIC) / CAST(content_read_pv AS NUMERIC), 4)
        END AS NUMERIC) AS like_rate,
        follow_total_count,
        CAST(CASE
            WHEN column_read_uv = 0 THEN 0
            ELSE ROUND(CAST(follow_uv AS NUMERIC) / CAST(column_read_uv AS NUMERIC), 4)
        END AS NUMERIC) AS read_follow_rate,
        tryon_total_count,
        CAST(CASE
            WHEN column_read_pv = 0 THEN 0
            ELSE ROUND(CAST(tryon_total_count AS NUMERIC) / CAST(column_read_pv AS NUMERIC), 4)
        END AS NUMERIC) AS read_tryon_rate,
        CURRENT_TIMESTAMP() AS update_time
    FROM daily;
    """
    logging.info("开始处理: ads_daily_content_performance")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_content_performance 刷新完成, 处理日期: {dates}")
