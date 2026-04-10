import logging
from datetime import date

from google.cloud import bigquery

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
VIDEO_SOURCE_DATASET_ID = "videos"
TORONTO_TZ = "America/Toronto"
YOUTUBE_BOB_TEAM_CHANNEL_IDS = [
    "UCynAg7WHdiy8SuQpESO__yw",
    "UCgBJozWjiE0cqwv7UNalyog",
    "UC4rj_IlO3V-d5ebVhgagh1w",
    "UC70Y2-yeuFOoTx6yNkqioKA",
    "UCiX5DzRDNIb9fH_MQvoIN7w",
    "UCLs3oc5eyptqVTEnXaIwHvQ",
    "UCPAQ09Nusb_OZzwfWGAE_Aw",
    "UCV5FBW_l8cayS8L_zAIvlkQ",
    "UCVsLt_8hbY5nG5da8sslD_w",
    "UCzIQMkLCHa63JekjxaA133w",
]
client = bigquery.Client(project=PROJECT_ID)


def dates_to_sql_list(dates):
    return ", ".join([f"'{d}'" for d in dates])


def dates_to_array_sql(dates):
    return ", ".join([f"DATE '{d}'" for d in dates])


def strings_to_sql_list(values):
    return ", ".join([f"'{value}'" for value in values])


def run_ads_daily_investor(dates: list[date]):
    """
    投资人主题日报（dt 一行）:
    - 视频侧指标基于 dws_video_daily，只保留 jhonson 团队视频
    - 新增视频平均播放数按新增视频在首次正播放采集日的累计播放数 / 新增视频数计算
    - 新增下载直接复用 dws_appsflyer_download_daily
    - 设备类指标只保留 app 数据
    - 时长 / 内容 / 商详 / 留存只保留 app 已注册用户数据
    - 留存 cohort 为当日活跃注册用户，不是 is_new_user
    """

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    min_date = min(dates)
    max_date = max(dates)
    youtube_bob_team_channel_ids_sql = strings_to_sql_list(
        YOUTUBE_BOB_TEAM_CHANNEL_IDS
    )

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_investor`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ads_daily_investor`
    (
        dt,
        new_video_count,
        active_video_count,
        new_video_view_count,
        new_video_contributed_view_count,
        avg_video_view_count,
        avg_new_video_view_count,
        new_download_count,
        play_download_conversion_rate,
        avg_video_download_conversion_count,
        new_device_count,
        active_device_count,
        active_registered_user_count,
        avg_duration_sec,
        avg_content_exposure_count,
        avg_content_click_count,
        content_ctr,
        product_detail_exposure_uv,
        product_detail_exposure_pv,
        avg_product_detail_exposure_count,
        next_day_retention_rate,
        day_7_retention_rate,
        update_time
    )
    WITH
    -- 生成本次待刷的日期清单。
    date_list AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 生成 jhonson 团队视频键。
    -- 当前 `docs/rules/video-mapping.md` 只对 YouTube 定义了 bob / jhonson 边界，
    -- 因此这里对 YouTube 排除 bob 团队渠道；TikTok / Ins 维持现有全量口径。
    johnson_video_keys AS (
        SELECT CONCAT('youtube:', CAST(video_id AS STRING)) AS video_key
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.yt_videos`
        WHERE channel_id NOT IN ({youtube_bob_team_channel_ids_sql})

        UNION DISTINCT

        SELECT CONCAT('tiktok:', CAST(id AS STRING)) AS video_key
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.videos`

        UNION DISTINCT

        SELECT CONCAT('ins:', CAST(media_id AS STRING)) AS video_key
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.ig_media`
    ),
    -- 汇总 jhonson 团队的视频相关指标，区分全量日增播放与新增视频贡献播放。
    video_metrics AS (
        SELECT
            v.dt,
            COUNT(DISTINCT IF(v.is_new_video, v.video_key, NULL)) AS new_video_count,
            COUNT(DISTINCT IF(v.is_active_video, v.video_key, NULL)) AS active_video_count,
            COALESCE(SUM(v.daily_view_increment), 0) AS new_video_view_count,
            COALESCE(SUM(v.new_video_contributed_view_count), 0)
                AS new_video_contributed_view_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_video_daily` v
        INNER JOIN johnson_video_keys j
            ON v.video_key = j.video_key
        WHERE v.dt IN ({dates_str})
        GROUP BY v.dt
    ),
    -- 汇总当日新增下载数。
    download_metrics AS (
        SELECT
            dt,
            COALESCE(SUM(new_download_count), 0) AS new_download_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_appsflyer_download_daily`
        WHERE dt IN ({dates_str})
        GROUP BY dt
    ),
    -- 计算 app 设备的首次活跃日，用于“新增设备数”的 app 口径。
    app_device_first_active AS (
        SELECT
            prop_device_id,
            MIN(dt) AS first_app_active_dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
        WHERE dt <= DATE '{max_date}'
            AND platform != 'h5'
        GROUP BY prop_device_id
    ),
    -- 汇总 app 设备规模。
    device_metrics AS (
        SELECT
            d.dt,
            COUNT(
                DISTINCT IF(f.first_app_active_dt = d.dt, d.prop_device_id, NULL)
            ) AS new_device_count,
            COUNT(DISTINCT d.prop_device_id) AS active_device_count
        FROM (
            SELECT
                dt,
                prop_device_id
            FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
            WHERE dt IN ({dates_str})
                AND platform != 'h5'
            GROUP BY dt, prop_device_id
        ) d
        LEFT JOIN app_device_first_active f
            ON d.prop_device_id = f.prop_device_id
        GROUP BY d.dt
    ),
    -- 提取本次日期内 app 已注册用户事件，供时长 / 内容 / 商详指标复用。
    app_registered_event_base AS (
        SELECT
            DATE(
                TIMESTAMP(
                    DATETIME(logAt_timestamp, 'UTC'),
                    COALESCE(NULLIF(prop_timezone, ''), 'UTC')
                ),
                '{TORONTO_TZ}'
            ) AS dt,
            hash_id,
            raw_event_id,
            event_name,
            prop_device_id,
            prop_user_id,
            args_session_duration,
            product_code,
            post_code
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
    ),
    -- 汇总 app 已注册用户对应的活跃设备数，作为人均指标分母。
    app_registered_device_metrics AS (
        SELECT
            dt,
            COUNT(DISTINCT prop_device_id) AS active_registered_app_device_count
        FROM app_registered_event_base
        WHERE prop_device_id IS NOT NULL
            AND prop_device_id != ''
        GROUP BY dt
    ),
    -- 对 app 已注册用户的 app_launch 时长事件去重后汇总。
    app_registered_duration_metrics AS (
        SELECT
            dt,
            COALESCE(SUM(duration_ms), 0) / 1000.0 AS total_duration_sec
        FROM (
            SELECT
                dt,
                prop_device_id,
                COALESCE(raw_event_id, hash_id) AS duration_event_id,
                MAX(CAST(args_session_duration AS FLOAT64)) AS duration_ms
            FROM app_registered_event_base
            WHERE event_name = 'app_launch'
                AND prop_device_id IS NOT NULL
                AND prop_device_id != ''
                AND args_session_duration IS NOT NULL
                AND COALESCE(raw_event_id, hash_id) IS NOT NULL
            GROUP BY dt, prop_device_id, duration_event_id
        )
        GROUP BY dt
    ),
    -- 汇总 app 已注册用户的内容 item 曝光 / 点击量。
    content_metrics AS (
        SELECT
            dt,
            COUNTIF(event_group = 'exposure') AS exposure_item_count,
            COUNTIF(event_group = 'click') AS click_item_count
        FROM (
            SELECT
                dt,
                event_group
            FROM (
                SELECT
                    dt,
                    'exposure' AS event_group,
                    raw_event_id,
                    COALESCE(
                        IF(
                            product_code IS NULL OR product_code = '',
                            NULL,
                            CONCAT('product:', product_code)
                        ),
                        IF(
                            post_code IS NULL OR post_code = '',
                            NULL,
                            CONCAT('post:', post_code)
                        )
                    ) AS item_key
                FROM app_registered_event_base
                WHERE event_name IN (
                    'v_home_feeds',
                    'v_shop_feeds',
                    'v_star_post_feeds',
                    'v_brand_post_feeds',
                    'v_kol_post_feeds'
                )
                    AND raw_event_id IS NOT NULL

                UNION ALL

                SELECT
                    dt,
                    'click' AS event_group,
                    raw_event_id,
                    COALESCE(
                        IF(
                            product_code IS NULL OR product_code = '',
                            NULL,
                            CONCAT('product:', product_code)
                        ),
                        IF(
                            post_code IS NULL OR post_code = '',
                            NULL,
                            CONCAT('post:', post_code)
                        )
                    ) AS item_key
                FROM app_registered_event_base
                WHERE event_name IN (
                    'v_product_detail',
                    'v_star_post_detail',
                    'v_magazine_post_detail',
                    'v_brand_post_detail',
                    'v_kol_post_detail'
                )
                    AND raw_event_id IS NOT NULL
            )
            WHERE item_key IS NOT NULL
            GROUP BY dt, event_group, raw_event_id, item_key
        )
        GROUP BY dt
    ),
    -- 汇总 app 已注册用户的商详页 UV / PV。
    product_detail_metrics AS (
        SELECT
            dt,
            COUNT(DISTINCT IF(prop_user_id IS NOT NULL, prop_user_id, NULL))
                AS product_detail_exposure_uv,
            COUNT(DISTINCT IF(event_id IS NOT NULL, event_id, NULL))
                AS product_detail_exposure_pv
        FROM (
            SELECT
                dt,
                prop_user_id,
                COALESCE(raw_event_id, hash_id) AS event_id
            FROM app_registered_event_base
            WHERE event_name = 'v_product_detail'
        )
        GROUP BY dt
    ),
    -- 准备 app 活跃注册用户留存口径所需的用户明细。
    active_users AS (
        SELECT
            dt,
            prop_user_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        WHERE dt BETWEEN DATE '{min_date}' AND LEAST(
            DATE_ADD(DATE '{max_date}', INTERVAL 7 DAY),
            CURRENT_DATE('{TORONTO_TZ}')
        )
            AND platform != 'h5'
        GROUP BY dt, prop_user_id
    ),
    -- 计算 app 活跃注册用户的 1 日和 7 日留存分子分母。
    retention_metrics AS (
        SELECT
            d.dt,
            COUNT(DISTINCT t.prop_user_id) AS active_registered_user_count,
            COUNT(DISTINCT n1.prop_user_id) AS retained_1d_user_count,
            COUNT(DISTINCT n7.prop_user_id) AS retained_7d_user_count
        FROM date_list d
        LEFT JOIN active_users t
            ON t.dt = d.dt
        LEFT JOIN active_users n1
            ON t.prop_user_id = n1.prop_user_id
            AND n1.dt = DATE_ADD(d.dt, INTERVAL 1 DAY)
        LEFT JOIN active_users n7
            ON t.prop_user_id = n7.prop_user_id
            AND n7.dt = DATE_ADD(d.dt, INTERVAL 7 DAY)
        GROUP BY d.dt
    )
    SELECT
        d.dt,
        COALESCE(v.new_video_count, 0) AS new_video_count,
        COALESCE(v.active_video_count, 0) AS active_video_count,
        COALESCE(v.new_video_view_count, 0) AS new_video_view_count,
        COALESCE(v.new_video_contributed_view_count, 0)
            AS new_video_contributed_view_count,
        CAST(CASE
            WHEN COALESCE(v.active_video_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(v.new_video_view_count, 0) AS NUMERIC)
                / CAST(v.active_video_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_video_view_count,
        CAST(CASE
            WHEN COALESCE(v.new_video_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(v.new_video_contributed_view_count, 0) AS NUMERIC)
                / CAST(v.new_video_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_new_video_view_count,
        COALESCE(dl.new_download_count, 0) AS new_download_count,
        CAST(CASE
            WHEN COALESCE(v.new_video_view_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(dl.new_download_count, 0) AS NUMERIC)
                / CAST(v.new_video_view_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS play_download_conversion_rate,
        CAST(NULL AS NUMERIC) AS avg_video_download_conversion_count,
        COALESCE(dev.new_device_count, 0) AS new_device_count,
        COALESCE(dev.active_device_count, 0) AS active_device_count,
        COALESCE(r.active_registered_user_count, 0) AS active_registered_user_count,
        CAST(CASE
            WHEN COALESCE(ard.active_registered_app_device_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(dur.total_duration_sec, 0) AS NUMERIC)
                / CAST(ard.active_registered_app_device_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_duration_sec,
        CAST(CASE
            WHEN COALESCE(ard.active_registered_app_device_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(c.exposure_item_count, 0) AS NUMERIC)
                / CAST(ard.active_registered_app_device_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_content_exposure_count,
        CAST(CASE
            WHEN COALESCE(ard.active_registered_app_device_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(c.click_item_count, 0) AS NUMERIC)
                / CAST(ard.active_registered_app_device_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_content_click_count,
        CAST(CASE
            WHEN COALESCE(c.exposure_item_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(c.click_item_count, 0) AS NUMERIC)
                / CAST(c.exposure_item_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS content_ctr,
        COALESCE(pd.product_detail_exposure_uv, 0) AS product_detail_exposure_uv,
        COALESCE(pd.product_detail_exposure_pv, 0) AS product_detail_exposure_pv,
        CAST(CASE
            WHEN COALESCE(pd.product_detail_exposure_uv, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(pd.product_detail_exposure_pv, 0) AS NUMERIC)
                / CAST(pd.product_detail_exposure_uv AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_product_detail_exposure_count,
        CAST(CASE
            WHEN COALESCE(r.active_registered_user_count, 0) = 0 THEN 0
            WHEN DATE_ADD(d.dt, INTERVAL 1 DAY) > DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY)
            THEN NULL
            ELSE ROUND(
                CAST(COALESCE(r.retained_1d_user_count, 0) AS NUMERIC)
                / CAST(r.active_registered_user_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS next_day_retention_rate,
        CAST(CASE
            WHEN COALESCE(r.active_registered_user_count, 0) = 0 THEN 0
            WHEN DATE_ADD(d.dt, INTERVAL 7 DAY) > DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY)
            THEN NULL
            ELSE ROUND(
                CAST(COALESCE(r.retained_7d_user_count, 0) AS NUMERIC)
                / CAST(r.active_registered_user_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS day_7_retention_rate,
        CURRENT_TIMESTAMP() AS update_time
    FROM date_list d
    LEFT JOIN video_metrics v USING (dt)
    LEFT JOIN download_metrics dl USING (dt)
    LEFT JOIN device_metrics dev USING (dt)
    LEFT JOIN app_registered_device_metrics ard USING (dt)
    LEFT JOIN app_registered_duration_metrics dur USING (dt)
    LEFT JOIN content_metrics c USING (dt)
    LEFT JOIN product_detail_metrics pd USING (dt)
    LEFT JOIN retention_metrics r USING (dt);
    """

    logging.info("开始处理: ads_daily_investor")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_investor 刷新完成, 处理日期: {dates}")
