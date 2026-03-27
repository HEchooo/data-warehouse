import logging
from datetime import date

from google.cloud import bigquery

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
client = bigquery.Client(project=PROJECT_ID)


def dates_to_sql_list(dates):
    return ", ".join([f"'{d}'" for d in dates])


def dates_to_array_sql(dates):
    return ", ".join([f"DATE '{d}'" for d in dates])


def run_ads_daily_investor(dates: list[date]):
    """
    投资人主题日报（dt 一行）:
    - 视频侧指标直接复用 dws_video_daily，其中新增视频播放数按当天全量视频播放增量汇总
    - 新增下载直接复用 dws_appsflyer_download_daily
    - 设备/注册用户/时长直接复用 dws_device_daily / dws_user_daily
    - 人均停留时长分母排除 h5 设备，因为 h5 没有 session_duration 埋点
    - 内容展示/点击来自 dws_content_item_device_daily
    - 留存 cohort 为当日活跃注册用户，不是 is_new_user
    """

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    min_date = min(dates)
    max_date = max(dates)

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_investor`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ads_daily_investor`
    (
        dt,
        new_video_count,
        active_video_count,
        new_video_view_count,
        avg_video_view_count,
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
        next_day_retention_rate,
        day_7_retention_rate,
        update_time
    )
    WITH
    date_list AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    video_metrics AS (
        SELECT
            dt,
            COUNT(DISTINCT IF(is_new_video, video_key, NULL)) AS new_video_count,
            COUNT(DISTINCT IF(is_active_video, video_key, NULL)) AS active_video_count,
            COALESCE(SUM(daily_view_increment), 0) AS new_video_view_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_video_daily`
        WHERE dt IN ({dates_str})
        GROUP BY dt
    ),
    download_metrics AS (
        SELECT
            dt,
            COALESCE(SUM(new_download_count), 0) AS new_download_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_appsflyer_download_daily`
        WHERE dt IN ({dates_str})
        GROUP BY dt
    ),
    device_metrics AS (
        SELECT
            dt,
            COUNT(DISTINCT IF(is_new_device, prop_device_id, NULL)) AS new_device_count,
            COUNT(DISTINCT prop_device_id) AS active_device_count,
            COUNT(DISTINCT IF(platform != 'h5', prop_device_id, NULL)) AS active_app_device_count,
            COALESCE(SUM(session_duration_sec), 0) AS total_duration_sec
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
        WHERE dt IN ({dates_str})
        GROUP BY dt
    ),
    content_metrics AS (
        SELECT
            dt,
            COALESCE(SUM(exposure_item_count), 0) AS exposure_item_count,
            COALESCE(SUM(click_item_count), 0) AS click_item_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_content_item_device_daily`
        WHERE dt IN ({dates_str})
        GROUP BY dt
    ),
    active_users AS (
        SELECT
            dt,
            prop_user_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        WHERE dt BETWEEN DATE '{min_date}' AND LEAST(
            DATE_ADD(DATE '{max_date}', INTERVAL 7 DAY),
            CURRENT_DATE('{TORONTO_TZ}')
        )
        GROUP BY dt, prop_user_id
    ),
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
        CAST(CASE
            WHEN COALESCE(v.active_video_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(v.new_video_view_count, 0) AS NUMERIC)
                / CAST(v.active_video_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_video_view_count,
        COALESCE(dl.new_download_count, 0) AS new_download_count,
        CAST(CASE
            WHEN COALESCE(v.new_video_view_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(dl.new_download_count, 0) AS NUMERIC)
                / CAST(v.new_video_view_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS play_download_conversion_rate,
        CAST(CASE
            WHEN COALESCE(v.active_video_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(dl.new_download_count, 0) AS NUMERIC)
                / CAST(v.active_video_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_video_download_conversion_count,
        COALESCE(dev.new_device_count, 0) AS new_device_count,
        COALESCE(dev.active_device_count, 0) AS active_device_count,
        COALESCE(r.active_registered_user_count, 0) AS active_registered_user_count,
        CAST(CASE
            WHEN COALESCE(dev.active_app_device_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(dev.total_duration_sec, 0) AS NUMERIC)
                / CAST(dev.active_app_device_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_duration_sec,
        CAST(CASE
            WHEN COALESCE(dev.active_device_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(c.exposure_item_count, 0) AS NUMERIC)
                / CAST(dev.active_device_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_content_exposure_count,
        CAST(CASE
            WHEN COALESCE(dev.active_device_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(c.click_item_count, 0) AS NUMERIC)
                / CAST(dev.active_device_count AS NUMERIC),
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
    LEFT JOIN content_metrics c USING (dt)
    LEFT JOIN retention_metrics r USING (dt);
    """

    logging.info("开始处理: ads_daily_investor")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_investor 刷新完成, 处理日期: {dates}")
