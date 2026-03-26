CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_investor` (
    dt DATE,
    new_video_count INT64,
    active_video_count INT64,
    new_video_view_count INT64,
    avg_video_view_count NUMERIC,
    new_download_count INT64,
    play_download_conversion_rate NUMERIC,
    avg_video_download_conversion_count NUMERIC,
    new_device_count INT64,
    active_device_count INT64,
    active_registered_user_count INT64,
    avg_duration_sec NUMERIC,
    avg_content_exposure_count NUMERIC,
    avg_content_click_count NUMERIC,
    content_ctr NUMERIC,
    next_day_retention_rate NUMERIC,
    day_7_retention_rate NUMERIC,
    update_time TIMESTAMP
)
PARTITION BY dt;
