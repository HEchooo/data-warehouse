CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_video` (
    dt DATE,
    new_video_count INT64,
    active_video_count INT64,
    new_video_view_count INT64,
    avg_video_view_count NUMERIC,
    update_time TIMESTAMP
)
PARTITION BY
    dt;
