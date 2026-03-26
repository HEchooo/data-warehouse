CREATE TABLE `my-project-8584-jetonai.decom.dws_video_daily` (
    dt DATE,
    channel STRING,
    video_id STRING,
    video_key STRING,
    published_dt DATE,
    raw_day_end_views INT64,
    day_end_views INT64,
    daily_view_increment INT64,
    has_snapshot BOOL,
    is_active_video BOOL,
    is_new_video BOOL,
    update_time TIMESTAMP
)
PARTITION BY
    dt
CLUSTER BY
    channel, video_key;
