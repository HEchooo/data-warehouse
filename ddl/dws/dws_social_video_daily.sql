CREATE TABLE `my-project-8584-jetonai.decom.dws_social_video_daily` (
    dt DATE,
    channel STRING,
    creator_id STRING,
    video_id STRING,
    video_key STRING,
    published_ts_utc TIMESTAMP,
    published_dt DATE,
    day_end_views INT64,
    day_end_likes INT64,
    daily_view_increment INT64,
    daily_like_increment INT64,
    has_snapshot BOOL,
    has_snapshot_to_dt BOOL,
    raw_day_end_views INT64,
    raw_day_end_likes INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY channel, creator_id, video_key;

ALTER TABLE `my-project-8584-jetonai.decom.dws_social_video_daily`
ADD COLUMN IF NOT EXISTS is_new_video BOOL;

ALTER TABLE `my-project-8584-jetonai.decom.dws_social_video_daily`
ADD COLUMN IF NOT EXISTS new_video_contributed_view_count INT64;
