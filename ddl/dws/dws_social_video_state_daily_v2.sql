CREATE TABLE `my-project-8584-jetonai.decom.dws_social_video_state_daily_v2` (
    dt DATE,
    channel STRING,
    creator_id STRING,
    video_id STRING,
    video_key STRING,
    published_ts_utc TIMESTAMP,
    published_dt DATE,
    day_end_views INT64,
    day_end_likes INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY channel, creator_id, video_key;
