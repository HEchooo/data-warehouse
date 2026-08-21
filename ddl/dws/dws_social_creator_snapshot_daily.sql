CREATE TABLE `my-project-8584-jetonai.decom.dws_social_creator_snapshot_daily` (
    dt DATE,
    channel STRING,
    creator_id STRING,
    account_handle STRING,
    account_name STRING,
    account_url STRING,
    usage_type STRING,
    is_active BOOL,
    day_end_follower_count INT64,
    day_end_following_count INT64,
    day_end_like_count INT64,
    day_end_video_count INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY channel, creator_id;
