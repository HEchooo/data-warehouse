CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_social_video_monitor_rankings_v2` (
    dt DATE,
    target_dt DATE,
    ranking_type STRING,
    rank_no INT64,
    channel STRING,
    creator_id STRING,
    video_id STRING,
    video_key STRING,
    video_title STRING,
    video_link STRING,
    account_name STRING,
    account_handle STRING,
    account_url STRING,
    view_increment INT64,
    total_view_count INT64,
    published_ts_utc TIMESTAMP,
    published_dt DATE,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY ranking_type, channel, video_key;
