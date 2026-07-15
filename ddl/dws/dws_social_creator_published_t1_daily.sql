CREATE TABLE `my-project-8584-jetonai.decom.dws_social_creator_published_t1_daily` (
    published_dt DATE,
    t1_dt DATE,
    channel STRING,
    creator_id STRING,
    account_name STRING,
    account_handle STRING,
    account_url STRING,
    published_video_count INT64,
    observed_t1_video_count INT64,
    observed_t1_view_count INT64,
    t1_avg_view_count NUMERIC,
    is_t1_snapshot_complete BOOL,
    update_time TIMESTAMP
)
PARTITION BY published_dt
CLUSTER BY channel, creator_id;
