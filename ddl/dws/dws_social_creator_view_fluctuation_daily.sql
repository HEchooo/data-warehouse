CREATE TABLE `my-project-8584-jetonai.decom.dws_social_creator_view_fluctuation_daily` (
    dt DATE,
    pool_window_start_dt DATE,
    pool_window_end_dt DATE,
    channel STRING,
    creator_id STRING,
    account_name STRING,
    account_handle STRING,
    account_url STRING,
    baseline_start_dt DATE,
    baseline_end_dt DATE,
    recent_start_dt DATE,
    recent_end_dt DATE,
    baseline_active_day_count INT64,
    baseline_video_count INT64,
    baseline_t1_view_count INT64,
    baseline_t1_avg_view_count NUMERIC,
    recent_video_count INT64,
    recent_t1_view_count INT64,
    recent_t1_avg_view_count NUMERIC,
    pool_published_video_count INT64,
    weekly_avg_published_video_count NUMERIC,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY channel, creator_id;
