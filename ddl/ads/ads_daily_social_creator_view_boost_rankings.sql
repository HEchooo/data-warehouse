CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_social_creator_view_boost_rankings` (
    dt DATE,
    baseline_start_dt DATE,
    baseline_end_dt DATE,
    recent_start_dt DATE,
    recent_end_dt DATE,
    rank_no INT64,
    channel_rank_no INT64,
    channel STRING,
    creator_id STRING,
    account_name STRING,
    account_handle STRING,
    account_url STRING,
    baseline_active_day_count INT64,
    baseline_video_count INT64,
    baseline_t1_view_count INT64,
    baseline_t1_avg_view_count NUMERIC,
    recent_video_count INT64,
    recent_t1_view_count INT64,
    recent_t1_avg_view_count NUMERIC,
    boost_ratio NUMERIC,
    update_time TIMESTAMP,
    pool_window_start_dt DATE,
    pool_window_end_dt DATE,
    pool_published_video_count INT64,
    weekly_avg_published_video_count NUMERIC
)
PARTITION BY dt
CLUSTER BY channel, creator_id;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_creator_view_boost_rankings`
DROP COLUMN IF EXISTS rank_no;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_creator_view_boost_rankings`
DROP COLUMN IF EXISTS channel_rank_no;
