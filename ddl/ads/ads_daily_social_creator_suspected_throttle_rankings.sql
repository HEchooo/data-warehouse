CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_social_creator_suspected_throttle_rankings` (
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
    recent_day_1_t1_avg_view_count NUMERIC,
    recent_day_2_t1_avg_view_count NUMERIC,
    recent_day_3_t1_avg_view_count NUMERIC,
    threshold_ratio NUMERIC,
    throttle_ratio NUMERIC,
    decline_rate NUMERIC,
    severity STRING,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY channel, creator_id;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_creator_suspected_throttle_rankings`
DROP COLUMN IF EXISTS recent_day_1_t1_avg_view_count;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_creator_suspected_throttle_rankings`
DROP COLUMN IF EXISTS recent_day_2_t1_avg_view_count;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_creator_suspected_throttle_rankings`
DROP COLUMN IF EXISTS recent_day_3_t1_avg_view_count;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_creator_suspected_throttle_rankings`
ADD COLUMN IF NOT EXISTS recent_video_1_t1_view_count NUMERIC;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_creator_suspected_throttle_rankings`
ADD COLUMN IF NOT EXISTS recent_video_2_t1_view_count NUMERIC;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_creator_suspected_throttle_rankings`
ADD COLUMN IF NOT EXISTS recent_video_3_t1_view_count NUMERIC;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_creator_suspected_throttle_rankings`
ADD COLUMN IF NOT EXISTS pool_window_start_dt DATE;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_creator_suspected_throttle_rankings`
ADD COLUMN IF NOT EXISTS pool_window_end_dt DATE;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_creator_suspected_throttle_rankings`
ADD COLUMN IF NOT EXISTS pool_published_video_count INT64;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_creator_suspected_throttle_rankings`
ADD COLUMN IF NOT EXISTS weekly_avg_published_video_count NUMERIC;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_creator_suspected_throttle_rankings`
ADD COLUMN IF NOT EXISTS decline_abs NUMERIC;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_creator_suspected_throttle_rankings`
DROP COLUMN IF EXISTS severity;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_creator_suspected_throttle_rankings`
DROP COLUMN IF EXISTS rank_no;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_creator_suspected_throttle_rankings`
DROP COLUMN IF EXISTS channel_rank_no;
