CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_social_content_v2` (
    dt DATE,
    channel STRING,
    daily_view_increment INT64,
    daily_like_increment INT64,
    creator_count_to_prev_day INT64,
    daily_view_increment_per_creator NUMERIC,
    latest_3_sample_creator_count INT64,
    latest_3_sample_video_count INT64,
    latest_3_sample_like_count INT64,
    latest_3_sample_view_count INT64,
    latest_3_avg_like_view_rate NUMERIC,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY channel;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_content_v2`
ADD COLUMN IF NOT EXISTS new_video_count INT64;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_content_v2`
ADD COLUMN IF NOT EXISTS new_video_contributed_view_count INT64;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_content_v2`
ADD COLUMN IF NOT EXISTS avg_new_video_view_count NUMERIC;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_content_v2`
ADD COLUMN IF NOT EXISTS published_7d_cohort_video_count INT64;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_content_v2`
ADD COLUMN IF NOT EXISTS published_7d_cohort_view_count INT64;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_content_v2`
ADD COLUMN IF NOT EXISTS published_7d_cohort_avg_view_count NUMERIC;
