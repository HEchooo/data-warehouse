CREATE TABLE `my-project-8584-jetonai.decom.dws_social_channel_daily_v2` (
    dt DATE,
    channel STRING,
    daily_view_increment INT64,
    daily_like_increment INT64,
    new_video_count INT64,
    new_video_contributed_view_count INT64,
    published_day0_view_count INT64,
    published_day1_view_count INT64,
    published_day3_view_count INT64,
    published_day7_view_count INT64,
    published_day14_view_count INT64,
    creator_count_to_prev_day INT64,
    latest_3_sample_creator_count INT64,
    latest_3_sample_video_count INT64,
    latest_3_sample_like_count INT64,
    latest_3_sample_view_count INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY channel;

ALTER TABLE `my-project-8584-jetonai.decom.dws_social_channel_daily_v2`
ADD COLUMN IF NOT EXISTS published_7d_cohort_video_count INT64;

ALTER TABLE `my-project-8584-jetonai.decom.dws_social_channel_daily_v2`
ADD COLUMN IF NOT EXISTS published_7d_cohort_view_count INT64;

ALTER TABLE `my-project-8584-jetonai.decom.dws_social_channel_daily_v2`
ADD COLUMN IF NOT EXISTS published_day0_view_count INT64;

ALTER TABLE `my-project-8584-jetonai.decom.dws_social_channel_daily_v2`
ADD COLUMN IF NOT EXISTS published_day1_view_count INT64;

ALTER TABLE `my-project-8584-jetonai.decom.dws_social_channel_daily_v2`
ADD COLUMN IF NOT EXISTS published_day3_view_count INT64;

ALTER TABLE `my-project-8584-jetonai.decom.dws_social_channel_daily_v2`
ADD COLUMN IF NOT EXISTS published_day7_view_count INT64;

ALTER TABLE `my-project-8584-jetonai.decom.dws_social_channel_daily_v2`
ADD COLUMN IF NOT EXISTS published_day14_view_count INT64;
