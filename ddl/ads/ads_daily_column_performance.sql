CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_column_performance` (
    dt DATE,
    module STRING,
    column_id STRING,
    column_name STRING,
    column_exposure_uv INT64,
    follow_total_count INT64,
    follow_rate NUMERIC,
    read_post_count INT64,
    avg_read_post_count_per_user NUMERIC,
    update_time TIMESTAMP
)
PARTITION BY
    dt;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_column_performance`
DROP COLUMN IF EXISTS creator;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_column_performance`
ADD COLUMN IF NOT EXISTS post_total_count INT64;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_column_performance`
ADD COLUMN IF NOT EXISTS tryon_post_count INT64;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_column_performance`
ADD COLUMN IF NOT EXISTS tryon_click_uv INT64;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_column_performance`
ADD COLUMN IF NOT EXISTS tryon_uv_rate NUMERIC;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_column_performance`
ADD COLUMN IF NOT EXISTS ai_tryon_penetration_rate NUMERIC;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_column_performance`
ADD COLUMN IF NOT EXISTS like_post_count INT64;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_column_performance`
ADD COLUMN IF NOT EXISTS like_rate NUMERIC;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_column_performance`
ADD COLUMN IF NOT EXISTS new_follow_count INT64;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_column_performance`
DROP COLUMN IF EXISTS follow_conversion_rate;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_column_performance`
ADD COLUMN IF NOT EXISTS follow_conversion_rate_7d NUMERIC;
