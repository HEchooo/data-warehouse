CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_post_performance` (
    dt DATE,
    post_id INT64,
    post_code STRING,
    post_name STRING,
    module STRING,
    column_id STRING,
    column_name STRING,
    post_exposure_uv INT64,
    like_total_count INT64,
    like_rate NUMERIC,
    follow_total_count INT64,
    follow_rate NUMERIC,
    tryon_total_count INT64,
    update_time TIMESTAMP
)
PARTITION BY
    dt;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_post_performance`
ADD COLUMN IF NOT EXISTS creator STRING;
