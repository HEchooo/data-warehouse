CREATE TABLE `my-project-8584-jetonai.decom.ads_weekly_ai_fashion_asset_overview` (
    week_start_dt DATE,
    week_end_dt DATE,
    module STRING,
    new_asset_count INT64,
    exposed_asset_count INT64,
    valid_exposure_asset_count INT64,
    insufficient_sample_asset_count INT64,
    zero_exposure_asset_count INT64,
    exposure_uv INT64,
    tryon_uv INT64,
    negative_feedback_uv INT64,
    negative_feedback_rate FLOAT64,
    update_time TIMESTAMP
)
PARTITION BY week_start_dt
CLUSTER BY module;

ALTER TABLE `my-project-8584-jetonai.decom.ads_weekly_ai_fashion_asset_overview`
DROP COLUMN IF EXISTS week_end_dt;

ALTER TABLE `my-project-8584-jetonai.decom.ads_weekly_ai_fashion_asset_overview`
ADD COLUMN IF NOT EXISTS week_range STRING;
