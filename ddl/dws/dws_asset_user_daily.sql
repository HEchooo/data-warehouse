CREATE TABLE `my-project-8584-jetonai.decom.dws_asset_user_daily` (
    dt DATE,
    asset_code STRING,
    user_id STRING,
    exposure_pv INT64,
    tryon_count INT64,
    negative_feedback_pv INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY asset_code, user_id;

ALTER TABLE `my-project-8584-jetonai.decom.dws_asset_user_daily`
ADD COLUMN IF NOT EXISTS product_click_pv INT64;
