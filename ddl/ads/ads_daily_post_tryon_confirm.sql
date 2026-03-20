CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_post_tryon_confirm` (
    dt DATE,
    post_id INT64,
    post_code STRING,
    post_name STRING,
    click_use_pv INT64,
    click_use_uv INT64,
    avg_click_use_count_per_user NUMERIC,
    update_time TIMESTAMP
)
PARTITION BY
    dt;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_post_tryon_confirm`
ADD COLUMN IF NOT EXISTS creator STRING;
