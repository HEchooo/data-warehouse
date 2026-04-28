CREATE TABLE `my-project-8584-jetonai.decom.ads_monthly_module_performance` (
    month DATE,
    module STRING,
    column_exposure_uv INT64,
    follow_total_count INT64,
    follow_rate NUMERIC,
    product_click_uv INT64,
    product_click_pv INT64,
    product_click_penetration_rate NUMERIC,
    product_card_exposure_uv INT64,
    product_exposure_click_rate NUMERIC,
    read_post_count INT64,
    avg_read_post_count_per_user NUMERIC,
    post_total_count INT64,
    tryon_post_count INT64,
    tryon_click_uv INT64,
    tryon_uv_rate NUMERIC,
    ai_tryon_penetration_rate NUMERIC,
    like_post_count INT64,
    like_rate NUMERIC,
    new_follow_count INT64,
    update_time TIMESTAMP
)
PARTITION BY month
CLUSTER BY module;

ALTER TABLE `my-project-8584-jetonai.decom.ads_monthly_module_performance`
ADD COLUMN IF NOT EXISTS like_follow_rate NUMERIC;
