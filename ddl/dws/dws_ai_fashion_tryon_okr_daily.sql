CREATE TABLE `my-project-8584-jetonai.decom.dws_ai_fashion_tryon_okr_daily` (
    dt DATE,
    dau_uv_total INT64,
    dau_new_uv INT64,
    dau_old_uv INT64,
    tryon_new_uv INT64,
    tryon_old_uv INT64,
    home_tryon_uv INT64,
    home_feeds_uv INT64,
    post_tryon_uv INT64,
    post_scene_exposure_uv INT64,
    product_detail_tryon_uv INT64,
    product_detail_uv INT64,
    styleme_uv INT64,
    tryon_uv_total INT64,
    tryon_pv INT64,
    update_time TIMESTAMP
)
PARTITION BY dt;

ALTER TABLE `my-project-8584-jetonai.decom.dws_ai_fashion_tryon_okr_daily`
ADD COLUMN IF NOT EXISTS product_click_uv INT64;

ALTER TABLE `my-project-8584-jetonai.decom.dws_ai_fashion_tryon_okr_daily`
ADD COLUMN IF NOT EXISTS product_exposure_uv INT64;

ALTER TABLE `my-project-8584-jetonai.decom.dws_ai_fashion_tryon_okr_daily`
ADD COLUMN IF NOT EXISTS home_product_click_uv INT64;

ALTER TABLE `my-project-8584-jetonai.decom.dws_ai_fashion_tryon_okr_daily`
ADD COLUMN IF NOT EXISTS home_product_exposure_uv INT64;

ALTER TABLE `my-project-8584-jetonai.decom.dws_ai_fashion_tryon_okr_daily`
ADD COLUMN IF NOT EXISTS tryon_result_click_uv INT64;

ALTER TABLE `my-project-8584-jetonai.decom.dws_ai_fashion_tryon_okr_daily`
ADD COLUMN IF NOT EXISTS tryon_result_exposure_uv INT64;

ALTER TABLE `my-project-8584-jetonai.decom.dws_ai_fashion_tryon_okr_daily`
ADD COLUMN IF NOT EXISTS shop_click_uv INT64;

ALTER TABLE `my-project-8584-jetonai.decom.dws_ai_fashion_tryon_okr_daily`
ADD COLUMN IF NOT EXISTS shop_exposure_uv INT64;

ALTER TABLE `my-project-8584-jetonai.decom.dws_ai_fashion_tryon_okr_daily`
ADD COLUMN IF NOT EXISTS product_click_pv_raw INT64;

ALTER TABLE `my-project-8584-jetonai.decom.dws_ai_fashion_tryon_okr_daily`
ADD COLUMN IF NOT EXISTS front_tryon_click_uv INT64,
ADD COLUMN IF NOT EXISTS shop_tryon_uv INT64,
ADD COLUMN IF NOT EXISTS closet_tryon_uv INT64,
ADD COLUMN IF NOT EXISTS closet_feeds_uv INT64,
ADD COLUMN IF NOT EXISTS styleme_exposure_uv INT64,
ADD COLUMN IF NOT EXISTS all_tryon_success_duration_task_count INT64,
ADD COLUMN IF NOT EXISTS all_tryon_success_total_duration_ms INT64,
ADD COLUMN IF NOT EXISTS quick_tryon_task_total_count INT64,
ADD COLUMN IF NOT EXISTS quick_tryon_task_success_count INT64,
ADD COLUMN IF NOT EXISTS quick_tryon_success_duration_task_count INT64,
ADD COLUMN IF NOT EXISTS quick_tryon_success_total_duration_ms INT64,
ADD COLUMN IF NOT EXISTS plus_tryon_task_total_count INT64,
ADD COLUMN IF NOT EXISTS plus_tryon_task_success_count INT64,
ADD COLUMN IF NOT EXISTS plus_tryon_success_duration_task_count INT64,
ADD COLUMN IF NOT EXISTS plus_tryon_success_total_duration_ms INT64;
