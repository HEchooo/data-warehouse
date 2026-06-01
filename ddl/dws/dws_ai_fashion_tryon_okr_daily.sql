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
