CREATE TABLE `my-project-8584-jetonai.decom.dws_column_interaction_daily` (
    dt DATE,
    module STRING,
    column_id STRING,
    column_exposure_uv INT64,
    follow_total_count INT64,
    follow_uv INT64,
    product_click_uv INT64,
    product_click_pv INT64,
    product_card_exposure_uv INT64,
    read_post_count INT64,
    tryon_post_count INT64,
    tryon_click_uv INT64,
    like_post_count INT64,
    like_uv INT64,
    new_follow_count INT64,
    follow_like_uv_7d INT64,
    like_uv_7d INT64,
    dau_uv INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY column_id, module;
