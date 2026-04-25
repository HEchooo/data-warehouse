CREATE TABLE `my-project-8584-jetonai.decom.dws_platform_content_daily` (
    dt DATE,
    platform STRING,
    platform_exposure_uv INT64,
    content_exposure_uv INT64,
    content_exposure_pv INT64,
    product_detail_exposure_uv INT64,
    product_detail_exposure_pv INT64,
    product_click_uv INT64,
    product_click_pv INT64,
    read_uv INT64,
    like_total_count INT64,
    like_uv INT64,
    follow_total_count INT64,
    follow_column_count INT64,
    follow_uv INT64,
    dau_uv INT64,
    tryon_total_count INT64,
    tryon_uv INT64,
    column_exposure_pv INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY platform;
