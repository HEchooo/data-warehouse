CREATE TABLE `my-project-8584-jetonai.decom.dws_product_behavior_daily` (
    dt DATE,
    spu STRING,
    sku STRING,
    exposure_uv INT64,
    tryon_total_count INT64,
    add_cart_uv INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY spu, sku;
