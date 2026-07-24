CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_launch_product_detail` (
    dt DATE,
    product_detail_pv INT64,
    product_detail_uv INT64,
    shopnow_pv INT64,
    shopnow_uv INT64,
    uv_shopnow_rate FLOAT64,
    pv_shopnow_rate FLOAT64,
    update_time TIMESTAMP
)
PARTITION BY dt;
