CREATE TABLE `my-project-8584-jetonai.decom.dws_product_detail_daily` (
    dt DATE,
    product_detail_exposure_uv INT64,
    product_detail_exposure_pv INT64,
    update_time TIMESTAMP
)
PARTITION BY dt;
