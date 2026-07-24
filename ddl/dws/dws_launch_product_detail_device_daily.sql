CREATE TABLE `my-project-8584-jetonai.decom.dws_launch_product_detail_device_daily` (
    dt DATE,
    device_id STRING,
    product_detail_pv INT64,
    shopnow_pv INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY device_id;
