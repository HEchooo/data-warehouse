CREATE TABLE `my-project-8584-jetonai.decom.dws_launch_product_detail_device_daily` (
    dt DATE,
    video_id STRING,
    channel STRING,
    kol_name STRING,
    country STRING,
    merchant STRING,
    device_id STRING,
    product_detail_pv INT64,
    shopnow_pv INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY video_id, channel;
