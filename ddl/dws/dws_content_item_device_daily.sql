CREATE TABLE `my-project-8584-jetonai.decom.dws_content_item_device_daily` (
    dt DATE,
    prop_device_id STRING,
    exposure_item_count INT64,
    click_item_count INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY prop_device_id;
