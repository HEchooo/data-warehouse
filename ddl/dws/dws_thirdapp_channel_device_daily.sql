CREATE TABLE `my-project-8584-jetonai.decom.dws_thirdapp_channel_device_daily` (
    dt DATE,
    channel STRING,
    device_id STRING,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY channel, device_id;
