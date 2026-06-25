CREATE TABLE `my-project-8584-jetonai.decom.dws_thirdapp_channel_device_daily` (
    dt DATE,
    channel STRING,
    device_id STRING,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY channel, device_id;

ALTER TABLE `my-project-8584-jetonai.decom.dws_thirdapp_channel_device_daily`
ADD COLUMN IF NOT EXISTS thirdapp_open_pv INT64;

ALTER TABLE `my-project-8584-jetonai.decom.dws_thirdapp_channel_device_daily`
ADD COLUMN IF NOT EXISTS video_detail_exposure_pv INT64;

ALTER TABLE `my-project-8584-jetonai.decom.dws_thirdapp_channel_device_daily`
ADD COLUMN IF NOT EXISTS product_click_count INT64;
