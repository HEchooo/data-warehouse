CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_thirdapp_channel_uv` (
    dt DATE,
    channel STRING,
    exposure_uv INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY channel;
