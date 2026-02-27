CREATE TABLE `my-project-8584-jetonai.decom.dws_download_daily` (
    dt DATE,
    platform STRING,
    new_download_count INT64,
    update_time TIMESTAMP
)
PARTITION BY
    dt
CLUSTER BY
    platform;
