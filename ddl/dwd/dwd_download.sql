CREATE TABLE `my-project-8584-jetonai.decom.dwd_download` (
    dt DATE,
    platform STRING,
    country_code STRING,
    new_download_count INT64,
    source STRING,
    update_time TIMESTAMP
)
PARTITION BY
    dt
CLUSTER BY
    platform,
    country_code;
