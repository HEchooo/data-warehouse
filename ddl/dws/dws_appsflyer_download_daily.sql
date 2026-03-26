CREATE TABLE `my-project-8584-jetonai.decom.dws_appsflyer_download_daily` (
    dt DATE,
    new_download_count INT64,
    update_time TIMESTAMP
)
PARTITION BY
    dt;
