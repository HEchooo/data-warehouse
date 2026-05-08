CREATE TABLE `my-project-8584-jetonai.decom.dws_push_daily` (
    dt DATE,
    push_send_count INT64,
    read_count INT64,
    update_time TIMESTAMP
)
PARTITION BY dt;
