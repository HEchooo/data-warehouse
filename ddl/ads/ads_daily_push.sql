CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_push` (
    dt DATE,
    push_send_count INT64,
    read_count INT64,
    read_rate NUMERIC,
    update_time TIMESTAMP
)
PARTITION BY dt;
