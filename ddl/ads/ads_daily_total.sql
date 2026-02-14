CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_total` (
    dt DATE,
    device_count INT64,
    user_count INT64,
    avg_duration_sec NUMERIC,
    avg_content_consume_count NUMERIC,
    next_day_retention_rate NUMERIC,
    update_time TIMESTAMP
)
PARTITION BY dt;
