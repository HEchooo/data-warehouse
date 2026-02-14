CREATE TABLE `my-project-8584-jetonai.decom.dws_device_daily` (
    dt DATE,
    prop_device_id STRING,
    platform STRING,
    first_active_date DATE,
    is_new_device BOOL,
    content_consume_count INT64,
    session_duration_sec FLOAT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY prop_device_id;
