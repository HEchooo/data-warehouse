CREATE TABLE `my-project-8584-jetonai.decom.dws_user_daily` (
    dt DATE,
    prop_user_id STRING,
    first_active_date DATE,
    is_new_user BOOL,
    content_consume_count INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY prop_user_id;
