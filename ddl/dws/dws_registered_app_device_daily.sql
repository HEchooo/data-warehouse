CREATE TABLE `my-project-8584-jetonai.decom.dws_registered_app_device_daily` (
    dt DATE,
    prop_device_id STRING,
    first_app_active_dt DATE,
    is_new_app_device BOOL,
    has_registered_user_activity BOOL,
    registered_duration_sec FLOAT64,
    registered_content_exposure_count INT64,
    registered_content_click_count INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY prop_device_id;
