CREATE TABLE `my-project-8584-jetonai.decom.dws_registered_app_user_daily` (
    dt DATE,
    prop_user_id STRING,
    first_app_active_dt DATE,
    is_new_app_user BOOL,
    is_next_day_observation_started BOOL,
    is_day_7_observation_started BOOL,
    is_next_day_retained BOOL,
    is_day_7_retained BOOL,
    product_detail_exposure_pv INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY prop_user_id;
