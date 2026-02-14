CREATE TABLE `my-project-8584-jetonai.decom.dim_user_first_active` (
    prop_user_id STRING,
    first_active_date DATE,
    update_time TIMESTAMP
)
CLUSTER BY prop_user_id;
