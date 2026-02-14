CREATE TABLE `my-project-8584-jetonai.decom.dim_device_first_active` (
    prop_device_id STRING,
    first_active_date DATE,
    update_time TIMESTAMP
) CLUSTER BY prop_device_id;
