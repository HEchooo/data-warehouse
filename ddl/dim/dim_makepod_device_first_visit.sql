CREATE TABLE `my-project-8584-jetonai.decom.dim_makepod_device_first_visit` (
    prop_device_id STRING,
    first_visit_date DATE,
    update_time TIMESTAMP
)
CLUSTER BY prop_device_id;
