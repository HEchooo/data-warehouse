CREATE TABLE `my-project-8584-jetonai.decom.dim_makepod_user_channel` (
    service_user_id STRING,
    invite_user_id STRING,
    channel STRING,
    register_date DATE,
    update_time TIMESTAMP
)
CLUSTER BY service_user_id;
