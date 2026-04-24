CREATE TABLE `my-project-8584-jetonai.decom.dim_post` (
    post_code STRING,
    post_id INT64,
    post_name STRING,
    creator STRING,
    column_id STRING,
    gmt_create TIMESTAMP,
    status INT64,
    update_time TIMESTAMP
) CLUSTER BY post_code, column_id;
