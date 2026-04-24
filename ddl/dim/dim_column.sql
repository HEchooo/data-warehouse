CREATE TABLE `my-project-8584-jetonai.decom.dim_column` (
    column_id STRING,
    module STRING,
    column_name STRING,
    kol_type INT64,
    status INT64,
    update_time TIMESTAMP
) CLUSTER BY column_id;
