CREATE TABLE `my-project-8584-jetonai.decom.dws_column_post_monthly` (
    month DATE,
    module STRING,
    column_id STRING,
    post_total_count INT64,
    update_time TIMESTAMP
)
PARTITION BY month
CLUSTER BY column_id, module;
