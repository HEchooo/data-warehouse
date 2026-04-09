CREATE TABLE `my-project-8584-jetonai.decom.dws_column_post_daily` (
    dt DATE,
    module STRING,
    column_id STRING,
    post_total_count INT64,
    update_time TIMESTAMP
)
PARTITION BY
    dt;
