CREATE TABLE `my-project-8584-jetonai.decom.dws_module_post_monthly` (
    month DATE,
    module STRING,
    post_total_count INT64,
    update_time TIMESTAMP
)
PARTITION BY month
CLUSTER BY module;
