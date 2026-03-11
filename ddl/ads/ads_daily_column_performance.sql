CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_column_performance` (
    dt DATE,
    module STRING,
    column_id STRING,
    column_name STRING,
    column_exposure_uv INT64,
    follow_total_count INT64,
    follow_rate NUMERIC,
    read_post_count INT64,
    avg_read_post_count_per_user NUMERIC,
    update_time TIMESTAMP
)
PARTITION BY
    dt;
