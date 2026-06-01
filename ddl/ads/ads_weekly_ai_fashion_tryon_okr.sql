CREATE TABLE `my-project-8584-jetonai.decom.ads_weekly_ai_fashion_tryon_okr` (
    week_start_dt DATE,
    week_end_dt DATE,
    week_label STRING,
    metric_type STRING,
    metric_name STRING,
    metric_bucket_order INT64,
    metric_bucket STRING,
    metric_day_count INT64,
    metric_value NUMERIC,
    update_time TIMESTAMP
)
CLUSTER BY week_start_dt, metric_name;
