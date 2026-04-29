CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_retention_user_duration_bucket` (
    dt DATE,
    user_group STRING,
    duration_bucket_order INT64,
    duration_bucket STRING,
    next_day_retained_user_count INT64,
    day_7_retained_user_count INT64,
    next_day_not_retained_user_count INT64,
    day_7_not_retained_user_count INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY user_group, duration_bucket_order;
