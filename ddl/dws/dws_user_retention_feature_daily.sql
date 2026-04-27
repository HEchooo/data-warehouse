CREATE TABLE `my-project-8584-jetonai.decom.dws_user_retention_feature_daily` (
    dt DATE,
    user_group STRING,
    feature_order INT64,
    feature_name STRING,
    next_day_retained_value INT64,
    day_7_retained_value INT64,
    next_day_not_retained_value INT64,
    day_7_not_retained_value INT64,
    next_day_retained_user_count INT64,
    day_7_retained_user_count INT64,
    next_day_not_retained_user_count INT64,
    day_7_not_retained_user_count INT64,
    next_day_retained_duration_sum_sec INT64,
    day_7_retained_duration_sum_sec INT64,
    next_day_not_retained_duration_sum_sec INT64,
    day_7_not_retained_duration_sum_sec INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY user_group, feature_order;
