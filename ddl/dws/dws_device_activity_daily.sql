CREATE TABLE `my-project-8584-jetonai.decom.dws_device_activity_daily` (
    dt DATE,
    user_segment STRING,
    dau_uv INT64,
    total_duration_min NUMERIC,
    app_launch_count INT64,
    retained_1d_uv INT64,
    retained_7d_uv INT64,
    retained_30d_uv INT64,
    mau_uv INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY user_segment;
