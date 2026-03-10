CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_user_duration_frequency` (
    dt DATE,
    dau_uv INT64,
    total_duration_min NUMERIC,
    avg_duration_min NUMERIC,
    app_launch_count INT64,
    avg_visit_freq NUMERIC,
    next_day_retention_rate NUMERIC,
    day_7_retention_rate NUMERIC,
    day_30_retention_rate NUMERIC,
    mau_uv INT64,
    dau_mau NUMERIC,
    update_time TIMESTAMP
)
PARTITION BY dt;
