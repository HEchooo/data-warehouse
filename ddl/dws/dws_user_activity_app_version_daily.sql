CREATE TABLE `my-project-8584-jetonai.decom.dws_user_activity_app_version_daily` (
    dt DATE,
    user_id STRING,
    app_version STRING,
    first_active_date DATE,
    used_ai_tryon INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY user_id, app_version;
