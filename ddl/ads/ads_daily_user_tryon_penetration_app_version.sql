CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_user_tryon_penetration_app_version` (
    dt DATE,
    user_group STRING,
    app_version STRING,
    dau_uv INT64,
    tryon_uv INT64,
    tryon_penetration_rate FLOAT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY user_group, app_version;
