CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_user_tryon_penetration` (
    dt DATE,
    user_group STRING,
    dau_uv INT64,
    tryon_uv INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY user_group;
