CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_user_home_module_performance` (
    dt DATE,
    user_group STRING,
    dau_uv INT64,
    feeds_exposure_uv INT64,
    feeds_click_uv INT64,
    star_exposure_uv INT64,
    star_click_uv INT64,
    magazine_exposure_uv INT64,
    magazine_click_uv INT64,
    brand_exposure_uv INT64,
    brand_click_uv INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY user_group;
