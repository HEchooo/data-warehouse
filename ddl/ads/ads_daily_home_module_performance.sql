CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_home_module_performance` (
    dt DATE,
    module STRING,
    module_exposure_uv INT64,
    module_click_uv INT64,
    click_rate NUMERIC,
    click_pv INT64,
    avg_click_count_per_user NUMERIC,
    update_time TIMESTAMP
)
PARTITION BY
    dt;
