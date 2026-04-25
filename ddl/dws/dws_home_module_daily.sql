CREATE TABLE `my-project-8584-jetonai.decom.dws_home_module_daily` (
    dt DATE,
    module STRING,
    module_exposure_uv INT64,
    module_click_uv INT64,
    click_pv INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY module;
