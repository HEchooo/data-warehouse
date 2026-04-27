CREATE TABLE `my-project-8584-jetonai.decom.dws_tryon_confirm_daily` (
    dt DATE,
    click_use_pv INT64,
    click_use_uv INT64,
    update_time TIMESTAMP
)
PARTITION BY dt;
