CREATE TABLE `my-project-8584-jetonai.decom.dws_tryon_confirm_post_daily` (
    dt DATE,
    post_code STRING,
    click_use_pv INT64,
    click_use_uv INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY post_code;
