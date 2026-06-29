CREATE TABLE `my-project-8584-jetonai.decom.dws_ai_fashion_tryon_okr_user_daily` (
    dt DATE,
    user_id STRING,
    tryon_task_count INT64,
    tryon_success_duration_task_count INT64,
    tryon_success_total_duration_ms INT64,
    product_click_pv_raw INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY user_id;
