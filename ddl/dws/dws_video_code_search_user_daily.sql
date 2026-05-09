CREATE TABLE `my-project-8584-jetonai.decom.dws_video_code_search_user_daily` (
    dt DATE,
    promotion_code STRING,
    user_id STRING,
    is_first_active_today BOOL,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY promotion_code, user_id;
