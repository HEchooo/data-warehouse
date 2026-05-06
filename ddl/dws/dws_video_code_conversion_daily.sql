CREATE TABLE `my-project-8584-jetonai.decom.dws_video_code_conversion_daily` (
    dt DATE,
    task_id STRING,
    promotion_code STRING,
    play_view_increment INT64,
    search_user_count INT64,
    first_active_search_user_count INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY task_id, promotion_code;
