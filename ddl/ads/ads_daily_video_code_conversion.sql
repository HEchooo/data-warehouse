CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_video_code_conversion` (
    dt DATE,
    stat_scope STRING,
    task_id STRING,
    promotion_code STRING,
    play_view_increment INT64,
    search_user_count INT64,
    first_active_search_user_count INT64,
    play_to_search_conversion_rate NUMERIC,
    play_to_download_conversion_rate NUMERIC,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY stat_scope, task_id;
