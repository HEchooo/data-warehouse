CREATE TABLE `my-project-8584-jetonai.decom.dws_post_interaction_daily` (
    dt DATE,
    post_code STRING,
    module STRING,
    column_id STRING,
    platform STRING,
    post_exposure_uv INT64,
    read_uv INT64,
    like_total_count INT64,
    like_uv INT64,
    follow_total_count INT64,
    follow_uv INT64,
    tryon_total_count INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY post_code, column_id, platform;
