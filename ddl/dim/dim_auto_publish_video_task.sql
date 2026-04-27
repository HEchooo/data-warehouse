CREATE TABLE `my-project-8584-jetonai.decom.dim_auto_publish_video_task` (
    video_key STRING,
    channel STRING,
    video_id STRING,
    auto_publish_task_id STRING,
    unified_video_key STRING,
    completed_task_id_count INT64,
    is_mapping_ambiguous BOOL,
    update_time TIMESTAMP
)
CLUSTER BY channel, auto_publish_task_id, video_key;
