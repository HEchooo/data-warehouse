CREATE TABLE `my-project-8584-jetonai.decom.dim_social_video_attribute` (
    video_key STRING,
    channel STRING,
    video_id STRING,
    task_id STRING,
    unified_video_key STRING,
    production_type STRING,
    video_attribute STRING,
    video_brand STRING,
    blogger_type STRING,
    blogger_attribute STRING,
    task_created_at DATETIME,
    task_updated_at DATETIME,
    mapping_updated_at DATETIME,
    update_time TIMESTAMP
)
CLUSTER BY channel, video_attribute, blogger_attribute, video_key;
