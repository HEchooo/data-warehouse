CREATE TABLE `my-project-8584-jetonai.decom.dim_video` (
    channel STRING,
    creator_id STRING,
    video_id STRING,
    video_key STRING,
    published_ts_utc TIMESTAMP,
    published_dt DATE,
    update_time TIMESTAMP
)
CLUSTER BY channel, creator_id, video_key;

ALTER TABLE `my-project-8584-jetonai.decom.dim_video`
ADD COLUMN IF NOT EXISTS is_formal_video BOOL;

ALTER TABLE `my-project-8584-jetonai.decom.dim_video`
ADD COLUMN IF NOT EXISTS formal_tag_created_ts_utc TIMESTAMP;
