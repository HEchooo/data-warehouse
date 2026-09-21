-- ECN 外部视频归属。粒度 platform × video_id。
-- 只保存查到的 task_id；404 / 409 不落行，下次继续补缺。
CREATE TABLE IF NOT EXISTS `my-project-8584-jetonai.decom.ods_ecn_video_publication_owner` (
    platform STRING,
    video_id STRING,
    task_id STRING,
    platform_account_name STRING,
    http_status INT64,
    error_code STRING,
    fetched_at TIMESTAMP,
    update_time TIMESTAMP
)
CLUSTER BY platform, video_id;
