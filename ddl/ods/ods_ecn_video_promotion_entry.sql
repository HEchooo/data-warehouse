-- ECN 视频首次投流发送时间快照。粒度 platform × video_id。
-- 尚未投流或查不到时 ads_first_sent_at 为空，保留一行。
CREATE TABLE IF NOT EXISTS `my-project-8584-jetonai.decom.ods_ecn_video_promotion_entry` (
    platform STRING,
    video_id STRING,
    ads_first_sent_at TIMESTAMP,
    http_status INT64,
    error_code STRING,
    fetched_at TIMESTAMP,
    update_time TIMESTAMP
)
CLUSTER BY platform, video_id;
