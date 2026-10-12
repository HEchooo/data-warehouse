-- 固定九账号的逐视频逐日展示表；日期为 America/Toronto。
CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_ever_pretty_video` (
    dt DATE,
    brand STRING,
    blogger_id STRING,
    blogger_name STRING,
    channel STRING,
    creator_id STRING,
    account_handle STRING,
    account_url STRING,
    video_key STRING,
    video_id STRING,
    video_title STRING,
    video_link STRING,
    published_ts_utc TIMESTAMP,
    published_dt DATE,
    published_time_basis STRING,
    total_views INT64,
    total_likes INT64,
    total_comments INT64,
    total_shares INT64,
    view_increment INT64,
    like_increment INT64,
    comment_increment INT64,
    share_increment INT64,
    has_snapshot BOOL,
    state_available BOOL,
    last_snapshot_dt DATE,
    collection_status STRING,
    is_complete_day BOOL,
    dim_updated_at TIMESTAMP,
    state_updated_at TIMESTAMP,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY blogger_name, channel, creator_id, video_key
OPTIONS (description = 'Ever Pretty 8博主9账号，自2026-09-30发布的视频逐日累计与记录差值。首次采集增量未知，沿用日不代表真实零增长。');
