CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_social_affiliate_video` (
    dt DATE,
    channel STRING,
    creator_id STRING,
    account_name STRING,
    account_handle STRING,
    account_url STRING,
    video_id STRING,
    video_key STRING,
    video_title STRING,
    video_link STRING,
    published_ts_utc TIMESTAMP,
    published_dt DATE,
    view_increment INT64,
    total_view_count INT64,
    like_increment INT64,
    total_like_count INT64,
    comment_increment INT64,
    total_comment_count INT64,
    share_increment INT64,
    total_share_count INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY channel, creator_id, video_key;
