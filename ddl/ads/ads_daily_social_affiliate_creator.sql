CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_social_affiliate_creator` (
    dt DATE,
    channel STRING,
    creator_id STRING,
    account_name STRING,
    account_handle STRING,
    account_url STRING,
    new_video_count INT64,
    total_video_count INT64,
    view_increment INT64,
    total_view_count INT64,
    like_increment INT64,
    total_like_count INT64,
    comment_increment INT64,
    total_comment_count INT64,
    share_increment INT64,
    total_share_count INT64,
    follower_increment INT64,
    total_follower_count INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY channel, creator_id;
