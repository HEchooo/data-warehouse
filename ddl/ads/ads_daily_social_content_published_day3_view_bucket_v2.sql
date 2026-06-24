CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_social_content_published_day3_view_bucket_v2` (
    dt DATE,
    channel STRING,
    published_day3_view_bucket_order INT64,
    published_day3_view_bucket STRING,
    video_count INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY channel, published_day3_view_bucket_order;
