CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_formal_video_increment` (
    dt DATE,
    daily_view_increment INT64,
    daily_like_increment INT64,
    new_video_count INT64,
    ltv_7d_video_count INT64,
    ltv_7d_view_count INT64,
    avg_ltv_7d_views NUMERIC,
    creator_count INT64,
    formal_video_count_to_date INT64,
    update_time TIMESTAMP
)
PARTITION BY dt;
