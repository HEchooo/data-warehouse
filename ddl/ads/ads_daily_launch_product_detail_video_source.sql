CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_launch_product_detail_video_source` (
    dt DATE,
    product_detail_pv INT64,
    product_detail_uv INT64,
    has_source_uv INT64,
    no_source_uv INT64,
    update_time TIMESTAMP
)
PARTITION BY dt;
