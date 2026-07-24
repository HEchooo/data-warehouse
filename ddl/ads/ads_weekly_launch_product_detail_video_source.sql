CREATE TABLE `my-project-8584-jetonai.decom.ads_weekly_launch_product_detail_video_source` (
    week_start_dt DATE,
    week_end_dt DATE,
    product_detail_pv INT64,
    product_detail_uv INT64,
    has_source_uv INT64,
    no_source_uv INT64,
    update_time TIMESTAMP
)
PARTITION BY week_start_dt;
