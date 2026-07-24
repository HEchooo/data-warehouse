CREATE TABLE `my-project-8584-jetonai.decom.ads_weekly_launch_product_detail_dimension` (
    week_start_dt DATE,
    week_end_dt DATE,
    video_id STRING,
    channel STRING,
    kol_name STRING,
    country STRING,
    merchant STRING,
    product_detail_pv INT64,
    product_detail_uv INT64,
    pv_contribution_rate FLOAT64,
    uv_contribution_rate FLOAT64,
    avg_visit_per_uv FLOAT64,
    update_time TIMESTAMP
)
PARTITION BY week_start_dt
CLUSTER BY video_id, channel;
