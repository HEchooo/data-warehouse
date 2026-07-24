CREATE TABLE `my-project-8584-jetonai.decom.ads_monthly_launch_product_detail_dimension` (
    month DATE,
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
PARTITION BY month
CLUSTER BY video_id, channel;
