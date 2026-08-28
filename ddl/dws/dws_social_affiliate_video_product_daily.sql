CREATE TABLE `my-project-8584-jetonai.decom.dws_social_affiliate_video_product_daily` (
    dt DATE,
    channel STRING,
    video_id STRING,
    video_key STRING,
    account_handle STRING,
    product_title STRING,
    product_url STRING,
    product_matched_by STRING,
    currency STRING,
    gmv NUMERIC,
    items_sold INT64,
    gmv_usd NUMERIC,
    fetched_at DATETIME,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY channel, video_key;
