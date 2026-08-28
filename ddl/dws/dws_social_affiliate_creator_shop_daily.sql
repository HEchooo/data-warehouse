CREATE TABLE `my-project-8584-jetonai.decom.dws_social_affiliate_creator_shop_daily` (
    dt DATE,
    channel STRING,
    account_handle STRING,
    currency STRING,
    gmv NUMERIC,
    items_sold INT64,
    sku_order_count INT64,
    gmv_usd NUMERIC,
    fetched_at DATETIME,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY channel, account_handle;
