CREATE TABLE `my-project-8584-jetonai.decom.dws_ai_fashion_product_detail_promotion_device_daily` (
    dt DATE,
    ac_channel STRING,
    ac_creative STRING,
    device_id STRING,
    product_detail_pv INT64,
    third_party_click_pv INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY ac_channel, ac_creative;
