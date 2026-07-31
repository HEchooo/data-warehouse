CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_ai_fashion_product_detail_promotion` (
    dt DATE,
    ac_creative STRING,
    product_detail_exposure_uv INT64,
    product_detail_exposure_pv INT64,
    third_party_click_uv INT64,
    third_party_click_pv INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY ac_creative;
