CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_tryon_add_cart_conversion` (
    dt DATE,
    tryon_uv INT64,
    add_cart_uv INT64,
    add_cart_rate NUMERIC,
    update_time TIMESTAMP
)
PARTITION BY
    dt;
