CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_makepod` (
    dt DATE,
    dau_uv_total INT64,
    dau_new_uv INT64,
    dau_old_uv INT64,
    register_user_count INT64,
    product_detail_view_uv INT64,
    add_cart_uv INT64,
    order_user_count INT64,
    pay_user_count INT64,
    pay_order_count INT64,
    pay_gmv_usd NUMERIC,
    pay_fee_amount NUMERIC,
    pay_sku_quantity INT64,
    kol_invited_visitor_uv INT64,
    update_time TIMESTAMP
)
PARTITION BY dt;
