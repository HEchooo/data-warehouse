CREATE TABLE `my-project-8584-jetonai.decom.dws_makepod_traffic_daily` (
    dt DATE,
    dau_uv_total INT64,
    dau_new_uv INT64,
    dau_old_uv INT64,
    register_user_count INT64,
    product_detail_view_uv INT64,
    add_cart_uv INT64,
    kol_invited_visitor_uv INT64,
    update_time TIMESTAMP
)
PARTITION BY dt;

ALTER TABLE `my-project-8584-jetonai.decom.dws_makepod_traffic_daily`
ADD COLUMN channel STRING;
