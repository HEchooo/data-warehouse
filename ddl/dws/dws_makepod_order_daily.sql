CREATE TABLE `my-project-8584-jetonai.decom.dws_makepod_order_daily` (
    dt DATE,
    order_user_count INT64,
    pay_user_count INT64,
    pay_order_count INT64,
    pay_gmv_usd NUMERIC,
    pay_fee_amount NUMERIC,
    pay_sku_quantity INT64,
    update_time TIMESTAMP
)
PARTITION BY dt;

ALTER TABLE `my-project-8584-jetonai.decom.dws_makepod_order_daily`
ADD COLUMN channel STRING;

ALTER TABLE `my-project-8584-jetonai.decom.dws_makepod_order_daily`
ADD COLUMN purchase_cost_usd NUMERIC;

ALTER TABLE `my-project-8584-jetonai.decom.dws_makepod_order_daily`
ADD COLUMN logistics_cost_usd NUMERIC;
