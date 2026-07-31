CREATE TABLE `my-project-8584-jetonai.decom.dws_makepod_order_weekly` (
    week_start_dt DATE,
    channel STRING,
    pay_user_count INT64,
    pay_order_count INT64,
    pay_gmv_usd NUMERIC,
    update_time TIMESTAMP
)
PARTITION BY week_start_dt
CLUSTER BY channel;

ALTER TABLE `my-project-8584-jetonai.decom.dws_makepod_order_weekly`
ADD COLUMN order_group STRING;
