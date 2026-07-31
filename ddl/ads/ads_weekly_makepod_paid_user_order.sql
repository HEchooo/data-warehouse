CREATE TABLE `my-project-8584-jetonai.decom.ads_weekly_makepod_paid_user_order` (
    week_start_dt DATE,
    week_range STRING,
    channel STRING,
    pay_user_count INT64,
    pay_user_count_wow FLOAT64,
    pay_order_count INT64,
    pay_order_count_wow FLOAT64,
    pay_gmv_usd NUMERIC,
    pay_gmv_usd_wow FLOAT64,
    pay_user_count_share FLOAT64,
    pay_gmv_share FLOAT64,
    update_time TIMESTAMP
)
PARTITION BY week_start_dt
CLUSTER BY channel;

ALTER TABLE `my-project-8584-jetonai.decom.ads_weekly_makepod_paid_user_order`
ADD COLUMN order_group STRING;
