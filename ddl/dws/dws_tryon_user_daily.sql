CREATE TABLE `my-project-8584-jetonai.decom.dws_tryon_user_daily` (
    dt DATE,
    visitor_id STRING,
    tryon_event_count INT64,
    add_cart_event_count INT64,
    has_tryon INT64,
    has_add_cart INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY visitor_id;
