CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_push_recall` (
    dt DATE,
    recall_strategy STRING,
    send_count INT64,
    click_count INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY recall_strategy;
