CREATE TABLE `my-project-8584-jetonai.decom.dws_push_recall_daily` (
    dt DATE,
    recall_strategy STRING,
    push_run_id STRING,
    actor_id STRING,
    has_send INT64,
    has_click INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY recall_strategy, push_run_id, actor_id;
