CREATE TABLE `my-project-8584-jetonai.decom.dws_user_activity_daily` (
    dt DATE,
    user_id STRING,
    first_active_date DATE,
    is_new_user BOOL,
    is_next_day_observation_started BOOL,
    is_day_7_observation_started BOOL,
    is_next_day_retained BOOL,
    is_day_7_retained BOOL,
    duration_sec INT64,
    viewed_product_detail INT64,
    used_search INT64,
    used_ai_tryon INT64,
    created_model INT64,
    followed_account INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY user_id;

ALTER TABLE `my-project-8584-jetonai.decom.dws_user_activity_daily`
ADD COLUMN followed_account_count INT64;
