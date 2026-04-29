CREATE TABLE `my-project-8584-jetonai.decom.dim_user_closet_first_use` (
    user_id STRING,
    first_single_dt DATE,
    first_any_dt DATE,
    update_time TIMESTAMP
)
CLUSTER BY user_id;
