CREATE TABLE `my-project-8584-jetonai.decom.dws_closet_daily` (
    dt DATE,
    closet_home_exposure_uv INT64,
    all_event_uv INT64,
    new_closet_user_count INT64,
    new_item_count INT64,
    total_closet_user_count INT64,
    penetration_user_count INT64,
    update_time TIMESTAMP
)
PARTITION BY dt;
