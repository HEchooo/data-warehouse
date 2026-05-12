CREATE TABLE `my-project-8584-jetonai.decom.ads_weekly_formal_video_increment_growth` (
    week_index INT64,
    week_start_dt DATE,
    week_end_dt DATE,
    week_label STRING,
    day_count INT64,
    weekly_view_increment INT64,
    weekly_avg_daily_view_increment NUMERIC,
    previous_week_avg_daily_view_increment NUMERIC,
    week_growth_multiple NUMERIC,
    avg_week_growth_multiple NUMERIC,
    monthly_growth_multiple NUMERIC,
    quarterly_growth_multiple NUMERIC,
    update_time TIMESTAMP
)
CLUSTER BY week_start_dt;
