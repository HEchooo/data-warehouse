CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_content_performance` (
    dt DATE,
    platform_exposure_uv INT64,
    avg_browse_content_count_per_user NUMERIC,
    like_total_count INT64,
    like_rate NUMERIC,
    follow_total_count INT64,
    read_follow_rate NUMERIC,
    tryon_total_count INT64,
    read_tryon_rate NUMERIC,
    update_time TIMESTAMP
)
PARTITION BY
    dt;

ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_content_performance`
ADD COLUMN IF NOT EXISTS read_rate NUMERIC;
