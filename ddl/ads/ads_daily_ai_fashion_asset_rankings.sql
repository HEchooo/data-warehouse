CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_ai_fashion_asset_rankings` (
    dt DATE,
    ranking_type STRING,
    rank_no INT64,
    asset_code STRING,
    asset_name STRING,
    module STRING,
    column_id STRING,
    column_name STRING,
    creator STRING,
    publish_time TIMESTAMP,
    online_days INT64,
    exposure_pv INT64,
    exposure_uv INT64,
    tryon_count INT64,
    tryon_uv INT64,
    negative_feedback_pv INT64,
    negative_feedback_uv INT64,
    negative_feedback_rate FLOAT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY ranking_type, asset_code;
