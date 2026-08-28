-- 【Social Content】TikTok GMV Max 投放日报，粒度 dt × advertiser_id × store_id × campaign_id
-- 同时保留可加的分子分母与单行派生值：
-- 明细行直接看派生列，Superset 汇总必须用 SUM(分子) / SUM(分母) 的 SQL metric。
CREATE TABLE IF NOT EXISTS `my-project-8584-jetonai.decom.ads_daily_tiktok_gmv_max` (
    dt DATE,
    advertiser_id STRING,
    store_id STRING,
    campaign_id STRING,
    campaign_name STRING,
    -- 可加分子分母
    cost FLOAT64,
    orders INT64,
    gross_revenue FLOAT64,
    -- 单行派生值，与 TikTok 后台 GMV Max 看板逐列对齐
    cost_per_order FLOAT64,
    roi FLOAT64,
    currency STRING,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY advertiser_id, store_id, campaign_id;
