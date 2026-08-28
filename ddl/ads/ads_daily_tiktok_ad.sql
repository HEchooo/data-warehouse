-- 【Social Content】Makepod 店铺 TK 广告投放数据日报，粒度 dt × advertiser_id × ad_id
-- 同时保留可加的分子分母与单行派生率值：
-- 明细行直接看派生列，Superset 汇总必须用 SUM(分子) / SUM(分母) 的 SQL metric，不能对派生列求和或求平均。
CREATE TABLE IF NOT EXISTS `my-project-8584-jetonai.decom.ads_daily_tiktok_ad` (
    dt DATE,
    advertiser_id STRING,
    campaign_id STRING,
    campaign_name STRING,
    adgroup_id STRING,
    adgroup_name STRING,
    ad_id STRING,
    ad_name STRING,
    -- 投放店铺；看板固定只看 makepod 店铺 7494736378860701060
    store_id STRING,
    -- 可加分子分母
    spend FLOAT64,
    impressions INT64,
    clicks INT64,
    conversion FLOAT64,
    result FLOAT64,
    secondary_goal_result FLOAT64,
    -- 单行派生率值
    cpc FLOAT64,
    cpm FLOAT64,
    ctr FLOAT64,
    cost_per_conversion FLOAT64,
    conversion_rate FLOAT64,
    cost_per_result FLOAT64,
    result_rate FLOAT64,
    cost_per_secondary_goal_result FLOAT64,
    secondary_goal_result_rate FLOAT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY advertiser_id, store_id, ad_id;

-- 2026-08-27 新增货币单位，透传自 DWS；当前单一广告主恒为 USD。
ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_tiktok_ad`
    ADD COLUMN IF NOT EXISTS currency STRING;
