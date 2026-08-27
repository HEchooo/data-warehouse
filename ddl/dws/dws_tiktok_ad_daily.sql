-- TikTok 广告投放事实层，粒度 dt × advertiser_id × ad_id
-- 只固化可加的分子分母；CPC / CPM / CTR / CVR / 各类成本与率值都不落库，
-- 由下游按 SUM(分子) / SUM(分母) 重算，避免任意维度汇总时把率值直接相加。
CREATE TABLE IF NOT EXISTS `my-project-8584-jetonai.decom.dws_tiktok_ad_daily` (
    dt DATE,
    advertiser_id STRING,
    campaign_id STRING,
    campaign_name STRING,
    adgroup_id STRING,
    adgroup_name STRING,
    ad_id STRING,
    ad_name STRING,
    -- 投放店铺；非带货广告（如 VIDEO_VIEWS Smart+）没有店铺概念，为空值
    store_id STRING,
    -- 消耗
    spend FLOAT64,
    -- 展示量
    impressions INT64,
    -- 点击量（目标页面）
    clicks INT64,
    -- 转化量
    conversion FLOAT64,
    -- 成效数
    result FLOAT64,
    -- 合格转化（深度转化目标）；广告组未配置深度转化目标时为空值，不按 0 起算
    secondary_goal_result FLOAT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY advertiser_id, store_id, ad_id;

-- 2026-08-27 新增货币单位，透传自 ODS；跨广告主聚合金额前必须先按 currency 分组或换算。
ALTER TABLE `my-project-8584-jetonai.decom.dws_tiktok_ad_daily`
    ADD COLUMN IF NOT EXISTS currency STRING;
