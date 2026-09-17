-- TikTok 广告投放日报原始层，粒度 dt × advertiser_id × ad_id
-- 数据源：TikTok Business API /report/integrated/get/（AUCTION_AD）+ /ad/get/（店铺归属）
-- dt 为广告主时区（Etc/GMT+5，固定 UTC-5 不含夏令时）自然日，与项目其他表的美东自然日在夏令时期间相差 1 小时
CREATE TABLE IF NOT EXISTS `my-project-8584-jetonai.decom.ods_tiktok_ad_daily` (
    dt DATE,
    advertiser_id STRING,
    campaign_id STRING,
    campaign_name STRING,
    adgroup_id STRING,
    adgroup_name STRING,
    ad_id STRING,
    ad_name STRING,
    -- 投放店铺，来自 /ad/get/ 的 showcase_products[].store_id；非带货广告（如 VIDEO_VIEWS Smart+）为空
    store_id STRING,
    -- 消耗
    spend FLOAT64,
    -- 平均点击成本（目标页面）
    cpc FLOAT64,
    -- 千次展示成本 CPM
    cpm FLOAT64,
    -- 展示量
    impressions INT64,
    -- 点击量（目标页面）
    clicks INT64,
    -- 点击率（目标页面），单位 %
    ctr FLOAT64,
    -- 转化量
    conversion FLOAT64,
    -- 平均转化成本
    cost_per_conversion FLOAT64,
    -- 转化率 CVR，单位 %
    conversion_rate FLOAT64,
    -- 成效数
    result FLOAT64,
    -- 平均成效成本
    cost_per_result FLOAT64,
    -- 成效率，单位 %
    result_rate FLOAT64,
    -- 合格转化（深度转化目标），未配置深度转化目标时 API 返回 "-"，落库为空值
    secondary_goal_result FLOAT64,
    -- 合格转化平均成本
    cost_per_secondary_goal_result FLOAT64,
    -- 合格转化率，单位 %
    secondary_goal_result_rate FLOAT64,
    report_timezone STRING,
    fetched_at TIMESTAMP,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY advertiser_id, store_id, ad_id;

-- 2026-08-27 新增货币单位。金额字段（spend / cpc / cpm / 各类成本）都以广告主账户货币计价，
-- 取自 /advertiser/info/ 的 currency；报表接口本身不返回货币。
ALTER TABLE `my-project-8584-jetonai.decom.ods_tiktok_ad_daily`
    ADD COLUMN IF NOT EXISTS currency STRING;

-- 2026-09-17 联盟项目看板：广告素材 ID、identity，以及 Shop 成交（AUCTION 没有 GMV Max 的 gross_revenue）。
ALTER TABLE `my-project-8584-jetonai.decom.ods_tiktok_ad_daily`
    ADD COLUMN IF NOT EXISTS tiktok_item_id STRING,
    ADD COLUMN IF NOT EXISTS identity_id STRING,
    ADD COLUMN IF NOT EXISTS identity_type STRING,
    ADD COLUMN IF NOT EXISTS identity_display_name STRING,
    ADD COLUMN IF NOT EXISTS identity_username STRING,
    ADD COLUMN IF NOT EXISTS shop_gross_revenue_by_order_submission FLOAT64,
    ADD COLUMN IF NOT EXISTS total_onsite_shopping_value FLOAT64,
    ADD COLUMN IF NOT EXISTS shop_total_items_purchased INT64,
    ADD COLUMN IF NOT EXISTS shop_total_purchase_by_order_submission FLOAT64;
