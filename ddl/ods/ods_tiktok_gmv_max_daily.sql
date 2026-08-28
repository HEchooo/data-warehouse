-- TikTok GMV Max 投放日报原始层，粒度 dt × advertiser_id × store_id × campaign_id
-- 数据源：TikTok Business API /gmv_max/report/get/（campaign 级）+ /gmv_max/store/list/（店铺）
-- GMV Max 没有 adgroup / ad 层，最细的对外层级就是 campaign，商品级 / 素材级需要另开报表请求。
-- dt 为广告主时区（Etc/GMT+5，固定 UTC-5 不含夏令时）自然日，与普通投放表同源。
CREATE TABLE IF NOT EXISTS `my-project-8584-jetonai.decom.ods_tiktok_gmv_max_daily` (
    dt DATE,
    advertiser_id STRING,
    -- GMV Max 店铺，取自 /gmv_max/store/list/；报表请求必须按 store 逐个查，单次只接受一个
    store_id STRING,
    campaign_id STRING,
    campaign_name STRING,
    -- 成本（页面「成本」）
    cost FLOAT64,
    -- SKU 订单数（页面「SKU 订单数」）
    orders INT64,
    -- 平均下单成本（页面「平均下单成本」）= cost / orders
    cost_per_order FLOAT64,
    -- 总收入（页面「总收入」），商品全渠道归因：付费 + 自然 + 达人，不是纯广告归因，且不扣退款
    gross_revenue FLOAT64,
    -- ROI（页面「ROI」）= gross_revenue / cost
    roi FLOAT64,
    -- 货币单位，GMV Max 报表接口直接返回 currency，不需要再查 /advertiser/info/
    currency STRING,
    report_timezone STRING,
    fetched_at TIMESTAMP,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY advertiser_id, store_id, campaign_id;
