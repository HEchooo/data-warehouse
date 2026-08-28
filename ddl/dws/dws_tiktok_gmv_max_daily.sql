-- TikTok GMV Max 投放事实层，粒度 dt × advertiser_id × store_id × campaign_id
-- 只固化可加的分子分母；ROI 与平均下单成本不落库，由下游按 SUM(分子) / SUM(分母) 重算。
-- 相比 ODS 少的是 API 生成的全零填充行：报表接口按 campaign × 天返回笛卡尔积，
-- campaign 未投放的日子也会给一行 cost=0 / orders=0，这里过滤掉。
CREATE TABLE IF NOT EXISTS `my-project-8584-jetonai.decom.dws_tiktok_gmv_max_daily` (
    dt DATE,
    advertiser_id STRING,
    store_id STRING,
    campaign_id STRING,
    campaign_name STRING,
    -- 可加分子分母
    cost FLOAT64,
    orders INT64,
    -- 商品全渠道归因收入（付费 + 自然 + 达人），不扣退款；
    -- 与普通投放的广告归因收入不是同一个业务指标，禁止相加
    gross_revenue FLOAT64,
    currency STRING,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY advertiser_id, store_id, campaign_id;
