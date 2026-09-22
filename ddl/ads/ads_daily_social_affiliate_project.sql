-- 【Affiliate】投放数据看板。粒度 dt × video_key。
-- 明细行可看 roi / sales_*；汇总 ROI 必须 SUM(广告 GMV) / SUM(spend)。
CREATE TABLE IF NOT EXISTS `my-project-8584-jetonai.decom.ads_daily_social_affiliate_project` (
    dt DATE,
    region STRING,
    creator_id STRING,
    account_name STRING,
    account_handle STRING,
    video_id STRING,
    video_key STRING,
    published_dt DATE,
    video_url STRING,
    video_link STRING,
    spend FLOAT64,
    ad_gmv FLOAT64,
    ad_gmv_usd FLOAT64,
    rpa_gmv FLOAT64,
    rpa_gmv_usd FLOAT64,
    sales_amount_usd FLOAT64,
    ad_items INT64,
    rpa_items INT64,
    sales_items INT64,
    items_before_ads INT64,
    organic_items_after_ads INT64,
    view_increment INT64,
    like_increment INT64,
    roi FLOAT64,
    ad_currency STRING,
    rpa_currency STRING,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY creator_id, video_key;

-- 2026-09-17 透传视频渠道，供看板区分 youtube / tiktok / ins。
ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_affiliate_project`
    ADD COLUMN IF NOT EXISTS channel STRING;

-- 2026-09-20 首次投放日、累计投放次数，以及广告漏斗率。率值只给明细行；汇总必须 SUM/SUM。
ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_affiliate_project`
    ADD COLUMN IF NOT EXISTS first_ad_dt DATE,
    ADD COLUMN IF NOT EXISTS ad_run_cnt INT64,
    ADD COLUMN IF NOT EXISTS impressions INT64,
    ADD COLUMN IF NOT EXISTS clicks INT64,
    ADD COLUMN IF NOT EXISTS ad_purchases FLOAT64,
    ADD COLUMN IF NOT EXISTS video_play_actions INT64,
    ADD COLUMN IF NOT EXISTS onsite_on_web_detail INT64,
    ADD COLUMN IF NOT EXISTS cpm FLOAT64,
    ADD COLUMN IF NOT EXISTS cpc FLOAT64,
    ADD COLUMN IF NOT EXISTS cps FLOAT64,
    ADD COLUMN IF NOT EXISTS play_click_rate FLOAT64,
    ADD COLUMN IF NOT EXISTS detail_purchase_cvr FLOAT64;

-- 2026-09-21 ECN 后台详情链接，未映射到 task_id 的视频保持空值。
ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_affiliate_project`
    ADD COLUMN IF NOT EXISTS ecn_detail_url STRING;

-- 2026-09-22 ECN AI 博主名称。
ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_affiliate_project`
    ADD COLUMN IF NOT EXISTS ai_kol_name STRING;
