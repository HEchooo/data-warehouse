-- 【Social Content】联盟项目数据看板。粒度 dt × video_key。
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
