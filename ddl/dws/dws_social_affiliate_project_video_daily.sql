-- 联盟项目视频日事实。粒度 dt × video_key。
-- 只固化可加分子分母和投流前件数；max / ROI / 美元换算 / 零填充过滤在 ADS。
CREATE TABLE IF NOT EXISTS `my-project-8584-jetonai.decom.dws_social_affiliate_project_video_daily` (
    dt DATE,
    channel STRING,
    creator_id STRING,
    account_name STRING,
    account_handle STRING,
    video_id STRING,
    video_key STRING,
    published_dt DATE,
    video_link STRING,
    region STRING,
    ads_first_sent_at TIMESTAMP,
    has_ad BOOL,
    spend FLOAT64,
    ad_gmv FLOAT64,
    ad_items INT64,
    ad_currency STRING,
    rpa_gmv FLOAT64,
    rpa_items INT64,
    rpa_currency STRING,
    items_before_ads INT64,
    view_increment INT64,
    like_increment INT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY channel, creator_id, video_key;

-- 2026-09-20 首次投放日、累计投放次数，以及 CPM/CPC/CPS/播放点击率/详情页转化率的分子分母。
ALTER TABLE `my-project-8584-jetonai.decom.dws_social_affiliate_project_video_daily`
    ADD COLUMN IF NOT EXISTS first_ad_dt DATE,
    ADD COLUMN IF NOT EXISTS ad_run_cnt INT64,
    ADD COLUMN IF NOT EXISTS impressions INT64,
    ADD COLUMN IF NOT EXISTS clicks INT64,
    ADD COLUMN IF NOT EXISTS ad_purchases FLOAT64,
    ADD COLUMN IF NOT EXISTS video_play_actions INT64,
    ADD COLUMN IF NOT EXISTS onsite_on_web_detail INT64;
