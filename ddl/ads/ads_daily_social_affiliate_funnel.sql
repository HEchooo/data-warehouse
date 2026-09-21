-- 【Affiliate】爆款漏斗。粒度 dt × region。
-- 活跃联盟号按当天快照还原封禁日；发布数 / 爆款数只统计当天仍活跃的账号。
CREATE TABLE IF NOT EXISTS `my-project-8584-jetonai.decom.ads_daily_social_affiliate_funnel` (
    dt DATE,
    active_account_cnt INT64,
    published_video_cnt INT64,
    first_run_hit_video_cnt INT64,
    update_time TIMESTAMP
)
PARTITION BY dt;

-- 2026-09-21 按联盟地区拆行，粒度改为 dt × region。
ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_affiliate_funnel`
    ADD COLUMN IF NOT EXISTS region STRING;

-- 2026-09-21 发布日队列：在投视频 / 当天新开投视频。
ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_affiliate_funnel`
    ADD COLUMN IF NOT EXISTS advertised_video_cnt INT64;
ALTER TABLE `my-project-8584-jetonai.decom.ads_daily_social_affiliate_funnel`
    ADD COLUMN IF NOT EXISTS new_ad_video_cnt INT64;
