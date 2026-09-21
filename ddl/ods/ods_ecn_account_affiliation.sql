-- ECN 账号联盟归属。粒度 platform × account_handle。
-- 只保存成功绑定；404 / 409 不落新行，未绑定账号下次继续查。
CREATE TABLE IF NOT EXISTS `my-project-8584-jetonai.decom.ods_ecn_account_affiliation` (
    platform STRING,
    account_handle STRING,
    account_identifier STRING,
    affiliation STRING,
    affiliation_label STRING,
    ai_kol_id STRING,
    binding_source STRING,
    http_status INT64,
    error_code STRING,
    fetched_at TIMESTAMP,
    update_time TIMESTAMP
)
CLUSTER BY platform, account_handle;

-- 2026-09-21 每日重拉橱窗状态，供活跃联盟号还原。地区仍以首次绑定为准，本表只保留最近一次 200。
ALTER TABLE `my-project-8584-jetonai.decom.ods_ecn_account_affiliation`
    ADD COLUMN IF NOT EXISTS tiktok_showcase_status STRING,
    ADD COLUMN IF NOT EXISTS tiktok_showcase_status_label STRING,
    ADD COLUMN IF NOT EXISTS tiktok_showcase_status_reason STRING,
    ADD COLUMN IF NOT EXISTS tiktok_showcase_status_updated_at TIMESTAMP;
