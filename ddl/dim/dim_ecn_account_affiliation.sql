-- ECN 账号联盟归属维表。粒度 platform × account_handle。
-- 只保留首次成功绑定；封号后上游删除时 ODS 会变空，本表不删不改。
CREATE TABLE IF NOT EXISTS `my-project-8584-jetonai.decom.dim_ecn_account_affiliation` (
    platform STRING,
    account_handle STRING,
    affiliation STRING,
    affiliation_label STRING,
    first_bound_at TIMESTAMP,
    update_time TIMESTAMP
)
CLUSTER BY platform, account_handle;

-- 2026-09-21 当前橱窗状态。地区永不覆盖；状态随 ODS 最近一次 200 更新。
ALTER TABLE `my-project-8584-jetonai.decom.dim_ecn_account_affiliation`
    ADD COLUMN IF NOT EXISTS tiktok_showcase_status STRING,
    ADD COLUMN IF NOT EXISTS tiktok_showcase_status_updated_at TIMESTAMP;

-- 2026-09-22 最近一次成功响应中的 ECN AI 博主名称。
ALTER TABLE `my-project-8584-jetonai.decom.dim_ecn_account_affiliation`
    ADD COLUMN IF NOT EXISTS ai_kol_name STRING;
