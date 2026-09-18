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
