-- ECN 账号联盟归属快照。粒度 platform × account_handle。
-- 404 / 409 是业务结果，保留一行空 affiliation_label，不当任务失败。
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
