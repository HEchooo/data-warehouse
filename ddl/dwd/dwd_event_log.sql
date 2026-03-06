CREATE TABLE `my-project-8584-jetonai.decom.dwd_event_log` (
    hash_id STRING,
    event_name STRING,
    logAt_timestamp TIMESTAMP,
    session_id STRING,
    prop_device_id STRING,
    prop_user_id STRING,
    prop_os STRING,
    prop_url STRING,
    prop_params STRING,
    prop_app_type STRING,
    prop_ua STRING,
    ext JSON,
    product_code STRING,
    args JSON,
    args_page_key STRING,
    args_title STRING,
    args_href STRING,
    oss_create_at INT64,
    oss_key STRING,
    tenant_code STRING,
    prop_share_code STRING,
    invite_user_id STRING,
    country STRING,
    update_time TIMESTAMP,
    args_from STRING,
    args_module STRING,
    args_spu STRING,
    prop_version_type STRING,
    args_star STRING,
    args_magazine STRING,
    args_brand STRING,
    args_post STRING,
    args_topic STRING,
    ext_recommend STRING,
    args_sku STRING,
    args_blogger STRING,
    args_progress STRING,
    post_code STRING,
    ext_productCode STRING
)
PARTITION BY
    DATE(logAt_timestamp)
CLUSTER BY
    tenant_code,
    prop_device_id,
    prop_user_id,
    hash_id;

ALTER TABLE `my-project-8584-jetonai.decom.dwd_event_log`
ADD COLUMN args_session_duration NUMERIC;

ALTER TABLE `my-project-8584-jetonai.decom.dwd_event_log`
ADD COLUMN prop_timezone STRING;

ALTER TABLE `my-project-8584-jetonai.decom.dwd_event_log`
ADD COLUMN raw_event_id STRING;
