CREATE TABLE `my-project-8584-jetonai.decom.ods_event_log` (
    logAt INT64,
    event_name STRING,
    logAt_timestamp TIMESTAMP,
    logAt_day STRING,
    session_id STRING,
    prop_device_id STRING,
    prop_user_id STRING,
    prop_os STRING,
    prop_url STRING,
    prop_params STRING,
    prop_app_type STRING,
    prop_ua STRING,
    ext JSON,
    ext_productCode JSON,
    args JSON,
    args_page_key STRING,
    args_title STRING,
    args_href STRING,
    oss_create_at INT64,
    oss_key STRING,
    prop_share_code STRING,
    country STRING,
    oss_key_date DATE,
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
    args_progress STRING
)
PARTITION BY
    oss_key_date
CLUSTER BY
    oss_key,
    logAt_timestamp;

ALTER TABLE `my-project-8584-jetonai.decom.ods_event_log`
ADD COLUMN args_session_duration NUMERIC;