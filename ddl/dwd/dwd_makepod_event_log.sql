CREATE TABLE `my-project-8584-jetonai.decom.dwd_makepod_event_log` (
    raw_event_id STRING,
    event_name STRING,
    logAt_timestamp TIMESTAMP,
    session_id STRING,
    prop_device_id STRING,
    prop_user_id STRING,
    prop_os STRING,
    prop_url STRING,
    prop_params STRING,
    prop_app_type STRING,
    prop_timezone STRING,
    prop_app_version STRING,
    prop_share_code STRING,
    prop_version_type STRING,
    country STRING,
    args JSON,
    args_page_key STRING,
    args_module STRING,
    args_element JSON,
    args_category_id STRING,
    args_spu JSON,
    args_sku JSON,
    product_code STRING,
    sku_code STRING,
    ext JSON,
    oss_key STRING,
    update_time TIMESTAMP
)
PARTITION BY
    DATE(logAt_timestamp)
CLUSTER BY
    oss_key,
    event_name,
    prop_device_id,
    product_code;

ALTER TABLE `my-project-8584-jetonai.decom.dwd_makepod_event_log`
ADD COLUMN invite_user_id STRING;
