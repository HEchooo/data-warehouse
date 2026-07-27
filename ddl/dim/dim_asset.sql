CREATE TABLE `my-project-8584-jetonai.decom.dim_asset` (
    asset_code STRING,
    asset_name STRING,
    creator STRING,
    asset_gmt_create TIMESTAMP,
    post_code STRING,
    publish_time TIMESTAMP,
    publish_dt DATE,
    post_type INT64,
    post_status INT64,
    module STRING,
    column_id STRING,
    column_name STRING,
    update_time TIMESTAMP
) CLUSTER BY asset_code, module, column_id;
