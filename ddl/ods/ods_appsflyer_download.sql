CREATE TABLE `my-project-8584-jetonai.decom.ods_appsflyer_download` (
    dt DATE,
    platform STRING,
    app_id STRING,
    media_source STRING,
    campaign STRING,
    installs INT64,
    report_timezone STRING,
    fetched_at TIMESTAMP,
    update_time TIMESTAMP
)
PARTITION BY
    dt
CLUSTER BY
    platform, app_id, media_source;

ALTER TABLE `my-project-8584-jetonai.decom.ods_appsflyer_download`
ADD COLUMN IF NOT EXISTS raw_row_json STRING;
