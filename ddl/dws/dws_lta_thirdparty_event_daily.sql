CREATE TABLE `my-project-8584-jetonai.decom.dws_lta_thirdparty_event_daily` (
    dt DATE,
    event_name STRING,
    raw_event_id STRING,
    device_id STRING,
    kol_id STRING,
    kol_nickname STRING,
    spu STRING,
    spu_name STRING,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY event_name, kol_id, spu;
