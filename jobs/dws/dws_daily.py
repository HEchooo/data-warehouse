from google.cloud import bigquery
from datetime import datetime, timezone
import logging

# 配置日志记录
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# 项目和数据集配置
PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
client = bigquery.Client(project=PROJECT_ID)

# 内容消费事件列表
CONTENT_EVENTS = (
    "'v_product_detail',"
    "'v_star_post_detail',"
    "'v_magazine_post_detail',"
    "'v_brand_post_detail',"
    "'v_kol_post_detail'"
)
TORONTO_TZ = "America/Toronto"


def event_ts_expr():
    """
    将 logAt_timestamp 先按 prop_timezone 解释为真实时间，再转成 UTC TIMESTAMP。
    logAt_timestamp 存的是“本地时间字面值”，不能直接当 UTC 使用。
    """
    return (
        "TIMESTAMP("
        "DATETIME(logAt_timestamp, 'UTC'), "
        "COALESCE(NULLIF(prop_timezone, ''), 'UTC')"
        ")"
    )


def get_dates_to_process():
    """
    找出 DWD 层有新数据但 DWS 层尚未处理的日期。
    逻辑: dwd_event_log.update_time > dws_device_daily 最大 update_time 的所有日期。
    首次运行时 dws_device_daily 为空，处理 dwd_event_log 全部日期。
    使用多伦多时间（America/Toronto）。
    """
    event_ts = event_ts_expr()
    query = f"""
    WITH base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            update_time
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
    )
    SELECT DISTINCT dt
    FROM base
    WHERE update_time > (
        SELECT COALESCE(MAX(update_time), TIMESTAMP('1970-01-01'))
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
    )
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def get_download_dates_to_process():
    """
    找出下载 DWD 层有新数据但 DWS 下载层尚未处理的日期。
    使用多伦多时间（America/Toronto），按 T+1 处理。
    """
    query = f"""
    SELECT DISTINCT dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dwd_download`
    WHERE update_time > (
        SELECT COALESCE(MAX(update_time), TIMESTAMP('1970-01-01'))
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_download_daily`
    )
    AND dt <= DATE_SUB(CURRENT_DATE('America/Toronto'), INTERVAL 1 DAY)
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def merge_dim_device_first_active(dates):
    """
    增量维护 dim_device_first_active 维度表。
    只扫描本批次日期分区，对新设备 INSERT，对已有设备取 LEAST。
    使用多伦多时间（America/Toronto）。
    """
    dates_str = ", ".join([f"'{d}'" for d in dates])

    event_ts = event_ts_expr()
    query = f"""
    MERGE `{PROJECT_ID}.{DATASET_ID}.dim_device_first_active` AS dim
    USING (
        WITH base AS (
            SELECT
                prop_device_id,
                DATE({event_ts}, '{TORONTO_TZ}') AS dt
            FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
            WHERE prop_device_id IS NOT NULL AND prop_device_id != ''
        )
        SELECT
            prop_device_id,
            MIN(dt) AS first_active_date
        FROM base
        WHERE dt IN ({dates_str})
        GROUP BY prop_device_id
    ) AS src
    ON dim.prop_device_id = src.prop_device_id
    WHEN MATCHED AND src.first_active_date < dim.first_active_date THEN
        UPDATE SET
            first_active_date = src.first_active_date,
            update_time = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (prop_device_id, first_active_date, update_time)
        VALUES (src.prop_device_id, src.first_active_date, CURRENT_TIMESTAMP());
    """
    logging.info(f"开始处理: dim_device_first_active")
    job = client.query(query)
    job.result()
    logging.info(f"dim_device_first_active MERGE 完成, 处理日期: {dates}")


def merge_dim_user_first_active(dates):
    """
    增量维护 dim_user_first_active 维度表。
    只扫描本批次日期分区，对新用户 INSERT，对已有用户取 LEAST。
    使用多伦多时间（America/Toronto）。
    """
    dates_str = ", ".join([f"'{d}'" for d in dates])

    event_ts = event_ts_expr()
    query = f"""
    MERGE `{PROJECT_ID}.{DATASET_ID}.dim_user_first_active` AS dim
    USING (
        WITH base AS (
            SELECT
                prop_user_id,
                DATE({event_ts}, '{TORONTO_TZ}') AS dt
            FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
            WHERE prop_user_id IS NOT NULL AND prop_user_id != ''
        )
        SELECT
            prop_user_id,
            MIN(dt) AS first_active_date
        FROM base
        WHERE dt IN ({dates_str})
        GROUP BY prop_user_id
    ) AS src
    ON dim.prop_user_id = src.prop_user_id
    WHEN MATCHED AND src.first_active_date < dim.first_active_date THEN
        UPDATE SET
            first_active_date = src.first_active_date,
            update_time = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (prop_user_id, first_active_date, update_time)
        VALUES (src.prop_user_id, src.first_active_date, CURRENT_TIMESTAMP());
    """
    logging.info(f"开始处理: dim_user_first_active")
    job = client.query(query)
    job.result()
    logging.info(f"dim_user_first_active MERGE 完成, 处理日期: {dates}")


def run_dws_device_daily(dates):
    """
    纯 SQL 计算 dws_device_daily，DELETE + INSERT 保证幂等。
    - first_active_date: 从 dim_device_first_active 维度表获取（不再全表扫描 DWD）
    - session_duration_sec: SUM(每个 session 的 MAX(logAt_timestamp) - MIN(logAt_timestamp))
    - content_consume_count: 5 种内容消费事件计数
    使用多伦多时间（America/Toronto）。
    """
    dates_str = ", ".join([f"'{d}'" for d in dates])

    event_ts = event_ts_expr()
    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
    (dt, prop_device_id, platform, first_active_date, is_new_device,
     content_consume_count, session_duration_sec, update_time)
    WITH
    normalized AS (
        SELECT
            {event_ts} AS event_ts_utc,
            event_name,
            session_id,
            prop_device_id,
            prop_os,
            prop_url,
            raw_event_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
    ),
    base AS (
        SELECT
            event_ts_utc,
            DATE(event_ts_utc, '{TORONTO_TZ}') AS dt,
            event_name,
            session_id,
            prop_device_id,
            prop_os,
            prop_url,
            raw_event_id
        FROM normalized
        WHERE DATE(event_ts_utc, '{TORONTO_TZ}') IN ({dates_str})
    ),
    -- 每天每个设备每个 session 的持续时间，一个 session 整体归属一个端
    session_duration AS (
        SELECT
            dt,
            prop_device_id,
            session_id,
            CASE
                WHEN LOGICAL_OR(prop_url IS NOT NULL AND prop_url != '') THEN 'h5'
                WHEN LOGICAL_OR(LOWER(prop_os) = 'ios') THEN 'iOS'
                WHEN LOGICAL_OR(LOWER(prop_os) IN ('android', 'harmony')) THEN 'Android'
                ELSE 'unknown'
            END AS platform,
            TIMESTAMP_DIFF(MAX(event_ts_utc), MIN(event_ts_utc), SECOND) AS duration_sec
        FROM base
        WHERE prop_device_id IS NOT NULL AND prop_device_id != ''
            AND session_id IS NOT NULL AND session_id != ''
        GROUP BY dt, prop_device_id, session_id
    ),
    active_devices AS (
        SELECT
            dt,
            prop_device_id,
            CASE
                WHEN prop_url IS NOT NULL AND prop_url != '' THEN 'h5'
                WHEN LOWER(prop_os) = 'ios' THEN 'iOS'
                WHEN LOWER(prop_os) IN ('android', 'harmony') THEN 'Android'
                ELSE 'unknown'
            END AS platform
        FROM base
        WHERE prop_device_id IS NOT NULL AND prop_device_id != ''
        GROUP BY dt, prop_device_id, platform
    ),
    content_events AS (
        SELECT
            dt,
            prop_device_id,
            CASE
                WHEN prop_url IS NOT NULL AND prop_url != '' THEN 'h5'
                WHEN LOWER(prop_os) = 'ios' THEN 'iOS'
                WHEN LOWER(prop_os) IN ('android', 'harmony') THEN 'Android'
                ELSE 'unknown'
            END AS platform,
            raw_event_id
        FROM base
        WHERE prop_device_id IS NOT NULL AND prop_device_id != ''
            AND event_name IN ({CONTENT_EVENTS})
            AND raw_event_id IS NOT NULL
        GROUP BY dt, prop_device_id, platform, raw_event_id
    ),
    device_content_agg AS (
        SELECT
            dt,
            prop_device_id,
            platform,
            COUNT(*) AS content_consume_count
        FROM content_events
        GROUP BY dt, prop_device_id, platform
    ),
    device_duration_agg AS (
        SELECT dt, prop_device_id, platform, SUM(duration_sec) AS total_duration_sec
        FROM session_duration
        GROUP BY dt, prop_device_id, platform
    )
    SELECT
        a.dt,
        a.prop_device_id,
        a.platform,
        f.first_active_date,
        (f.first_active_date = a.dt) AS is_new_device,
        COALESCE(c.content_consume_count, 0) AS content_consume_count,
        COALESCE(d.total_duration_sec, 0) AS session_duration_sec,
        CURRENT_TIMESTAMP() AS update_time
    FROM active_devices a
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_device_first_active` f
        ON a.prop_device_id = f.prop_device_id
    LEFT JOIN device_content_agg c
        ON a.dt = c.dt AND a.prop_device_id = c.prop_device_id AND a.platform = c.platform
    LEFT JOIN device_duration_agg d
        ON a.dt = d.dt AND a.prop_device_id = d.prop_device_id AND a.platform = d.platform;
    """
    logging.info(f"开始处理: dws_device_daily")
    job = client.query(query)
    job.result()
    logging.info(f"dws_device_daily 刷新完成, 处理日期: {dates}")


def run_dws_user_daily(dates):
    """
    纯 SQL 计算 dws_user_daily，DELETE + INSERT 保证幂等。
    只包含 prop_user_id 非空的注册用户，不含停留时长。
    - first_active_date: 从 dim_user_first_active 维度表获取（不再全表扫描 DWD）
    使用多伦多时间（America/Toronto）。
    """
    dates_str = ", ".join([f"'{d}'" for d in dates])

    event_ts = event_ts_expr()
    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
    (dt, prop_user_id, platform, first_active_date, is_new_user,
     content_consume_count, update_time)
    WITH
    normalized AS (
        SELECT
            {event_ts} AS event_ts_utc,
            event_name,
            prop_user_id,
            prop_os,
            prop_url,
            raw_event_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
    ),
    base AS (
        SELECT
            DATE(event_ts_utc, '{TORONTO_TZ}') AS dt,
            event_name,
            prop_user_id,
            prop_os,
            prop_url,
            raw_event_id
        FROM normalized
        WHERE DATE(event_ts_utc, '{TORONTO_TZ}') IN ({dates_str})
    ),
    active_users AS (
        SELECT
            dt,
            prop_user_id,
            CASE
                WHEN prop_url IS NOT NULL AND prop_url != '' THEN 'h5'
                WHEN LOWER(prop_os) = 'ios' THEN 'iOS'
                WHEN LOWER(prop_os) IN ('android', 'harmony') THEN 'Android'
                ELSE 'unknown'
            END AS platform
        FROM base
        WHERE prop_user_id IS NOT NULL AND prop_user_id != ''
        GROUP BY dt, prop_user_id, platform
    ),
    content_events AS (
        SELECT
            dt,
            prop_user_id,
            CASE
                WHEN prop_url IS NOT NULL AND prop_url != '' THEN 'h5'
                WHEN LOWER(prop_os) = 'ios' THEN 'iOS'
                WHEN LOWER(prop_os) IN ('android', 'harmony') THEN 'Android'
                ELSE 'unknown'
            END AS platform,
            raw_event_id
        FROM base
        WHERE prop_user_id IS NOT NULL AND prop_user_id != ''
            AND event_name IN ({CONTENT_EVENTS})
            AND raw_event_id IS NOT NULL
        GROUP BY dt, prop_user_id, platform, raw_event_id
    ),
    user_content_agg AS (
        SELECT
            dt,
            prop_user_id,
            platform,
            COUNT(*) AS content_consume_count
        FROM content_events
        GROUP BY dt, prop_user_id, platform
    )
    SELECT
        a.dt,
        a.prop_user_id,
        a.platform,
        f.first_active_date,
        (f.first_active_date = a.dt) AS is_new_user,
        COALESCE(c.content_consume_count, 0) AS content_consume_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM active_users a
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_user_first_active` f
        ON a.prop_user_id = f.prop_user_id
    LEFT JOIN user_content_agg c
        ON a.dt = c.dt AND a.prop_user_id = c.prop_user_id AND a.platform = c.platform;
    """
    logging.info(f"开始处理: dws_user_daily")
    job = client.query(query)
    job.result()
    logging.info(f"dws_user_daily 刷新完成, 处理日期: {dates}")


def run_dws_download_daily(dates):
    """
    纯 SQL 计算 dws_download_daily，DELETE + INSERT 保证幂等。
    指标: 每日每端新增下载量。
    """
    dates_str = ", ".join([f"'{d}'" for d in dates])

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.dws_download_daily`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.dws_download_daily`
    (dt, platform, new_download_count, update_time)
    SELECT
        dt,
        platform,
        CAST(SUM(new_download_count) AS INT64) AS new_download_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM `{PROJECT_ID}.{DATASET_ID}.dwd_download`
    WHERE dt IN ({dates_str})
    GROUP BY dt, platform;
    """
    logging.info("开始处理: dws_download_daily")
    job = client.query(query)
    job.result()
    logging.info(f"dws_download_daily 刷新完成, 处理日期: {dates}")


if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)

    event_dates = get_dates_to_process()
    download_dates = get_download_dates_to_process()

    if not event_dates and not download_dates:
        logging.info("没有新数据需要处理")
        exit(0)

    if event_dates:
        logging.info(f"事件待处理日期: {event_dates}")

        # 先增量更新维度表，再计算事件相关 DWS
        merge_dim_device_first_active(event_dates)
        logging.info("-" * 50)

        merge_dim_user_first_active(event_dates)
        logging.info("-" * 50)

        run_dws_device_daily(event_dates)
        logging.info("-" * 50)

        run_dws_user_daily(event_dates)
        logging.info("-" * 50)

    if download_dates:
        logging.info(f"下载待处理日期: {download_dates}")
        run_dws_download_daily(download_dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"DWS ETL 完成, 耗时: {elapsed:.1f} 秒")
