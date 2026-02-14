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


def get_dates_to_process():
    """
    找出 DWD 层有新数据但 DWS 层尚未处理的日期。
    逻辑: dwd_event_log.update_time > dws_device_daily 最大 update_time 的所有日期。
    首次运行时 dws_device_daily 为空，处理 dwd_event_log 全部日期。
    使用多伦多时间（America/Toronto）。
    """
    query = f"""
    SELECT DISTINCT DATE(logAt_timestamp, 'America/Toronto') AS dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
    WHERE update_time > (
        SELECT COALESCE(MAX(update_time), TIMESTAMP('1970-01-01'))
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
    )
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

    query = f"""
    MERGE `{PROJECT_ID}.{DATASET_ID}.dim_device_first_active` AS dim
    USING (
        SELECT
            prop_device_id,
            MIN(DATE(logAt_timestamp, 'America/Toronto')) AS first_active_date
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE(logAt_timestamp, 'America/Toronto') IN ({dates_str})
            AND prop_device_id IS NOT NULL AND prop_device_id != ''
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

    query = f"""
    MERGE `{PROJECT_ID}.{DATASET_ID}.dim_user_first_active` AS dim
    USING (
        SELECT
            prop_user_id,
            MIN(DATE(logAt_timestamp, 'America/Toronto')) AS first_active_date
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE(logAt_timestamp, 'America/Toronto') IN ({dates_str})
            AND prop_user_id IS NOT NULL AND prop_user_id != ''
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

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
    (dt, prop_device_id, first_active_date, is_new_device,
     content_consume_count, session_duration_sec, update_time)
    WITH
    -- 每天每个设备每个 session 的持续时间
    session_duration AS (
        SELECT
            DATE(logAt_timestamp, 'America/Toronto') AS dt,
            prop_device_id,
            session_id,
            TIMESTAMP_DIFF(MAX(logAt_timestamp), MIN(logAt_timestamp), SECOND) AS duration_sec
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE(logAt_timestamp, 'America/Toronto') IN ({dates_str})
            AND prop_device_id IS NOT NULL AND prop_device_id != ''
            AND session_id IS NOT NULL AND session_id != ''
        GROUP BY DATE(logAt_timestamp, 'America/Toronto'), prop_device_id, session_id
    ),
    -- 每天每个设备的内容消费事件计数
    device_daily_agg AS (
        SELECT
            DATE(e.logAt_timestamp, 'America/Toronto') AS dt,
            e.prop_device_id,
            COUNTIF(e.event_name IN ({CONTENT_EVENTS})) AS content_consume_count
            -- dwd 中相同 session 的 list 拆分的 code 是去重的
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log` e
        WHERE DATE(e.logAt_timestamp, 'America/Toronto') IN ({dates_str})
            AND e.prop_device_id IS NOT NULL AND e.prop_device_id != ''
        GROUP BY DATE(e.logAt_timestamp, 'America/Toronto'), e.prop_device_id
    ),
    device_duration_agg AS (
        SELECT dt, prop_device_id, SUM(duration_sec) AS total_duration_sec
        FROM session_duration
        GROUP BY dt, prop_device_id
    )
    SELECT
        a.dt,
        a.prop_device_id,
        f.first_active_date,
        (f.first_active_date = a.dt) AS is_new_device,
        a.content_consume_count,
        COALESCE(d.total_duration_sec, 0) AS session_duration_sec,
        CURRENT_TIMESTAMP() AS update_time
    FROM device_daily_agg a
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_device_first_active` f
        ON a.prop_device_id = f.prop_device_id
    LEFT JOIN device_duration_agg d
        ON a.dt = d.dt AND a.prop_device_id = d.prop_device_id;
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

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
    (dt, prop_user_id, first_active_date, is_new_user,
     content_consume_count, update_time)
    WITH
    user_daily_agg AS (
        SELECT
            DATE(e.logAt_timestamp, 'America/Toronto') AS dt,
            e.prop_user_id,
            COUNTIF(e.event_name IN ({CONTENT_EVENTS})) AS content_consume_count
            -- dwd 中相同 session 的 list 拆分的 code 是去重的
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log` e
        WHERE DATE(e.logAt_timestamp, 'America/Toronto') IN ({dates_str})
            AND e.prop_user_id IS NOT NULL AND e.prop_user_id != ''
        GROUP BY DATE(e.logAt_timestamp, 'America/Toronto'), e.prop_user_id
    )
    SELECT
        a.dt,
        a.prop_user_id,
        f.first_active_date,
        (f.first_active_date = a.dt) AS is_new_user,
        a.content_consume_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM user_daily_agg a
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_user_first_active` f
        ON a.prop_user_id = f.prop_user_id;
    """
    logging.info(f"开始处理: dws_user_daily")
    job = client.query(query)
    job.result()
    logging.info(f"dws_user_daily 刷新完成, 处理日期: {dates}")


if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)

    dates = get_dates_to_process()
    if not dates:
        logging.info("没有新数据需要处理")
        exit(0)

    logging.info(f"待处理日期: {dates}")

    # 先增量更新维度表，再计算 DWS
    merge_dim_device_first_active(dates)
    logging.info("-" * 50)

    merge_dim_user_first_active(dates)
    logging.info("-" * 50)

    run_dws_device_daily(dates)
    logging.info("-" * 50)

    run_dws_user_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"DWS ETL 完成, 耗时: {elapsed:.1f} 秒")
