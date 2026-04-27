from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "ads_daily_total"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "ads" / TABLE_NAME

client = bigquery.Client(project=PROJECT_ID)


def configure_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{datetime.now().strftime('%Y-%m-%d')}_{TABLE_NAME}.log"

    logger = logging.getLogger()
    logger.handlers.clear()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return log_path


def dates_to_sql_list(dates: list[date]) -> str:
    return ", ".join([f"'{d}'" for d in dates])


def target_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


def target_table_exists() -> bool:
    try:
        client.get_table(target_table_id())
        return True
    except NotFound:
        return False


def get_target_row_count() -> int:
    if not target_table_exists():
        return 0

    query = f"""
    SELECT COUNT(*) AS row_count
    FROM `{target_table_id()}`
    """
    rows = list(client.query(query).result())
    return int(rows[0].row_count) if rows else 0


def get_ads_daily_total_dates_to_process() -> list[date]:
    """
    按 ads_daily_total 自身依赖并集判定待处理日期。

    规则：
    - 目标表不存在或为空时，按设备 / 用户事实并集全量初始化
    - 目标表已存在时，合并所有 update_time 晚于目标表最大 update_time 的上游日期
    - 次留成熟依赖次日设备活跃，因此设备事实变更还要补回前一天
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM (
            SELECT dt FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
            UNION DISTINCT
            SELECT dt FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        )
        WHERE dt <= CURRENT_DATE('{TORONTO_TZ}')
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    WITH
    -- 读取 ads_daily_total 当前水位。
    target_watermark AS (
        SELECT COALESCE(MAX(update_time), TIMESTAMP('1970-01-01')) AS max_update_time
        FROM `{target_table_id()}`
    ),
    -- 收敛所有会影响 ads_daily_total 的日期。
    changed_dates AS (
        SELECT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
        WHERE update_time > (SELECT max_update_time FROM target_watermark)

        UNION DISTINCT

        SELECT DATE_SUB(dt, INTERVAL 1 DAY) AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
        WHERE update_time > (SELECT max_update_time FROM target_watermark)

        UNION DISTINCT

        SELECT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        WHERE update_time > (SELECT max_update_time FROM target_watermark)
    )
    SELECT DISTINCT dt
    FROM changed_dates
    WHERE dt IS NOT NULL
        AND dt <= CURRENT_DATE('{TORONTO_TZ}')
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_ads_daily_total(dates: list[date]) -> None:
    """
    总量报表:
    - device_count: 活跃设备数
    - user_count: 活跃注册用户数
    - avg_duration_sec: 活跃设备的平均停留时长
    - avg_content_consume_count: 活跃设备的人均内容消费数
    - next_day_retention_rate: 活跃设备的次日留存率
    当前版本只消费稳定中层：
    - dws_device_daily
    - dws_user_daily
    """
    if not dates:
        logging.info("没有新的 ads_daily_total 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        platform,
        device_count,
        user_count,
        avg_duration_sec,
        avg_content_consume_count,
        next_day_retention_rate,
        update_time
    )
    WITH
    -- 活跃设备指标：复用设备日表的人均时长和人均内容消费。
    device_metrics AS (
        SELECT
            dt,
            platform,
            COUNT(DISTINCT prop_device_id) AS device_count,
            CAST(SUM(session_duration_sec) / COUNT(DISTINCT prop_device_id) AS NUMERIC) AS avg_duration_sec,
            CAST(SUM(content_consume_count) / COUNT(DISTINCT prop_device_id) AS NUMERIC) AS avg_content_consume_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
        WHERE dt IN ({dates_str})
        GROUP BY dt, platform
    ),
    -- 活跃注册用户指标：复用用户日表的去重用户数。
    user_metrics AS (
        SELECT
            dt,
            platform,
            COUNT(DISTINCT prop_user_id) AS user_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        WHERE dt IN ({dates_str})
        GROUP BY dt, platform
    ),
    -- 活跃设备次留：以当日活跃设备 cohort 关联次日同平台活跃。
    device_retention AS (
        SELECT
            t.dt,
            t.platform,
            COUNT(DISTINCT t.prop_device_id) AS active_count,
            COUNT(DISTINCT n.prop_device_id) AS retained_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily` t
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dws_device_daily` n
            ON t.prop_device_id = n.prop_device_id
            AND n.dt = DATE_ADD(t.dt, INTERVAL 1 DAY)
            AND n.platform = t.platform
        WHERE t.dt IN ({dates_str})
        GROUP BY t.dt, t.platform
    ),
    -- 指标主键：收敛所有来源的 dt × platform，避免漏行。
    metric_keys AS (
        SELECT dt, platform FROM device_metrics
        UNION DISTINCT
        SELECT dt, platform FROM user_metrics
        UNION DISTINCT
        SELECT dt, platform FROM device_retention
    )
    SELECT
        k.dt AS dt,
        k.platform AS platform,
        COALESCE(d.device_count, 0) AS device_count,
        COALESCE(u.user_count, 0) AS user_count,
        COALESCE(d.avg_duration_sec, 0) AS avg_duration_sec,
        COALESCE(d.avg_content_consume_count, 0) AS avg_content_consume_count,
        CAST(CASE
            WHEN COALESCE(r.active_count, 0) = 0 THEN 0
            ELSE ROUND(r.retained_count / r.active_count, 4)
        END AS NUMERIC) AS next_day_retention_rate,
        CURRENT_TIMESTAMP() AS update_time
    FROM metric_keys k
    LEFT JOIN device_metrics d
        ON k.dt = d.dt AND k.platform = d.platform
    LEFT JOIN user_metrics u
        ON k.dt = u.dt AND k.platform = u.platform
    LEFT JOIN device_retention r
        ON k.dt = r.dt AND k.platform = r.platform;
    """
    logging.info("开始处理: ads_daily_total")
    client.query(query).result()
    logging.info(f"ads_daily_total 刷新完成, 处理日期: {dates}")


if __name__ == "__main__":
    log_path = configure_logging()
    start_time = datetime.now(timezone.utc)

    dates = get_ads_daily_total_dates_to_process()
    if not dates:
        logging.info("没有新的 ads_daily_total 数据需要处理")
        logging.info(f"日志文件: {log_path}")
        raise SystemExit(0)

    logging.info(f"ads_daily_total 待处理日期: {dates}")
    run_ads_daily_total(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"ads_daily_total 执行完成, 耗时: {elapsed:.1f} 秒")
    logging.info(f"日志文件: {log_path}")
