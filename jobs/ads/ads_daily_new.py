from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "ads_daily_new"
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


def get_ads_daily_new_dates_to_process() -> list[date]:
    """
    按 ads_daily_new 自身依赖并集判定待处理日期。

    规则：
    - 目标表不存在或为空时，按设备 / 用户 / 下载三条上游事实的并集全量初始化
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
            UNION DISTINCT
            SELECT dt FROM `{PROJECT_ID}.{DATASET_ID}.dws_appsflyer_download_daily`
        )
        WHERE dt <= CURRENT_DATE('{TORONTO_TZ}')
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    WITH
    -- 读取 ads_daily_new 当前水位。
    target_watermark AS (
        SELECT COALESCE(MAX(update_time), TIMESTAMP('1970-01-01')) AS max_update_time
        FROM `{target_table_id()}`
    ),
    -- 收敛所有会影响 ads_daily_new 的日期。
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

        UNION DISTINCT

        SELECT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_appsflyer_download_daily`
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


def run_ads_daily_new(dates: list[date]) -> None:
    """
    新增报表:
    - device_count: 新增设备数 (is_new_device = TRUE)
    - user_count: 新增注册用户数 (is_new_user = TRUE)
    - new_user_login_rate: 新增注册用户 / 新增设备
    - anonymous_device_rate: 未登录设备数 / 当日总设备数
    - anonymous_old_device_rate: 未登录老设备数 / 当日总设备数
    - avg_duration_sec: 新增设备的平均停留时长
    - avg_content_consume_count: 新增设备的人均内容消费数
    - next_day_retention_rate: 新增设备的次日留存率
    - new_download_count: 新增下载量（来自 dws_appsflyer_download_daily）

    当前版本只消费稳定中层：
    - dws_device_daily
    - dws_user_daily
    - dws_appsflyer_download_daily
    """
    if not dates:
        logging.info("没有新的 ads_daily_new 日期需要处理")
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
        new_user_login_rate,
        anonymous_device_rate,
        anonymous_old_device_rate,
        avg_duration_sec,
        avg_content_consume_count,
        next_day_retention_rate,
        new_download_count,
        update_time
    )
    WITH
    -- 新增设备指标：复用设备日表中的新增设备、人均时长和人均内容消费。
    new_device_metrics AS (
        SELECT
            dt,
            platform,
            COUNT(DISTINCT prop_device_id) AS device_count,
            CAST(SUM(session_duration_sec) / COUNT(DISTINCT prop_device_id) AS NUMERIC) AS avg_duration_sec,
            CAST(SUM(content_consume_count) / COUNT(DISTINCT prop_device_id) AS NUMERIC) AS avg_content_consume_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
        WHERE dt IN ({dates_str})
            AND is_new_device = TRUE
        GROUP BY dt, platform
    ),
    -- 新增注册用户指标：复用用户日表中的新增注册用户数。
    new_user_metrics AS (
        SELECT
            dt,
            platform,
            COUNT(DISTINCT prop_user_id) AS user_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        WHERE dt IN ({dates_str})
            AND is_new_user = TRUE
        GROUP BY dt, platform
    ),
    -- 新增设备次留：以新增设备 cohort 关联次日同平台活跃。
    new_device_retention AS (
        SELECT
            t.dt,
            t.platform,
            COUNT(DISTINCT t.prop_device_id) AS new_count,
            COUNT(DISTINCT n.prop_device_id) AS retained_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily` t
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dws_device_daily` n
            ON t.prop_device_id = n.prop_device_id
            AND n.dt = DATE_ADD(t.dt, INTERVAL 1 DAY)
            AND n.platform = t.platform
        WHERE t.dt IN ({dates_str})
            AND t.is_new_device = TRUE
        GROUP BY t.dt, t.platform
    ),
    -- 全量活跃设备：为未登录占比提供统一分母。
    total_device_metrics AS (
        SELECT
            dt,
            platform,
            COUNT(DISTINCT prop_device_id) AS total_device_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
        WHERE dt IN ({dates_str})
        GROUP BY dt, platform
    ),
    -- 未登录设备指标：直接复用 dws_device_daily 下沉的匿名设备标记。
    anonymous_device_metrics AS (
        SELECT
            dt,
            platform,
            COUNT(DISTINCT IF(has_anonymous_event, prop_device_id, NULL))
                AS anonymous_device_count,
            COUNT(DISTINCT IF(has_anonymous_event AND is_new_device, prop_device_id, NULL))
                AS anonymous_new_device_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_device_daily`
        WHERE dt IN ({dates_str})
        GROUP BY dt, platform
    ),
    -- 下载指标：复用 AppsFlyer 下载日事实。
    download_metrics AS (
        SELECT
            dt,
            platform,
            CAST(SUM(new_download_count) AS INT64) AS new_download_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_appsflyer_download_daily`
        WHERE dt IN ({dates_str})
        GROUP BY dt, platform
    ),
    -- 指标主键：收敛所有来源的 dt × platform，避免漏行。
    metric_keys AS (
        SELECT dt, platform FROM new_device_metrics
        UNION DISTINCT
        SELECT dt, platform FROM new_user_metrics
        UNION DISTINCT
        SELECT dt, platform FROM new_device_retention
        UNION DISTINCT
        SELECT dt, platform FROM total_device_metrics
        UNION DISTINCT
        SELECT dt, platform FROM anonymous_device_metrics
        UNION DISTINCT
        SELECT dt, platform FROM download_metrics
    )
    SELECT
        k.dt AS dt,
        k.platform AS platform,
        COALESCE(d.device_count, 0) AS device_count,
        COALESCE(u.user_count, 0) AS user_count,
        CAST(
            COALESCE(
                ROUND(
                    SAFE_DIVIDE(COALESCE(u.user_count, 0), COALESCE(d.device_count, 0)),
                    4
                ),
                0
            ) AS NUMERIC
        ) AS new_user_login_rate,
        CAST(
            COALESCE(
                ROUND(
                    SAFE_DIVIDE(
                        COALESCE(a.anonymous_device_count, 0),
                        COALESCE(t.total_device_count, 0)
                    ),
                    4
                ),
                0
            ) AS NUMERIC
        ) AS anonymous_device_rate,
        CAST(
            COALESCE(
                ROUND(
                    SAFE_DIVIDE(
                        COALESCE(a.anonymous_device_count, 0) - COALESCE(a.anonymous_new_device_count, 0),
                        COALESCE(t.total_device_count, 0)
                    ),
                    4
                ),
                0
            ) AS NUMERIC
        ) AS anonymous_old_device_rate,
        COALESCE(d.avg_duration_sec, 0) AS avg_duration_sec,
        COALESCE(d.avg_content_consume_count, 0) AS avg_content_consume_count,
        CAST(CASE
            WHEN COALESCE(r.new_count, 0) = 0 THEN 0
            ELSE ROUND(r.retained_count / r.new_count, 4)
        END AS NUMERIC) AS next_day_retention_rate,
        COALESCE(dl.new_download_count, 0) AS new_download_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM metric_keys k
    LEFT JOIN new_device_metrics d
        ON k.dt = d.dt AND k.platform = d.platform
    LEFT JOIN new_user_metrics u
        ON k.dt = u.dt AND k.platform = u.platform
    LEFT JOIN new_device_retention r
        ON k.dt = r.dt AND k.platform = r.platform
    LEFT JOIN total_device_metrics t
        ON k.dt = t.dt AND k.platform = t.platform
    LEFT JOIN anonymous_device_metrics a
        ON k.dt = a.dt AND k.platform = a.platform
    LEFT JOIN download_metrics dl
        ON k.dt = dl.dt AND k.platform = dl.platform;
    """
    logging.info("开始处理: ads_daily_new")
    client.query(query).result()
    logging.info(f"ads_daily_new 刷新完成, 处理日期: {dates}")


if __name__ == "__main__":
    log_path = configure_logging()
    start_time = datetime.now(timezone.utc)

    dates = get_ads_daily_new_dates_to_process()
    if not dates:
        logging.info("没有新的 ads_daily_new 数据需要处理")
        logging.info(f"日志文件: {log_path}")
        raise SystemExit(0)

    logging.info(f"ads_daily_new 待处理日期: {dates}")
    run_ads_daily_new(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"ads_daily_new 执行完成, 耗时: {elapsed:.1f} 秒")
    logging.info(f"日志文件: {log_path}")
