from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_user_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
CONTENT_EVENTS = (
    "'v_product_detail',"
    "'v_star_post_detail',"
    "'v_magazine_post_detail',"
    "'v_brand_post_detail',"
    "'v_kol_post_detail'"
)

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


def event_ts_expr() -> str:
    return (
        "TIMESTAMP("
        "DATETIME(logAt_timestamp, 'UTC'), "
        "COALESCE(NULLIF(prop_timezone, ''), 'UTC')"
        ")"
    )


def get_dws_user_daily_dates_to_process() -> list[date]:
    """
    按 dws_user_daily 自身水位判定待处理日期。

    规则：
    - 目标表不存在或为空时，全量初始化所有注册用户活跃日期
    - 目标表已存在时，只处理源表 update_time 晚于目标表最大 update_time 的日期
    """
    event_ts = event_ts_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE prop_user_id IS NOT NULL
            AND prop_user_id != ''
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
    WHERE prop_user_id IS NOT NULL
        AND prop_user_id != ''
        AND update_time > (
            SELECT COALESCE(MAX(update_time), TIMESTAMP('1970-01-01'))
            FROM `{target_table_id()}`
        )
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_user_daily(dates: list[date]) -> None:
    """
    产出注册用户日事实。

    粒度：
    - dt × prop_user_id × platform

    语义：
    - first_active_date / is_new_user 复用 dim_user_first_active
    - content_consume_count 保持 raw_event_id 去重口径
    """
    if not dates:
        logging.info("没有新的 dws_user_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    event_ts = event_ts_expr()

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        prop_user_id,
        platform,
        first_active_date,
        is_new_user,
        content_consume_count,
        update_time
    )
    WITH
    -- 统一事件时间与用户基础字段。
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
    -- 截取本批日期内的事件明细。
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
    -- 汇总每天活跃的注册用户键。
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
        WHERE prop_user_id IS NOT NULL
            AND prop_user_id != ''
        GROUP BY dt, prop_user_id, platform
    ),
    -- 提取内容消费事件并按 raw_event_id 去重。
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
        WHERE prop_user_id IS NOT NULL
            AND prop_user_id != ''
            AND event_name IN ({CONTENT_EVENTS})
            AND raw_event_id IS NOT NULL
        GROUP BY dt, prop_user_id, platform, raw_event_id
    ),
    -- 汇总用户日内容消费次数。
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
        ON a.dt = c.dt
        AND a.prop_user_id = c.prop_user_id
        AND a.platform = c.platform;
    """
    logging.info("开始处理: dws_user_daily")
    client.query(query).result()
    logging.info(f"dws_user_daily 刷新完成, 处理日期: {dates}")


if __name__ == "__main__":
    log_path = configure_logging()
    start_time = datetime.now(timezone.utc)

    dates = get_dws_user_daily_dates_to_process()
    if not dates:
        logging.info("没有新的 dws_user_daily 数据需要处理")
        logging.info(f"日志文件: {log_path}")
        raise SystemExit(0)

    logging.info(f"dws_user_daily 待处理日期: {dates}")
    run_dws_user_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"dws_user_daily 执行完成, 耗时: {elapsed:.1f} 秒")
    logging.info(f"日志文件: {log_path}")
