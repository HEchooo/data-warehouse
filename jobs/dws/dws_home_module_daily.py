from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_home_module_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6

MODULE_MAP_SQL = """
SELECT 'star' AS module, 'v_home_star' AS exposure_event, 'c_home_star' AS click_event
UNION ALL
SELECT 'magazine' AS module, 'v_home_magazine' AS exposure_event, 'c_home_magazine' AS click_event
UNION ALL
SELECT 'brand' AS module, 'v_home_brand' AS exposure_event, 'c_home_brand' AS click_event
UNION ALL
SELECT 'feeds' AS module, 'v_home_feeds' AS exposure_event, 'c_home_feeds' AS click_event
""".strip()
HOME_MODULE_EVENT_NAMES = (
    "v_home_star",
    "c_home_star",
    "v_home_magazine",
    "c_home_magazine",
    "v_home_brand",
    "c_home_brand",
    "v_home_feeds",
    "c_home_feeds",
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


def dates_to_array_sql(dates: list[date]) -> str:
    return ", ".join([f"DATE '{d}'" for d in dates])


def string_values_to_sql_list(values: tuple[str, ...]) -> str:
    return ", ".join([f"'{value}'" for value in values])


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


def get_home_module_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    首页模块 DWS 只依赖事件表，不复用共享水位。

    规则：
    - 目标表不存在或为空时，按事件表中所有业务日全量初始化
    - 目标表已存在时，只取最近 6 小时事件表变更映射出的业务日

    说明：
    - 首页模块报表历史上要求“当天即使没有首页模块事件，也要保留 4 个模块的 0 值行”
    - 因此日期骨架不能只靠 `v_home_* / c_home_*` 事件发现，否则会把老分区里的全零天数丢掉
    """
    event_ts = event_ts_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
    WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
        AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_home_module_daily(dates: list[date]) -> None:
    """
    产出首页模块主题核心事实层。

    粒度：
    - dt × module

    指标：
    - module_exposure_uv
    - module_click_uv
    - click_pv
    """
    if not dates:
        logging.info("没有新的 dws_home_module_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    event_ts = event_ts_expr()

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        module,
        module_exposure_uv,
        module_click_uv,
        click_pv,
        update_time
    )
    WITH
    -- 固定首页模块与曝光/点击事件映射。
    module_map AS (
        {MODULE_MAP_SQL}
    ),
    -- 提取目标日期范围内的首页模块事件，只保留已登录用户。
    base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            event_name,
            prop_user_id,
            raw_event_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND event_name IN (
                SELECT exposure_event AS event FROM module_map
                UNION DISTINCT
                SELECT click_event AS event FROM module_map
            )
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
    ),
    -- 将事件映射到模块，并标记曝光/点击类型。
    events AS (
        SELECT
            b.dt,
            b.prop_user_id AS user_id,
            m.module,
            b.event_name = m.exposure_event AS is_exposure,
            b.event_name = m.click_event AS is_click,
            b.raw_event_id
        FROM base b
        INNER JOIN module_map m
            ON b.event_name IN (m.exposure_event, m.click_event)
    ),
    -- 生成目标日期清单，用于补齐零值日期。
    date_list AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 生成目标日期与固定模块的全量组合。
    metric_keys AS (
        SELECT d.dt, m.module
        FROM date_list d
        CROSS JOIN (SELECT module FROM module_map) m
    ),
    -- 汇总已登录用户口径下的曝光 UV、点击 UV 和点击 PV。
    metric_daily AS (
        SELECT
            dt,
            module,
            COUNT(DISTINCT IF(is_exposure, user_id, NULL)) AS module_exposure_uv,
            COUNT(DISTINCT IF(is_click, user_id, NULL)) AS module_click_uv,
            COUNT(DISTINCT IF(is_click, raw_event_id, NULL)) AS click_pv
        FROM events
        GROUP BY dt, module
    )
    SELECT
        k.dt,
        k.module,
        COALESCE(m.module_exposure_uv, 0) AS module_exposure_uv,
        COALESCE(m.module_click_uv, 0) AS module_click_uv,
        COALESCE(m.click_pv, 0) AS click_pv,
        CURRENT_TIMESTAMP() AS update_time
    FROM metric_keys k
    LEFT JOIN metric_daily m
        ON k.dt = m.dt
        AND k.module = m.module;
    """
    logging.info("开始处理: dws_home_module_daily")
    job = client.query(query)
    job.result()
    logging.info(f"dws_home_module_daily 刷新完成, 处理日期: {dates}")


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info(f"日志文件: {log_path}")

    start_time = datetime.now(timezone.utc)
    dates = get_home_module_dates_to_process()
    logging.info(f"待处理日期: {dates if dates else '[]'}")

    if not dates:
        logging.info("没有新的 dws_home_module_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_home_module_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"dws_home_module_daily 完成, 耗时: {elapsed:.1f} 秒")
