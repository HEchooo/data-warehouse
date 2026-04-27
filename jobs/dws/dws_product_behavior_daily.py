from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_product_behavior_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6

PRODUCT_EVENT_NAMES = (
    "v_product_detail",
    "v_home_feeds",
    "c_tryon",
    "c_add_cart",
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


def clean_sku_expr(field_name: str) -> str:
    """
    dwd_event_log 的 args_sku 有时携带多层双引号/转义。
    这里统一去掉首尾双引号与反斜杠，并把 '' / 'null' 归一为 NULL。
    """
    cleaned = f"TRIM(TRIM(CAST({field_name} AS STRING)), r'\\\"')"
    return f"NULLIF(CASE WHEN LOWER({cleaned}) = 'null' THEN NULL ELSE {cleaned} END, '')"


def get_product_behavior_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    商品主题 DWS 只依赖商品相关事件，不复用共享水位。

    规则：
    - 目标表不存在或为空时，按商品相关事件历史业务日初始化
    - 目标表已存在时，只取最近 6 小时相关事件变更映射出的业务日
    """
    event_ts = event_ts_expr()
    product_events_sql = string_values_to_sql_list(PRODUCT_EVENT_NAMES)
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE event_name IN ({product_events_sql})
            AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
    WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
        AND event_name IN ({product_events_sql})
        AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_product_behavior_daily(dates: list[date]) -> None:
    """
    产出商品主题核心事实层。

    粒度：
    - dt × spu × sku

    指标：
    - exposure_uv：商品曝光 UV
    - tryon_total_count：商详页试穿次数
    - add_cart_uv：商品加购 UV
    """
    if not dates:
        logging.info("没有新的 dws_product_behavior_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    event_ts = event_ts_expr()
    sku_sql = clean_sku_expr("args_sku")
    product_events_sql = string_values_to_sql_list(PRODUCT_EVENT_NAMES)

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        spu,
        sku,
        exposure_uv,
        tryon_total_count,
        add_cart_uv,
        update_time
    )
    WITH
    -- 事件基础层：抽取商品主题相关事件，并统一访客、动作主键与 SKU 清洗规则。
    base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            event_name,
            COALESCE(NULLIF(prop_user_id, ''), NULLIF(prop_device_id, '')) AS visitor_id,
            COALESCE(raw_event_id, hash_id) AS action_event_id,
            NULLIF(CAST(product_code AS STRING), '') AS spu,
            {sku_sql} AS sku,
            CAST(args_page_key AS STRING) AS page_key
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND event_name IN ({product_events_sql})
    ),
    -- 商品曝光层：统一沉淀商详页与首页 Feed 的曝光 UV。
    exposure_daily AS (
        SELECT
            dt,
            spu,
            COALESCE(sku, '__NULL__') AS sku_key,
            COUNT(DISTINCT visitor_id) AS exposure_uv
        FROM base
        WHERE event_name IN ('v_product_detail', 'v_home_feeds')
            AND spu IS NOT NULL
            AND visitor_id IS NOT NULL
        GROUP BY dt, spu, sku_key
    ),
    -- 商品试穿层：只保留商详页触发的试穿动作，并按原始动作去重。
    tryon_daily AS (
        SELECT
            dt,
            spu,
            COALESCE(sku, '__NULL__') AS sku_key,
            COUNT(DISTINCT action_event_id) AS tryon_total_count
        FROM base
        WHERE event_name = 'c_tryon'
            AND page_key = 'product_detail'
            AND spu IS NOT NULL
            AND sku IS NOT NULL
            AND action_event_id IS NOT NULL
        GROUP BY dt, spu, sku_key
    ),
    -- 商品加购层：沉淀商品粒度的加购 UV。
    add_cart_daily AS (
        SELECT
            dt,
            spu,
            COALESCE(sku, '__NULL__') AS sku_key,
            COUNT(DISTINCT IF(visitor_id IS NOT NULL, visitor_id, NULL)) AS add_cart_uv
        FROM base
        WHERE event_name = 'c_add_cart'
            AND spu IS NOT NULL
            AND sku IS NOT NULL
        GROUP BY dt, spu, sku_key
    ),
    -- 主题键集合层：合并曝光、试穿、加购三类事实的业务键。
    keys AS (
        SELECT dt, spu, sku_key FROM exposure_daily
        UNION DISTINCT
        SELECT dt, spu, sku_key FROM tryon_daily
        UNION DISTINCT
        SELECT dt, spu, sku_key FROM add_cart_daily
    )
    SELECT
        k.dt,
        k.spu,
        NULLIF(k.sku_key, '__NULL__') AS sku,
        COALESCE(e.exposure_uv, 0) AS exposure_uv,
        COALESCE(t.tryon_total_count, 0) AS tryon_total_count,
        COALESCE(a.add_cart_uv, 0) AS add_cart_uv,
        CURRENT_TIMESTAMP() AS update_time
    FROM keys k
    LEFT JOIN exposure_daily e USING (dt, spu, sku_key)
    LEFT JOIN tryon_daily t USING (dt, spu, sku_key)
    LEFT JOIN add_cart_daily a USING (dt, spu, sku_key);
    """
    logging.info("开始处理: dws_product_behavior_daily")
    job = client.query(query)
    job.result()
    logging.info("dws_product_behavior_daily 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_product_behavior_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 dws_product_behavior_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_product_behavior_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dws_product_behavior_daily 完成, 耗时: %.1f 秒", elapsed)
