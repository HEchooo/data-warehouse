from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
EASTERN_TZ = "America/New_York"
TABLE_NAME = "dws_lta_thirdparty_event_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6
# LTA 看板只看 mycch.me 域名下的三个第三方事件。
LTA_EVENT_NAMES = (
    "'v_third_party_page', "
    "'v_third_party_goods', "
    "'c_third_party_goods'"
)
LTA_HOST = "mycch.me"

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


def host_filter_expr() -> str:
    """仅保留 args.current_url 域名为 mycch.me 的事件。"""
    return f"NET.HOST(JSON_VALUE(args, '$.current_url')) = '{LTA_HOST}'"


def get_lta_thirdparty_event_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    LTA 第三方事件 DWS 只依赖事件表。

    规则：
    - 目标表不存在或为空时，按历史事件日期全量初始化
    - 目标表已存在时，重刷最近 6 小时受影响日期
    - 同时固定补当前日和前一日，保证零曝光日也能被 ADS 及时产出 0 值
    """
    event_ts = event_ts_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT DATE({event_ts}, '{EASTERN_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE event_name IN ({LTA_EVENT_NAMES})
            AND {host_filter_expr()}
            AND NULLIF(raw_event_id, '') IS NOT NULL
            AND DATE({event_ts}, '{EASTERN_TZ}') IS NOT NULL
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    WITH
    -- 最近 6 小时发生变化的 LTA 事件业务日。
    changed_event_dates AS (
        SELECT DISTINCT DATE({event_ts}, '{EASTERN_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND event_name IN ({LTA_EVENT_NAMES})
            AND {host_filter_expr()}
            AND NULLIF(raw_event_id, '') IS NOT NULL
            AND DATE({event_ts}, '{EASTERN_TZ}') IS NOT NULL
    ),
    -- 固定补最近两天，保证 ADS 能及时产出零值行。
    recent_dates AS (
        SELECT DATE_SUB(CURRENT_DATE('{EASTERN_TZ}'), INTERVAL 1 DAY) AS dt
        UNION DISTINCT
        SELECT CURRENT_DATE('{EASTERN_TZ}') AS dt
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt FROM changed_event_dates
        UNION DISTINCT
        SELECT dt FROM recent_dates
    )
    WHERE dt IS NOT NULL
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_lta_thirdparty_event_daily(dates: list[date]) -> None:
    """
    产出 LTA 第三方事件明细中间层。

    粒度：
    - 商品曝光事件按 args.spu / args.spu_name 数组展开到商品级（一行一个商品）
    - 页面曝光与商品点击事件保持事件级（页面无商品，点击取标量 spu）

    说明：
    - DWD 中同一 raw_event_id 会因商品拆分产生多行且 args 完全一致，
      先按 raw_event_id 去重再展开，避免行爆炸
    - 冗余 kol_nickname（关联 v3_decom.kol_rel 并按 user_id 去重），
      供 ADS 直接使用，避免 ADS 读取外部源表
    - pv/uv 由 ADS 层用 COUNT(DISTINCT raw_event_id) / COUNT(DISTINCT device_id) 计算
    """
    if not dates:
        logging.info("没有新的 dws_lta_thirdparty_event_daily 日期需要处理")
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
        event_name,
        raw_event_id,
        device_id,
        kol_id,
        kol_nickname,
        spu,
        spu_name,
        update_time
    )
    WITH
    -- 本批待处理日期清单。
    target_dates AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 红人维度：kol_rel 存在少量重复 user_id，先按 user_id 去重取单一昵称。
    kol_dim AS (
        SELECT
            CAST(user_id AS STRING) AS kol_id,
            MAX(nickname) AS nickname
        FROM `{PROJECT_ID}.v3_decom.kol_rel`
        WHERE user_id IS NOT NULL
        GROUP BY user_id
    ),
    -- 提取原始字段并按 raw_event_id 去重（同一事件的多行 args 完全一致）。
    base_events AS (
        SELECT
            DATE({event_ts}, '{EASTERN_TZ}') AS dt,
            event_name,
            raw_event_id,
            NULLIF(prop_device_id, '') AS device_id,
            REGEXP_EXTRACT(JSON_VALUE(args, '$.current_url'), r'[?&]kolUserId=([^&]+)') AS kol_id,
            args
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE event_name IN ({LTA_EVENT_NAMES})
            AND {host_filter_expr()}
            AND NULLIF(raw_event_id, '') IS NOT NULL
            AND DATE({event_ts}, '{EASTERN_TZ}') IN (
                SELECT dt
                FROM target_dates
            )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY raw_event_id
            ORDER BY logAt_timestamp
        ) = 1
    ),
    -- 页面曝光与商品点击：事件级单行。
    non_feeds_events AS (
        SELECT
            dt,
            event_name,
            raw_event_id,
            device_id,
            kol_id,
            IF(
                event_name = 'c_third_party_goods',
                NULLIF(JSON_VALUE(args, '$.spu'), ''),
                NULL
            ) AS spu,
            CAST(NULL AS STRING) AS spu_name
        FROM base_events
        WHERE event_name IN ('v_third_party_page', 'c_third_party_goods')
    ),
    -- 商品曝光：按 args.spu / args.spu_name 数组同位展开到商品级。
    feeds_events AS (
        SELECT
            b.dt,
            b.event_name,
            b.raw_event_id,
            b.device_id,
            b.kol_id,
            NULLIF(spu_arr, '') AS spu,
            NULLIF(JSON_VALUE_ARRAY(b.args, '$.spu_name')[SAFE_OFFSET(off)], '') AS spu_name
        FROM (
            SELECT *
            FROM base_events
            WHERE event_name = 'v_third_party_goods'
        ) b
        LEFT JOIN UNNEST(JSON_VALUE_ARRAY(b.args, '$.spu')) AS spu_arr WITH OFFSET off
    ),
    exploded_events AS (
        SELECT * FROM non_feeds_events
        UNION ALL
        SELECT * FROM feeds_events
    )
    SELECT
        e.dt,
        e.event_name,
        e.raw_event_id,
        e.device_id,
        e.kol_id,
        k.nickname AS kol_nickname,
        e.spu,
        e.spu_name,
        CURRENT_TIMESTAMP() AS update_time
    FROM exploded_events e
    LEFT JOIN kol_dim k
        ON e.kol_id = k.kol_id;
    """
    logging.info("开始处理: dws_lta_thirdparty_event_daily")
    client.query(query).result()
    logging.info("dws_lta_thirdparty_event_daily 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_lta_thirdparty_event_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 dws_lta_thirdparty_event_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_lta_thirdparty_event_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dws_lta_thirdparty_event_daily 完成, 耗时: %.1f 秒", elapsed)
