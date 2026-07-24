from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
EASTERN_TZ = "America/New_York"
TABLE_NAME = "dws_launch_product_detail_device_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6
LAUNCH_PRODUCT_DETAIL_EVENT_NAMES = (
    "'v_launch_product_detail', "
    "'c_launch_product_detail_shopnow'"
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


def dimension_exprs() -> str:
    """
    5 个多维度看板维度，均来自 args JSON。
    video_id 缺失时归为“无法识别来源”，其余维度缺失归为“-”。
    """
    return """
        COALESCE(NULLIF(JSON_VALUE(args, '$.video_id'), ''), '无法识别来源') AS video_id,
        COALESCE(NULLIF(JSON_VALUE(args, '$.channel'), ''), '-') AS channel,
        COALESCE(NULLIF(JSON_VALUE(args, '$.kol_name'), ''), '-') AS kol_name,
        COALESCE(NULLIF(JSON_VALUE(args, '$.country'), ''), '-') AS country,
        COALESCE(NULLIF(JSON_VALUE(args, '$.merchant'), ''), '-') AS merchant
    """


def get_launch_product_detail_device_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    Launch 商详页主题 DWS 只依赖事件表。

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
        WHERE event_name IN ({LAUNCH_PRODUCT_DETAIL_EVENT_NAMES})
            AND NULLIF(prop_device_id, '') IS NOT NULL
            AND DATE({event_ts}, '{EASTERN_TZ}') IS NOT NULL
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    WITH
    -- 最近 6 小时发生变化的 Launch 商详页主题业务日。
    changed_event_dates AS (
        SELECT DISTINCT DATE({event_ts}, '{EASTERN_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND event_name IN ({LAUNCH_PRODUCT_DETAIL_EVENT_NAMES})
            AND NULLIF(prop_device_id, '') IS NOT NULL
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


def run_dws_launch_product_detail_device_daily(dates: list[date]) -> None:
    """
    产出 Launch 商详页主题设备级中间层。

    粒度：
    - dt × video_id × channel × kol_name × country × merchant × device_id

    说明：
    - DWS 保留设备 + 多维度主题事实，供 ADS 汇总商品页 / 第三方外跳的 PV、UV，
      以及按视频代码 / 投放平台 / 红人账号 / 国家 / 商家的多维度拆分
    - PV 口径按 `raw_event_id` 全局唯一，设备级先 COUNT(DISTINCT raw_event_id)，
      ADS 再按需要的粒度对设备求和即得对应 PV
    - UV 口径由 ADS 对 `pv > 0` 的设备去重计数得到（同一设备当日可能落在多个维度组合，
      不能直接对各维度 UV 求和得到总 UV）
    """
    if not dates:
        logging.info("没有新的 dws_launch_product_detail_device_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    event_ts = event_ts_expr()
    dims = dimension_exprs()

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        video_id,
        channel,
        kol_name,
        country,
        merchant,
        device_id,
        product_detail_pv,
        shopnow_pv,
        update_time
    )
    WITH
    -- 本批待处理日期清单。
    target_dates AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 提取商品页浏览与第三方外跳两类事件的设备、多维度与去重原始事件 id。
    base_events AS (
        SELECT
            DATE({event_ts}, '{EASTERN_TZ}') AS dt,
            NULLIF(prop_device_id, '') AS device_id,
            {dims},
            event_name,
            NULLIF(raw_event_id, '') AS raw_event_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE event_name IN ({LAUNCH_PRODUCT_DETAIL_EVENT_NAMES})
            AND NULLIF(prop_device_id, '') IS NOT NULL
            AND DATE({event_ts}, '{EASTERN_TZ}') IN (
                SELECT dt
                FROM target_dates
            )
    ),
    -- 聚合到设备 + 多维度主题事实，PV 按 raw_event_id 去重。
    device_metric_daily AS (
        SELECT
            dt,
            video_id,
            channel,
            kol_name,
            country,
            merchant,
            device_id,
            COUNT(DISTINCT IF(event_name = 'v_launch_product_detail', raw_event_id, NULL)) AS product_detail_pv,
            COUNT(DISTINCT IF(event_name = 'c_launch_product_detail_shopnow', raw_event_id, NULL)) AS shopnow_pv
        FROM base_events
        GROUP BY dt, video_id, channel, kol_name, country, merchant, device_id
    )
    SELECT
        dt,
        video_id,
        channel,
        kol_name,
        country,
        merchant,
        device_id,
        product_detail_pv,
        shopnow_pv,
        CURRENT_TIMESTAMP() AS update_time
    FROM device_metric_daily;
    """
    logging.info("开始处理: dws_launch_product_detail_device_daily")
    client.query(query).result()
    logging.info(
        "dws_launch_product_detail_device_daily 刷新完成, 处理日期: %s", dates
    )


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_launch_product_detail_device_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 dws_launch_product_detail_device_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_launch_product_detail_device_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(
        "dws_launch_product_detail_device_daily 完成, 耗时: %.1f 秒", elapsed
    )
