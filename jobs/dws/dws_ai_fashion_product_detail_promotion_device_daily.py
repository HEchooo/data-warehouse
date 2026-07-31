"""AI-Fashion 商详页投放设备日事实层。

落地 ``decom.dws_ai_fashion_product_detail_promotion_device_daily``，
粒度：``dt × ac_channel × ac_creative × device_id``。

依赖：
- decom.dwd_event_log（商详页曝光 v_product_detail、第三方商品点击 c_third_party_goods）

硬性条件：
- 投放流量界定为 prop_url 的 origin 严格等于 https://alvinclub.com，
  且 prop_params 中 ac_channel、ac_creative 同时非空；不限 args.page_key
- c_third_party_goods 与 LTA 主题（decom.dws_lta_thirdparty_event_daily）共用，
  domain 过滤是两个主题唯一的隔离手段（LTA 走 mycch.me），不得放宽
- 事件名在前端上报为 v_Product_Detail，但 dwd_event_log 落库时统一小写化
  （jobs/dwd/dwd_event_log/dwd_event_log.py 的 row.event_name.lower()），
  这里必须用小写 v_product_detail 匹配

刻意不使用「目标表不存在或行数为 0 -> 全量」判定：本主题过滤后当前 0 条数据，
DWS 会长期是空表，行数判定会让每轮调度都全表扫描 dwd_event_log
（实测约 317 MB/次，cron 每 30 分钟一次），且 run() 永远拿不到日期、
表永远写不进行，形成死循环。全量重刷改为 --full-refresh 人工触发。
"""

import argparse
from datetime import date, datetime, timedelta, timezone
import logging
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_ai_fashion_product_detail_promotion_device_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6
# 投放商详页主题只看这两个埋点（均为 dwd_event_log 落库后的小写形态）。
PROMOTION_EVENT_NAMES = "'v_product_detail', 'c_third_party_goods'"
# 投放流量的落地域名；c_third_party_goods 与 LTA 主题共用，靠这个域名隔离。
PROMOTION_ORIGIN = "https://alvinclub.com"
# --full-refresh 起始业务日：v_product_detail 在 dwd_event_log 中的最早业务日。
FULL_REFRESH_START_DT = date(2026, 2, 9)

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="构建 AI-Fashion 商详页投放设备日事实（dt × ac_channel × ac_creative × device_id）"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="从 FULL_REFRESH_START_DT 到今天逐日全量重刷，而不是默认最近增量窗口",
    )
    return parser.parse_args()


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


def promotion_traffic_filter_expr() -> str:
    """
    投放流量界定：
    - prop_url 的 origin 严格等于 https://alvinclub.com（不含 www、不含 .ai / .ca；
      prop_url 为空时表达式为 NULL，自动被过滤）
    - prop_params 中 ac_channel 与 ac_creative 同时存在且值非空
      （结尾 [^&] 即“至少一个非 & 字符”，等价非空串判定；
        [?&] 前缀保证不会误命中 ac_channel_id= / xxac_channel= 这类参数名）
    实测 prop_url 非空 <=> prop_app_type='h5'，投放流量必然是 H5，不会误伤 App 内曝光。
    """
    return (
        f"REGEXP_EXTRACT(prop_url, r'^(https?://[^/]+)') = '{PROMOTION_ORIGIN}'\n"
        "        AND REGEXP_CONTAINS(prop_params, r'[?&]ac_channel=[^&]')\n"
        "        AND REGEXP_CONTAINS(prop_params, r'[?&]ac_creative=[^&]')"
    )


def promotion_dimension_exprs() -> str:
    """投放维度取值；同名参数重复出现时取第一个，值保持 URL 原样不做 percent-decode。"""
    return """
        NULLIF(REGEXP_EXTRACT(prop_params, r'[?&]ac_channel=([^&]*)'), '') AS ac_channel,
        NULLIF(REGEXP_EXTRACT(prop_params, r'[?&]ac_creative=([^&]*)'), '') AS ac_creative
    """


def get_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
    full_refresh: bool = False,
) -> list[date]:
    """
    投放商详页 DWS 只依赖 dwd_event_log。

    规则：
    - --full-refresh：从 FULL_REFRESH_START_DT 到今天（美东）逐日重刷，
      日期数组直接在 Python 侧生成，不扫描事件表
    - 默认：最近 6 小时 update_time 命中的投放流量业务日 + 固定补最近两天

    刻意不使用「目标表不存在或行数为 0 -> 全量」判定，见模块 docstring。
    """
    if full_refresh:
        today = datetime.now(ZoneInfo(TORONTO_TZ)).date()
        span = (today - FULL_REFRESH_START_DT).days
        return [FULL_REFRESH_START_DT + timedelta(days=i) for i in range(span + 1)]

    event_ts = event_ts_expr()
    query = f"""
    WITH
    -- 最近 6 小时 dwd_event_log 变更命中的投放流量业务日。
    changed_event_dates AS (
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND event_name IN ({PROMOTION_EVENT_NAMES})
            AND {promotion_traffic_filter_expr()}
            AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
    ),
    -- 固定补最近两天，保证投放埋点上报当天即可出数，且空数据期也持续刷新分区。
    recent_dates AS (
        SELECT DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY) AS dt
        UNION DISTINCT
        SELECT CURRENT_DATE('{TORONTO_TZ}') AS dt
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt FROM changed_event_dates
        UNION DISTINCT
        SELECT dt FROM recent_dates
    )
    WHERE dt IS NOT NULL
        AND dt <= CURRENT_DATE('{TORONTO_TZ}')
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_ai_fashion_product_detail_promotion_device_daily(dates: list[date]) -> None:
    """
    产出投放商详页设备级中间层。

    粒度：
    - dt × ac_channel × ac_creative × device_id
      （DWS 预留投放维度，ADS 当前只按 dt × ac_creative 聚合；
        未来 ADS 加渠道维度不需要重建 DWS）

    说明：
    - 先按 raw_event_id 去重再分组，保证每个事件只归属一个维度组合，
      ADS 端 SUM(pv) 恒等于 COUNT(DISTINCT raw_event_id)
    - device_id 为空的事件保留（device_id = NULL）：PV 需要计入，
      UV 由下游 COUNT(DISTINCT device_id) 自动忽略 NULL
    """
    if not dates:
        logging.info("没有新的 %s 日期需要处理", TABLE_NAME)
        return

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    event_ts = event_ts_expr()
    min_dt = min(dates)
    max_dt = max(dates)

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        ac_channel,
        ac_creative,
        device_id,
        product_detail_pv,
        third_party_click_pv,
        update_time
    )
    WITH
    -- 本批待处理日期清单。
    target_dates AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 投放流量事件：域名 + 投放参数双重界定，并按 raw_event_id 去重到唯一维度组合。
    base_events AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            {promotion_dimension_exprs()},
            NULLIF(prop_device_id, '') AS device_id,
            event_name,
            raw_event_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE(logAt_timestamp) BETWEEN DATE_SUB(DATE '{min_dt}', INTERVAL 1 DAY)
                AND DATE_ADD(DATE '{max_dt}', INTERVAL 1 DAY)
            AND DATE({event_ts}, '{TORONTO_TZ}') IN (
                SELECT dt
                FROM target_dates
            )
            AND event_name IN ({PROMOTION_EVENT_NAMES})
            AND NULLIF(raw_event_id, '') IS NOT NULL
            AND {promotion_traffic_filter_expr()}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY raw_event_id
            ORDER BY logAt_timestamp, hash_id
        ) = 1
    ),
    -- 聚合到投放维度 + 设备粒度，两个事件各自按 raw_event_id 去重计 PV。
    device_metric_daily AS (
        SELECT
            dt,
            ac_channel,
            ac_creative,
            device_id,
            COUNT(DISTINCT IF(event_name = 'v_product_detail', raw_event_id, NULL))
                AS product_detail_pv,
            COUNT(DISTINCT IF(event_name = 'c_third_party_goods', raw_event_id, NULL))
                AS third_party_click_pv
        FROM base_events
        GROUP BY dt, ac_channel, ac_creative, device_id
    )
    SELECT
        dt,
        ac_channel,
        ac_creative,
        device_id,
        product_detail_pv,
        third_party_click_pv,
        CURRENT_TIMESTAMP() AS update_time
    FROM device_metric_daily;
    """
    logging.info("开始处理: %s", TABLE_NAME)
    client.query(query).result()
    logging.info("%s 刷新完成, 处理日期: %s", TABLE_NAME, dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    if not target_table_exists():
        logging.error("目标表不存在，请先执行 ddl/dws/%s.sql", TABLE_NAME)
        raise SystemExit(1)

    cli_args = parse_args()
    start_time = datetime.now(timezone.utc)
    dates = get_dates_to_process(full_refresh=cli_args.full_refresh)
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 %s 日期需要处理", TABLE_NAME)
        raise SystemExit(0)

    run_dws_ai_fashion_product_detail_promotion_device_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
