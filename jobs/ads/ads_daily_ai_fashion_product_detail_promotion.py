"""AI-Fashion 商详页投放日报。

落地 ``decom.ads_daily_ai_fashion_product_detail_promotion``，
粒度：``dt × ac_creative``（含 ``ac_creative = 'overall'`` 汇总行）。

依赖：
- decom.dws_ai_fashion_product_detail_promotion_device_daily

硬性条件：
- ADS 只消费 DWS，不直接读 dwd_event_log
- ``overall`` 行按设备全局去重，不能对各素材 UV 直接求和（同一设备可能看过多个素材）
- 报表起始日期前当前无投放数据，靠补零使目标表永远有行，避免全量分支空转
"""

import argparse
from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_daily_ai_fashion_product_detail_promotion"
SOURCE_TABLE_NAME = "dws_ai_fashion_product_detail_promotion_device_daily"
TORONTO_TZ = "America/Toronto"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "ads" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6
# 报表零值行起始日 = 看板上线日。投放埋点未上报期间，靠补零让表永远有行，
# 避免「目标表为空 -> 全量分支」在无数据时空转，也让 Superset 有行可渲染。
REPORT_START_DT = date(2026, 7, 31)

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
        description="构建 AI-Fashion 商详页投放日报（dt × ac_creative，含 overall 汇总行）"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="从 REPORT_START_DT 到今天全量重刷，而不是默认最近增量窗口",
    )
    return parser.parse_args()


def dates_to_sql_list(dates: list[date]) -> str:
    return ", ".join([f"'{d}'" for d in dates])


def dates_to_array_sql(dates: list[date]) -> str:
    return ", ".join([f"DATE '{d}'" for d in dates])


def target_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


def source_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{SOURCE_TABLE_NAME}"


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


def get_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
    full_refresh: bool = False,
) -> list[date]:
    """
    ADS 只消费 dws_ai_fashion_product_detail_promotion_device_daily。

    规则：
    - --full-refresh 或目标表不存在 / 为空：REPORT_START_DT ~ 今天（美东）逐日补齐，
      并并入 DWS 已落地的更早 dt（防止 DWS 被回刷到更早日期时 ADS 漏产）
    - 否则：最近 6 小时 DWS 变更日期 + 固定补最近两天

    注意：全量分支的起始日取自模块常量，不取 DWS 的 MIN/MAX dt。
    本主题过滤后当前 0 条数据，DWS 会长期是空表，
    用 DWS 边界推日期会得到空列表 -> ADS 永远写不进行 -> 每轮都回到全量分支。
    """
    is_full_snapshot = (
        full_refresh or (not target_table_exists()) or get_target_row_count() == 0
    )

    if is_full_snapshot:
        query = f"""
        WITH
        -- 报表起始日到今天的连续日期骨架。
        report_dates AS (
            SELECT dt
            FROM UNNEST(
                GENERATE_DATE_ARRAY(DATE '{REPORT_START_DT}', CURRENT_DATE('{TORONTO_TZ}'))
            ) AS dt
        ),
        -- DWS 已落地日期，兜住早于报表起始日的历史回刷。
        source_dates AS (
            SELECT DISTINCT dt
            FROM `{source_table_id()}`
            WHERE dt IS NOT NULL
        )
        SELECT DISTINCT dt
        FROM (
            SELECT dt FROM report_dates
            UNION DISTINCT
            SELECT dt FROM source_dates
        )
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    WITH
    -- 最近 6 小时投放 DWS 变更日期。
    changed_dws_dates AS (
        SELECT DISTINCT dt
        FROM `{source_table_id()}`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND dt IS NOT NULL
    ),
    -- 固定补最近两天，保证零投放流量日也持续产出 overall 0 值行。
    recent_dates AS (
        SELECT DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY) AS dt
        UNION DISTINCT
        SELECT CURRENT_DATE('{TORONTO_TZ}') AS dt
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt FROM changed_dws_dates
        UNION DISTINCT
        SELECT dt FROM recent_dates
    )
    WHERE dt IS NOT NULL
        AND dt >= DATE '{REPORT_START_DT}'
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run(dates: list[date]) -> None:
    """
    投放商详页日报。

    粒度：
    - dt × ac_creative（含 ac_creative = 'overall' 汇总行）

    指标：
    - PV 直接 SUM（DWS 已把每个 raw_event_id 归属到唯一维度组合）
    - UV 对 pv > 0 的 device_id 去重，不能对拆维度后的行 COUNTIF

    说明：
    - 只对 overall 行按 target_dates 补零；素材明细行按实际数据出，
      不做 CROSS JOIN 历史素材补零（否则行数随投放规模爆炸）
    - overall 行必须单独按设备全局去重，不能对各素材 UV 求和
      （同一设备当日可能看过多个素材）
    """
    if not dates:
        logging.info("没有新的 %s 日期需要处理", TABLE_NAME)
        return

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        ac_creative,
        product_detail_exposure_uv,
        product_detail_exposure_pv,
        third_party_click_uv,
        third_party_click_pv,
        update_time
    )
    WITH
    -- 本批待处理日期清单，保证零投放流量日也补 overall 0 行。
    target_dates AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 素材粒度：只出现实际有数据的 (dt, ac_creative) 组合，不做历史素材补零。
    creative_metric_daily AS (
        SELECT
            dt,
            ac_creative,
            COUNT(DISTINCT IF(product_detail_pv > 0, device_id, NULL))
                AS product_detail_exposure_uv,
            SUM(product_detail_pv) AS product_detail_exposure_pv,
            COUNT(DISTINCT IF(third_party_click_pv > 0, device_id, NULL))
                AS third_party_click_uv,
            SUM(third_party_click_pv) AS third_party_click_pv
        FROM `{source_table_id()}`
        WHERE dt IN (
            SELECT dt
            FROM target_dates
        )
        GROUP BY dt, ac_creative
    ),
    -- overall 粒度：按设备全局去重，不跨素材相加。
    overall_metric_daily AS (
        SELECT
            dt,
            COUNT(DISTINCT IF(product_detail_pv > 0, device_id, NULL))
                AS product_detail_exposure_uv,
            SUM(product_detail_pv) AS product_detail_exposure_pv,
            COUNT(DISTINCT IF(third_party_click_pv > 0, device_id, NULL))
                AS third_party_click_uv,
            SUM(third_party_click_pv) AS third_party_click_pv
        FROM `{source_table_id()}`
        WHERE dt IN (
            SELECT dt
            FROM target_dates
        )
        GROUP BY dt
    ),
    -- overall 行按目标日期补零，保证零投放流量日也有 overall 行。
    overall_level AS (
        SELECT
            d.dt,
            'overall' AS ac_creative,
            COALESCE(m.product_detail_exposure_uv, 0) AS product_detail_exposure_uv,
            COALESCE(m.product_detail_exposure_pv, 0) AS product_detail_exposure_pv,
            COALESCE(m.third_party_click_uv, 0) AS third_party_click_uv,
            COALESCE(m.third_party_click_pv, 0) AS third_party_click_pv
        FROM target_dates d
        LEFT JOIN overall_metric_daily m
            ON d.dt = m.dt
    )
    SELECT
        dt,
        ac_creative,
        product_detail_exposure_uv,
        product_detail_exposure_pv,
        third_party_click_uv,
        third_party_click_pv,
        CURRENT_TIMESTAMP() AS update_time
    FROM creative_metric_daily
    UNION ALL
    SELECT
        dt,
        ac_creative,
        product_detail_exposure_uv,
        product_detail_exposure_pv,
        third_party_click_uv,
        third_party_click_pv,
        CURRENT_TIMESTAMP() AS update_time
    FROM overall_level;
    """
    logging.info("开始处理: %s", TABLE_NAME)
    client.query(query).result()
    logging.info("%s 刷新完成, 处理日期: %s", TABLE_NAME, dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    cli_args = parse_args()
    start_time = datetime.now(timezone.utc)
    dates = get_dates_to_process(full_refresh=cli_args.full_refresh)
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 %s 日期需要处理", TABLE_NAME)
        raise SystemExit(0)

    run(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
