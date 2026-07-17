from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_daily_makepod"
LOG_DIR = PROJECT_ROOT / "logs" / "ads" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6

client = bigquery.Client(project=PROJECT_ID)

MAKEPOD_REPORT_CHANNELS = [
    "TikTok",
    "Google",
    "Facebook",
    "TK-ELSA-AI",
    "YOUTUBE-ELSA-AI",
    "Other Channel",
    "Organic",
    "Overall",
]


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


def sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def makepod_report_channels_array_sql() -> str:
    return ", ".join(sql_string_literal(channel) for channel in MAKEPOD_REPORT_CHANNELS)


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


def get_ads_daily_makepod_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    Makepod ADS 只消费两个主题 DWS。

    规则：
    - 目标表不存在或为空时，按两个 DWS 已落地历史日期全量初始化
    - 目标表已存在时，只读取最近 6 小时内 DWS 变更的 dt
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM (
            SELECT dt
            FROM `{PROJECT_ID}.{DATASET_ID}.dws_makepod_traffic_daily`

            UNION DISTINCT

            SELECT dt
            FROM `{PROJECT_ID}.{DATASET_ID}.dws_makepod_order_daily`
        )
        WHERE dt IS NOT NULL
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT dt
    FROM (
        SELECT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_makepod_traffic_daily`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)

        UNION DISTINCT

        SELECT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_makepod_order_daily`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    )
    WHERE dt IS NOT NULL
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_ads_daily_makepod(dates: list[date]) -> None:
    """
    Makepod ADS。

    当前版本只消费：
    - dws_makepod_traffic_daily
    - dws_makepod_order_daily

    ADS 只保留日报展示字段，不再直接读取事件和订单源表。
    """
    if not dates:
        logging.info("没有新的 ads_daily_makepod 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    channels_array_sql = makepod_report_channels_array_sql()

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        channel,
        dau_uv_total,
        dau_new_uv,
        dau_old_uv,
        register_user_count,
        product_detail_view_uv,
        add_cart_uv,
        order_user_count,
        pay_user_count,
        pay_order_count,
        pay_gmv_usd,
        pay_fee_amount,
        pay_sku_quantity,
        purchase_cost_usd,
        logistics_cost_usd,
        gross_profit_usd,
        gross_profit_rate,
        update_time
    )
    WITH
    -- 本批待产出的日期清单。
    target_dates AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 渠道枚举：固定产出 8 个渠道分类。
    channel_list AS (
        SELECT channel
        FROM UNNEST([{channels_array_sql}]) AS channel
    ),
    -- Makepod 流量主题 DWS：只取本批目标日期。
    traffic_fact AS (
        SELECT
            dt,
            channel,
            dau_uv_total,
            dau_new_uv,
            dau_old_uv,
            register_user_count,
            product_detail_view_uv,
            add_cart_uv
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_makepod_traffic_daily`
        WHERE dt IN ({dates_str})
    ),
    -- Makepod 订单主题 DWS：只取本批目标日期。
    order_fact AS (
        SELECT
            dt,
            channel,
            order_user_count,
            pay_user_count,
            pay_order_count,
            pay_gmv_usd,
            pay_fee_amount,
            pay_sku_quantity,
            purchase_cost_usd,
            logistics_cost_usd
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_makepod_order_daily`
        WHERE dt IN ({dates_str})
    )
    SELECT
        d.dt,
        c.channel,
        COALESCE(t.dau_uv_total, 0) AS dau_uv_total,
        COALESCE(t.dau_new_uv, 0) AS dau_new_uv,
        COALESCE(t.dau_old_uv, 0) AS dau_old_uv,
        COALESCE(t.register_user_count, 0) AS register_user_count,
        COALESCE(t.product_detail_view_uv, 0) AS product_detail_view_uv,
        COALESCE(t.add_cart_uv, 0) AS add_cart_uv,
        COALESCE(o.order_user_count, 0) AS order_user_count,
        COALESCE(o.pay_user_count, 0) AS pay_user_count,
        COALESCE(o.pay_order_count, 0) AS pay_order_count,
        COALESCE(o.pay_gmv_usd, 0) AS pay_gmv_usd,
        COALESCE(o.pay_fee_amount, 0) AS pay_fee_amount,
        COALESCE(o.pay_sku_quantity, 0) AS pay_sku_quantity,
        COALESCE(o.purchase_cost_usd, 0) AS purchase_cost_usd,
        COALESCE(o.logistics_cost_usd, 0) AS logistics_cost_usd,
        COALESCE(o.pay_gmv_usd, 0)
            - COALESCE(o.purchase_cost_usd, 0)
            - COALESCE(o.logistics_cost_usd, 0) AS gross_profit_usd,
        CAST(
            SAFE_DIVIDE(
                COALESCE(o.pay_gmv_usd, 0)
                    - COALESCE(o.purchase_cost_usd, 0)
                    - COALESCE(o.logistics_cost_usd, 0),
                NULLIF(COALESCE(o.pay_gmv_usd, 0), 0)
            ) AS NUMERIC
        ) AS gross_profit_rate,
        CURRENT_TIMESTAMP() AS update_time
    FROM target_dates d
    CROSS JOIN channel_list c
    LEFT JOIN traffic_fact t
        ON d.dt = t.dt
        AND c.channel = t.channel
    LEFT JOIN order_fact o
        ON d.dt = o.dt
        AND c.channel = o.channel;
    """
    logging.info("开始处理: ads_daily_makepod")
    client.query(query).result()
    logging.info("ads_daily_makepod 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_ads_daily_makepod_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 ads_daily_makepod 日期需要处理")
        raise SystemExit(0)

    run_ads_daily_makepod(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("ads_daily_makepod 完成, 耗时: %.1f 秒", elapsed)
