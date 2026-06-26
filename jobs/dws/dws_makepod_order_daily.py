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
V3_DATASET_ID = "v3_decom"
SHANGHAI_TZ = "Asia/Shanghai"
TABLE_NAME = "dws_makepod_order_daily"
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6
TENANT_CODE = "T18002707"

client = bigquery.Client(project=PROJECT_ID)

MAKEPOD_INVITE_USER_CHANNEL_RULES = (
    {
        "channel": "Facebook",
        "invite_user_id": "2606093799415",
    },
    {
        "channel": "Google",
        "invite_user_id": "2606090368095",
    },
    {
        "channel": "TikTok",
        "invite_user_id": "2606091027981",
    },
)
MAKEPOD_FALLBACK_CHANNEL = "Other"
MAKEPOD_CHANNELS = [
    *(rule["channel"] for rule in MAKEPOD_INVITE_USER_CHANNEL_RULES),
    MAKEPOD_FALLBACK_CHANNEL,
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


def makepod_channels_array_sql() -> str:
    return ", ".join(sql_string_literal(channel) for channel in MAKEPOD_CHANNELS)


def build_makepod_invite_user_channel_case_sql(field_expr: str) -> str:
    when_clauses = "\n".join(
        [
            "                "
            + f"WHEN CAST({field_expr} AS STRING) = "
            + f"{sql_string_literal(rule['invite_user_id'])} "
            + f"THEN {sql_string_literal(rule['channel'])}"
            for rule in MAKEPOD_INVITE_USER_CHANNEL_RULES
        ]
    )
    return (
        "CASE\n"
        f"{when_clauses}\n"
        f"                ELSE {sql_string_literal(MAKEPOD_FALLBACK_CHANNEL)}\n"
        "            END"
    )


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


def service_ts_expr(field_name: str) -> str:
    return f"TIMESTAMP({field_name}, 'UTC')"


def datastream_change_ts_expr() -> str:
    """
    Datastream 源表优先用 source_timestamp 判断最近变更；
    缺失时回退到 gmt_update / gmt_create。
    """
    return (
        "COALESCE("
        "TIMESTAMP_MILLIS(datastream_metadata.source_timestamp), "
        "TIMESTAMP(COALESCE(gmt_update, gmt_create), 'UTC')"
        ")"
    )


def normal_change_ts_expr() -> str:
    return "TIMESTAMP(COALESCE(gmt_update, gmt_create), 'UTC')"


def get_makepod_order_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    Makepod 订单 DWS 同时依赖下单、支付和订单商品明细。

    规则：
    - 目标表不存在或为空时，按下单日与支付日的最早业务日起全量初始化到今天
    - 目标表已存在时，按最近 6 小时变更的订单键映射回受影响业务日
    - 固定补当前日和前一日，保证零值日期也能落盘
    """
    order_ts = service_ts_expr("gmt_create")
    pay_ts = service_ts_expr("pay_time")
    pay_change_ts = datastream_change_ts_expr()
    order_change_ts = datastream_change_ts_expr()
    item_change_ts = normal_change_ts_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        WITH
        source_bounds AS (
            SELECT MIN(source_dt) AS min_dt
            FROM (
                SELECT MIN(DATE({order_ts}, '{SHANGHAI_TZ}')) AS source_dt
                FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_order`
                WHERE tenant_code = '{TENANT_CODE}'
                    AND DATE({order_ts}, '{SHANGHAI_TZ}') IS NOT NULL

                UNION ALL

                SELECT MIN(DATE({pay_ts}, '{SHANGHAI_TZ}')) AS source_dt
                FROM `{PROJECT_ID}.{V3_DATASET_ID}.pay_order`
                WHERE tenant_code = '{TENANT_CODE}'
                    AND pay_status = 4
                    AND pay_time IS NOT NULL
                    AND DATE({pay_ts}, '{SHANGHAI_TZ}') IS NOT NULL
            )
        )
        SELECT dt
        FROM source_bounds,
        UNNEST(
            IF(
                min_dt IS NULL,
                ARRAY<DATE>[],
                GENERATE_DATE_ARRAY(min_dt, CURRENT_DATE('{SHANGHAI_TZ}'))
            )
        ) AS dt
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    WITH
    -- 最近 6 小时发生变化的订单键。
    changed_order_ids AS (
        SELECT DISTINCT CAST(order_id AS STRING) AS order_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_order`
        WHERE tenant_code = '{TENANT_CODE}'
            AND {order_change_ts}
                >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND order_id IS NOT NULL

        UNION DISTINCT

        SELECT DISTINCT CAST(order_id AS STRING) AS order_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.pay_order`
        WHERE tenant_code = '{TENANT_CODE}'
            AND {pay_change_ts}
                >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND order_id IS NOT NULL

        UNION DISTINCT

        SELECT DISTINCT CAST(order_id AS STRING) AS order_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_order_item`
        WHERE {item_change_ts}
                >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND order_id IS NOT NULL
    ),
    -- 受影响的下单业务日。
    changed_order_dates AS (
        SELECT DISTINCT DATE({order_ts}, '{SHANGHAI_TZ}') AS dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_order` u
        INNER JOIN changed_order_ids c
            ON CAST(u.order_id AS STRING) = c.order_id
        WHERE u.tenant_code = '{TENANT_CODE}'
            AND DATE({order_ts}, '{SHANGHAI_TZ}') IS NOT NULL
    ),
    -- 受影响的支付业务日；这里不限制当前 pay_status，避免状态变更时漏刷旧支付日。
    changed_pay_dates AS (
        SELECT DISTINCT DATE({pay_ts}, '{SHANGHAI_TZ}') AS dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.pay_order` p
        INNER JOIN changed_order_ids c
            ON CAST(p.order_id AS STRING) = c.order_id
        WHERE p.tenant_code = '{TENANT_CODE}'
            AND p.pay_time IS NOT NULL
            AND DATE({pay_ts}, '{SHANGHAI_TZ}') IS NOT NULL
    ),
    -- 固定补最近两天，保证新自然日即使零值也能产出。
    recent_dates AS (
        SELECT DATE_SUB(CURRENT_DATE('{SHANGHAI_TZ}'), INTERVAL 1 DAY) AS dt
        UNION DISTINCT
        SELECT CURRENT_DATE('{SHANGHAI_TZ}') AS dt
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt FROM changed_order_dates
        UNION DISTINCT
        SELECT dt FROM changed_pay_dates
        UNION DISTINCT
        SELECT dt FROM recent_dates
    )
    WHERE dt IS NOT NULL
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_makepod_order_daily(dates: list[date]) -> None:
    """
    产出 Makepod 订单主题日事实层。

    粒度：
    - dt

    指标：
    - 下单人数
    - 支付人数
    - 支付订单数
    - 支付 GMV（USD）
    - 支付手续费（USD）
    - 支付 SKU 件数
    """
    if not dates:
        logging.info("没有新的 dws_makepod_order_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    channels_array_sql = makepod_channels_array_sql()
    order_ts = service_ts_expr("gmt_create")
    pay_ts = service_ts_expr("pay_time")
    user_channel_sql = build_makepod_invite_user_channel_case_sql("invite_user_id")

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        channel,
        order_user_count,
        pay_user_count,
        pay_order_count,
        pay_gmv_usd,
        pay_fee_amount,
        pay_sku_quantity,
        update_time
    )
    WITH
    -- 本批待产出的日期清单。
    target_dates AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 渠道枚举：固定产出 4 个展示渠道。
    channel_list AS (
        SELECT channel
        FROM UNNEST([{channels_array_sql}]) AS channel
    ),
    -- 汇率字典：按币种统一映射到 USD。
    currency_rate_dim AS (
        SELECT
            UPPER(from_currency) AS from_currency,
            MAX(exchange_rate) AS usd_rate
        FROM `{PROJECT_ID}.{DATASET_ID}.currency_conversion_dict`
        WHERE UPPER(to_currency) = 'USD'
        GROUP BY 1
    ),
    -- 订单商品件数：先按 order_id 汇总，避免支付表 join 后重复放大。
    order_item_quantity AS (
        SELECT
            CAST(order_id AS STRING) AS order_id,
            SUM(COALESCE(SAFE_CAST(sku_quantity AS INT64), 0)) AS pay_sku_quantity
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_order_item`
        WHERE order_id IS NOT NULL
        GROUP BY 1
    ),
    -- 用户渠道维表：按 user_info 把服务端 user_id 映射为稳定渠道。
    user_channel_dim AS (
        SELECT
            service_user_id,
            channel
        FROM (
            SELECT
                CAST(user_id AS STRING) AS service_user_id,
                {user_channel_sql} AS channel,
                ROW_NUMBER() OVER (
                    PARTITION BY CAST(user_id AS STRING)
                    ORDER BY
                        {datastream_change_ts_expr()} DESC,
                        TIMESTAMP(gmt_create, 'UTC') DESC
                ) AS rn
            FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_info`
            WHERE tenant_code = '{TENANT_CODE}'
                AND NULLIF(CAST(user_id AS STRING), '') IS NOT NULL
        )
        WHERE rn = 1
    ),
    -- 下单人数：按 user_order.gmt_create 和用户渠道统计。
    order_metrics AS (
        SELECT
            DATE({order_ts}, '{SHANGHAI_TZ}') AS dt,
            COALESCE(c.channel, '{MAKEPOD_FALLBACK_CHANNEL}') AS channel,
            COUNT(
                DISTINCT IF(
                    NULLIF(CAST(o.user_id AS STRING), '') IS NOT NULL,
                    CAST(o.user_id AS STRING),
                    NULL
                )
            ) AS order_user_count
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_order` o
        LEFT JOIN user_channel_dim c
            ON CAST(o.user_id AS STRING) = c.service_user_id
        WHERE o.tenant_code = '{TENANT_CODE}'
            AND DATE({order_ts}, '{SHANGHAI_TZ}') IN ({dates_str})
        GROUP BY dt, channel
    ),
    -- 支付指标：按 pay_order.pay_time 和用户渠道统计支付成功订单，并统一换算 USD。
    pay_metrics AS (
        SELECT
            DATE({pay_ts}, '{SHANGHAI_TZ}') AS dt,
            COALESCE(c.channel, '{MAKEPOD_FALLBACK_CHANNEL}') AS channel,
            COUNT(
                DISTINCT IF(
                    NULLIF(CAST(p.user_id AS STRING), '') IS NOT NULL,
                    CAST(p.user_id AS STRING),
                    NULL
                )
            ) AS pay_user_count,
            COUNT(
                DISTINCT IF(
                    NULLIF(CAST(p.order_id AS STRING), '') IS NOT NULL,
                    CAST(p.order_id AS STRING),
                    NULL
                )
            ) AS pay_order_count,
            COALESCE(SUM(
                (
                    COALESCE(SAFE_CAST(p.pay_amount AS NUMERIC), 0)
                    + COALESCE(SAFE_CAST(p.pay_fee AS NUMERIC), 0)
                )
                * COALESCE(
                    r.usd_rate,
                    CASE
                        WHEN UPPER(CAST(p.currency_type AS STRING)) = 'USD' THEN 1
                        ELSE 0
                    END
                )
            ), 0) AS pay_gmv_usd,
            COALESCE(SUM(
                COALESCE(SAFE_CAST(p.pay_fee AS NUMERIC), 0)
                * COALESCE(
                    r.usd_rate,
                    CASE
                        WHEN UPPER(CAST(p.currency_type AS STRING)) = 'USD' THEN 1
                        ELSE 0
                    END
                )
            ), 0) AS pay_fee_amount,
            COALESCE(SUM(COALESCE(i.pay_sku_quantity, 0)), 0) AS pay_sku_quantity
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.pay_order` p
        LEFT JOIN currency_rate_dim r
            ON UPPER(CAST(p.currency_type AS STRING)) = r.from_currency
        LEFT JOIN order_item_quantity i
            ON CAST(p.order_id AS STRING) = i.order_id
        LEFT JOIN user_channel_dim c
            ON CAST(p.user_id AS STRING) = c.service_user_id
        WHERE p.tenant_code = '{TENANT_CODE}'
            AND p.pay_status = 4
            AND p.pay_time IS NOT NULL
            AND DATE({pay_ts}, '{SHANGHAI_TZ}') IN ({dates_str})
        GROUP BY dt, channel
    )
    SELECT
        d.dt,
        c.channel,
        COALESCE(o.order_user_count, 0) AS order_user_count,
        COALESCE(p.pay_user_count, 0) AS pay_user_count,
        COALESCE(p.pay_order_count, 0) AS pay_order_count,
        COALESCE(p.pay_gmv_usd, 0) AS pay_gmv_usd,
        COALESCE(p.pay_fee_amount, 0) AS pay_fee_amount,
        COALESCE(p.pay_sku_quantity, 0) AS pay_sku_quantity,
        CURRENT_TIMESTAMP() AS update_time
    FROM target_dates d
    CROSS JOIN channel_list c
    LEFT JOIN order_metrics o
        ON d.dt = o.dt
        AND c.channel = o.channel
    LEFT JOIN pay_metrics p
        ON d.dt = p.dt
        AND c.channel = p.channel;
    """
    logging.info("开始处理: dws_makepod_order_daily")
    client.query(query).result()
    logging.info("dws_makepod_order_daily 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_makepod_order_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 dws_makepod_order_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_makepod_order_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dws_makepod_order_daily 完成, 耗时: %.1f 秒", elapsed)
