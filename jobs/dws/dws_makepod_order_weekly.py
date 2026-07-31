"""DWS Makepod 订单主题周事实：支撑周维度支付用户/订单/GMV 统计。

落地 ``decom.dws_makepod_order_weekly``。

依赖：
- v3_decom.pay_order（支付成功订单明细）
- decom.dim_makepod_user_channel（用户渠道归属）
- decom.currency_conversion_dict（汇率换算）

粒度：week_start_dt（周日起自然周） × channel × order_group

背景：周维度支付用户数不能由 dws_makepod_order_daily 的日值直接相加——跨天多次支付的
同一用户会被重复计数，必须回到 pay_order 明细按 user_id 在周窗口内整体去重后重新聚合。
详见 docs/topics/【Makepod】高价值用户监控/patterns.md。

order_group（订单分组）沿用【Makepod】高价值用户监控的分桶口径：按用户在同一周内的支付成功
订单数分为 1单 / 2单 / 3单及以上；另外保留 "全部分组"（不分桶的原有汇总）以兼容既有展示。
"""

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
TABLE_NAME = "dws_makepod_order_weekly"
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6
TENANT_CODE = "T18002707"
# Makepod 周报口径固定周日 ~ 周六（与业务方口径对齐，跟仓库其他周表 WEEK(MONDAY) 不同）。
WEEK_START_EXPR = "DATE_TRUNC({col}, WEEK(SUNDAY))"

client = bigquery.Client(project=PROJECT_ID)

MAKEPOD_ORGANIC_CHANNEL = "Organic"
MAKEPOD_REPORT_CHANNELS = [
    "TikTok",
    "Google",
    "Facebook",
    "TK-ELSA-AI",
    "YOUTUBE-ELSA-AI",
    "Reddit",
    "Other Channel",
    MAKEPOD_ORGANIC_CHANNEL,
    "Overall",
]

# 订单分组："全部分组" 为不分桶的原有汇总（兼容既有展示），其余三个是按周内支付订单数分桶。
MAKEPOD_ALL_ORDER_GROUP = "全部分组"
MAKEPOD_ORDER_GROUPS = [
    MAKEPOD_ALL_ORDER_GROUP,
    "1单",
    "2单",
    "3单及以上",
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


def makepod_order_groups_array_sql() -> str:
    return ", ".join(sql_string_literal(group) for group in MAKEPOD_ORDER_GROUPS)


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


def pay_ts_expr() -> str:
    return "TIMESTAMP(pay_time, 'UTC')"


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


def get_week_starts_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    Makepod 订单周事实同时依赖支付明细和用户渠道维表。

    规则：
    - 目标表不存在或为空时，从支付成功订单最早业务日所在周起，全量初始化到当前周
    - 目标表已存在时，按最近 6 小时变更的支付订单、渠道维表变更映射回受影响支付周
    - 固定补当前周，保证零值周也能落盘
    """
    pay_ts = pay_ts_expr()
    pay_change_ts = datastream_change_ts_expr()
    current_week_expr = WEEK_START_EXPR.format(col=f"CURRENT_DATE('{SHANGHAI_TZ}')")
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        WITH
        bounds AS (
            SELECT MIN(DATE({pay_ts}, '{SHANGHAI_TZ}')) AS min_pay_date
            FROM `{PROJECT_ID}.{V3_DATASET_ID}.pay_order`
            WHERE tenant_code = '{TENANT_CODE}'
                AND pay_status = 4
                AND pay_time IS NOT NULL
        )
        SELECT week_start_dt
        FROM bounds,
        UNNEST(
            IF(
                min_pay_date IS NULL,
                ARRAY<DATE>[],
                GENERATE_DATE_ARRAY(
                    {WEEK_START_EXPR.format(col='min_pay_date')},
                    {current_week_expr},
                    INTERVAL 7 DAY
                )
            )
        ) AS week_start_dt
        ORDER BY week_start_dt
        """
        results = client.query(query).result()
        return [row.week_start_dt for row in results]

    query = f"""
    WITH
    -- 最近 6 小时发生变化的支付订单键。
    changed_order_ids AS (
        SELECT DISTINCT CAST(order_id AS STRING) AS order_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.pay_order`
        WHERE tenant_code = '{TENANT_CODE}'
            AND {pay_change_ts}
                >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND order_id IS NOT NULL
    ),
    -- 受影响的支付周；这里不限制当前 pay_status，避免状态变更时漏刷旧支付周。
    changed_pay_weeks AS (
        SELECT DISTINCT {WEEK_START_EXPR.format(col=f"DATE({pay_ts}, '{SHANGHAI_TZ}')")}
            AS week_start_dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.pay_order` p
        INNER JOIN changed_order_ids c
            ON CAST(p.order_id AS STRING) = c.order_id
        WHERE p.tenant_code = '{TENANT_CODE}'
            AND p.pay_time IS NOT NULL
    ),
    -- 最近 6 小时用户渠道维表变化影响的支付周；同样不限制当前 pay_status。
    changed_channel_pay_weeks AS (
        SELECT DISTINCT {WEEK_START_EXPR.format(col=f"DATE({pay_ts}, '{SHANGHAI_TZ}')")}
            AS week_start_dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.pay_order` p
        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.dim_makepod_user_channel` c
            ON CAST(p.user_id AS STRING) = c.service_user_id
        WHERE p.tenant_code = '{TENANT_CODE}'
            AND c.update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND p.pay_time IS NOT NULL
    ),
    -- 固定补当前周，保证本周数据随支付事实滚动更新。
    current_week AS (
        SELECT {current_week_expr} AS week_start_dt
    )
    SELECT DISTINCT week_start_dt
    FROM (
        SELECT week_start_dt FROM changed_pay_weeks
        UNION DISTINCT
        SELECT week_start_dt FROM changed_channel_pay_weeks
        UNION DISTINCT
        SELECT week_start_dt FROM current_week
    )
    WHERE week_start_dt IS NOT NULL
        AND week_start_dt <= {current_week_expr}
    ORDER BY week_start_dt
    """
    results = client.query(query).result()
    return [row.week_start_dt for row in results]


def run_dws_makepod_order_weekly(week_starts: list[date]) -> None:
    """
    产出 Makepod 订单主题周事实层。

    粒度：
    - week_start_dt × channel × order_group

    指标：
    - 支付人数（周内按 user_id 去重）
    - 支付订单数（周内按 order_id 去重）
    - 支付 GMV（USD）

    order_group："全部分组"为不分桶汇总（兼容既有展示），其余按用户周内支付订单数分为
    1单 / 2单 / 3单及以上，口径对齐【Makepod】高价值用户监控。
    """
    if not week_starts:
        logging.info("没有新的 dws_makepod_order_weekly 周需要处理")
        return

    week_starts_str = dates_to_sql_list(week_starts)
    week_starts_array_sql = dates_to_array_sql(week_starts)
    channels_array_sql = makepod_report_channels_array_sql()
    order_groups_array_sql = makepod_order_groups_array_sql()
    pay_ts = pay_ts_expr()

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE week_start_dt IN ({week_starts_str});

    INSERT INTO `{target_table_id()}`
    (
        week_start_dt,
        channel,
        order_group,
        pay_user_count,
        pay_order_count,
        pay_gmv_usd,
        update_time
    )
    WITH
    -- 本批待产出的周区间。
    target_weeks AS (
        SELECT
            week_start_dt,
            DATE_ADD(week_start_dt, INTERVAL 6 DAY) AS week_end_dt
        FROM UNNEST([{week_starts_array_sql}]) AS week_start_dt
    ),
    -- 渠道枚举：固定产出 8 个互斥渠道和 1 个 Overall 汇总渠道。
    channel_list AS (
        SELECT channel
        FROM UNNEST([{channels_array_sql}]) AS channel
    ),
    -- 订单分组枚举："全部分组"（不分桶）+ 3 个订单数分桶。
    order_group_list AS (
        SELECT order_group
        FROM UNNEST([{order_groups_array_sql}]) AS order_group
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
    -- 用户渠道维表：复用 DIM 层稳定服务端用户渠道。
    user_channel_dim AS (
        SELECT
            service_user_id,
            channel
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_makepod_user_channel`
        WHERE NULLIF(service_user_id, '') IS NOT NULL
    ),
    -- 支付成功订单：同一 order_id 只保留最新一条支付记录，避免金额重复放大。
    paid_order_fact AS (
        SELECT * EXCEPT(row_num)
        FROM (
            SELECT
                CAST(p.order_id AS STRING) AS order_id,
                CAST(p.user_id AS STRING) AS user_id,
                DATE({pay_ts}, '{SHANGHAI_TZ}') AS pay_date,
                p.pay_amount,
                p.pay_fee,
                p.currency_type,
                ROW_NUMBER() OVER (
                    PARTITION BY CAST(p.order_id AS STRING)
                    ORDER BY
                        COALESCE(
                            TIMESTAMP_MILLIS(p.datastream_metadata.source_timestamp),
                            TIMESTAMP(p.gmt_update, 'UTC'),
                            TIMESTAMP(p.gmt_create, 'UTC')
                        ) DESC,
                        p.id DESC
                ) AS row_num
            FROM `{PROJECT_ID}.{V3_DATASET_ID}.pay_order` p
            WHERE p.tenant_code = '{TENANT_CODE}'
                AND p.pay_status = 4
                AND p.pay_time IS NOT NULL
                AND NULLIF(CAST(p.order_id AS STRING), '') IS NOT NULL
        )
        WHERE row_num = 1
    ),
    -- 按周关联支付订单，并打上用户渠道、换算 GMV（USD）。
    order_in_week AS (
        SELECT
            w.week_start_dt,
            o.order_id,
            o.user_id,
            (
                COALESCE(SAFE_CAST(o.pay_amount AS NUMERIC), 0)
                + COALESCE(SAFE_CAST(o.pay_fee AS NUMERIC), 0)
            ) * COALESCE(
                r.usd_rate,
                CASE
                    WHEN UPPER(CAST(o.currency_type AS STRING)) = 'USD' THEN 1
                    ELSE 0
                END
            ) AS gmv_usd,
            COALESCE(c.channel, '{MAKEPOD_ORGANIC_CHANNEL}') AS channel
        FROM target_weeks w
        JOIN paid_order_fact o
            ON o.pay_date BETWEEN w.week_start_dt AND w.week_end_dt
        LEFT JOIN currency_rate_dim r
            ON UPPER(CAST(o.currency_type AS STRING)) = r.from_currency
        LEFT JOIN user_channel_dim c
            ON o.user_id = c.service_user_id
    ),
    -- 用户周内支付订单数：渠道对用户是固定属性，按 user_id 直接计数不会重复放大。
    user_week_order_count AS (
        SELECT
            week_start_dt,
            user_id,
            COUNT(DISTINCT order_id) AS order_count
        FROM order_in_week
        WHERE NULLIF(user_id, '') IS NOT NULL
        GROUP BY week_start_dt, user_id
    ),
    -- 用户周内订单分桶：1单 / 2单 / 3单及以上，口径对齐【Makepod】高价值用户监控。
    user_week_bucket AS (
        SELECT
            week_start_dt,
            user_id,
            CASE
                WHEN order_count = 1 THEN '1单'
                WHEN order_count = 2 THEN '2单'
                ELSE '3单及以上'
            END AS order_group
        FROM user_week_order_count
    ),
    -- 支付明细打上订单分桶，用于分桶级聚合。
    order_in_week_bucketed AS (
        SELECT
            o.week_start_dt,
            o.order_id,
            o.user_id,
            o.gmv_usd,
            o.channel,
            b.order_group
        FROM order_in_week o
        JOIN user_week_bucket b
            ON o.week_start_dt = b.week_start_dt
            AND o.user_id = b.user_id
    ),
    -- 分渠道 × 分桶周指标：按渠道 + 订单分桶统计支付人数、支付订单数、支付 GMV。
    pay_channel_group_metrics AS (
        SELECT
            week_start_dt,
            channel,
            order_group,
            COUNT(DISTINCT user_id) AS pay_user_count,
            COUNT(DISTINCT order_id) AS pay_order_count,
            COALESCE(SUM(gmv_usd), 0) AS pay_gmv_usd
        FROM order_in_week_bucketed
        GROUP BY week_start_dt, channel, order_group
    ),
    -- Overall × 分桶周指标：不分渠道，按用户/订单全局去重。
    pay_overall_group_metrics AS (
        SELECT
            week_start_dt,
            'Overall' AS channel,
            order_group,
            COUNT(DISTINCT user_id) AS pay_user_count,
            COUNT(DISTINCT order_id) AS pay_order_count,
            COALESCE(SUM(gmv_usd), 0) AS pay_gmv_usd
        FROM order_in_week_bucketed
        GROUP BY week_start_dt, order_group
    ),
    -- 分渠道 × 全部分组周指标：不分桶的原有汇总，兼容既有展示。
    pay_channel_allgroup_metrics AS (
        SELECT
            week_start_dt,
            channel,
            '{MAKEPOD_ALL_ORDER_GROUP}' AS order_group,
            COUNT(DISTINCT IF(NULLIF(user_id, '') IS NOT NULL, user_id, NULL))
                AS pay_user_count,
            COUNT(DISTINCT IF(NULLIF(order_id, '') IS NOT NULL, order_id, NULL))
                AS pay_order_count,
            COALESCE(SUM(gmv_usd), 0) AS pay_gmv_usd
        FROM order_in_week
        GROUP BY week_start_dt, channel
    ),
    -- Overall × 全部分组周指标：不分渠道、不分桶的原有汇总，兼容既有展示。
    pay_overall_allgroup_metrics AS (
        SELECT
            week_start_dt,
            'Overall' AS channel,
            '{MAKEPOD_ALL_ORDER_GROUP}' AS order_group,
            COUNT(DISTINCT IF(NULLIF(user_id, '') IS NOT NULL, user_id, NULL))
                AS pay_user_count,
            COUNT(DISTINCT IF(NULLIF(order_id, '') IS NOT NULL, order_id, NULL))
                AS pay_order_count,
            COALESCE(SUM(gmv_usd), 0) AS pay_gmv_usd
        FROM order_in_week
        GROUP BY week_start_dt
    ),
    -- 支付指标汇总：合并 渠道×分桶 / Overall×分桶 / 渠道×全部分组 / Overall×全部分组 四类。
    pay_metrics AS (
        SELECT * FROM pay_channel_group_metrics

        UNION ALL

        SELECT * FROM pay_overall_group_metrics

        UNION ALL

        SELECT * FROM pay_channel_allgroup_metrics

        UNION ALL

        SELECT * FROM pay_overall_allgroup_metrics
    )
    SELECT
        w.week_start_dt,
        c.channel,
        g.order_group,
        COALESCE(p.pay_user_count, 0) AS pay_user_count,
        COALESCE(p.pay_order_count, 0) AS pay_order_count,
        COALESCE(p.pay_gmv_usd, 0) AS pay_gmv_usd,
        CURRENT_TIMESTAMP() AS update_time
    FROM target_weeks w
    CROSS JOIN channel_list c
    CROSS JOIN order_group_list g
    LEFT JOIN pay_metrics p
        ON w.week_start_dt = p.week_start_dt
        AND c.channel = p.channel
        AND g.order_group = p.order_group;
    """
    logging.info("开始处理: dws_makepod_order_weekly")
    client.query(query).result()
    logging.info("dws_makepod_order_weekly 刷新完成, 处理周: %s", week_starts)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    week_starts = get_week_starts_to_process()
    logging.info("待处理周: %s", week_starts if week_starts else "[]")

    if not week_starts:
        logging.info("没有新的 dws_makepod_order_weekly 周需要处理")
        raise SystemExit(0)

    run_dws_makepod_order_weekly(week_starts)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dws_makepod_order_weekly 完成, 耗时: %.1f 秒", elapsed)
