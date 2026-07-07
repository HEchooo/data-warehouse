from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from jobs.event_activity_rules import PASSIVE_ACTIVITY_EXCLUDED_EVENTS_SQL

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
V3_DATASET_ID = "v3_decom"
SHANGHAI_TZ = "Asia/Shanghai"
TABLE_NAME = "dws_makepod_traffic_daily"
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6
TENANT_CODE = "T18002707"

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


def build_makepod_event_channel_case_sql(
    share_code_expr: str,
    invite_user_id_expr: str,
) -> str:
    return f"""
    CASE
        WHEN CAST({share_code_expr} AS STRING) = 'AAACXvMzgK4'
            OR CAST({invite_user_id_expr} AS STRING) = '2606091027981'
            THEN 'TikTok'
        WHEN CAST({share_code_expr} AS STRING) = 'AAACXvMF8vw'
            OR CAST({invite_user_id_expr} AS STRING) = '2606090368095'
            THEN 'Google'
        WHEN CAST({share_code_expr} AS STRING) = 'AAACXvNZSVQ'
            OR CAST({invite_user_id_expr} AS STRING) = '2606093799415'
            THEN 'Facebook'
        WHEN CAST({share_code_expr} AS STRING) = 'AAACXzUeDVA'
            OR CAST({invite_user_id_expr} AS STRING) = '2607062764531'
            THEN 'TK-ELSA-AI'
        WHEN CAST({share_code_expr} AS STRING) = 'AAACXzUDkV8'
            OR CAST({invite_user_id_expr} AS STRING) = '2607063062524'
            THEN 'YOUTUBE-ELSA-AI'
        WHEN NULLIF(CAST({share_code_expr} AS STRING), '') IS NOT NULL
            OR NULLIF(CAST({invite_user_id_expr} AS STRING), '') IS NOT NULL
            THEN 'Other Channel'
        ELSE 'Organic'
    END
    """


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


def makepod_event_date_expr() -> str:
    return f"DATE(logAt_timestamp, '{SHANGHAI_TZ}')"


def service_ts_expr(field_name: str = "gmt_create") -> str:
    return f"TIMESTAMP({field_name}, 'UTC')"


def get_makepod_traffic_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    Makepod 流量 DWS 同时依赖事件、首次到访维表和用户渠道维表。

    规则：
    - 目标表不存在或为空时，按事件与注册的最早业务日起全量初始化到今天
    - 目标表已存在时，只重刷最近 6 小时受影响的业务日
    - 固定补当前日和前一日，保证零值日期也能落盘
    """
    event_dt = makepod_event_date_expr()
    register_ts = service_ts_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        WITH
        source_bounds AS (
            SELECT MIN(source_dt) AS min_dt
            FROM (
                SELECT MIN({event_dt}) AS source_dt
                FROM `{PROJECT_ID}.{DATASET_ID}.dwd_makepod_event_log`
                WHERE {event_dt} IS NOT NULL

                UNION ALL

                SELECT MIN(DATE({register_ts}, '{SHANGHAI_TZ}')) AS source_dt
                FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_info`
                WHERE tenant_code = '{TENANT_CODE}'
                    AND DATE({register_ts}, '{SHANGHAI_TZ}') IS NOT NULL
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
    -- 最近 6 小时变更的 Makepod 事件业务日。
    changed_event_dates AS (
        SELECT DISTINCT {event_dt} AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_makepod_event_log`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND {event_dt} IS NOT NULL
    ),
    -- 最近 6 小时首次到访维表变化的设备，可能影响该设备所有后续访问日的新老标签。
    changed_first_visit_devices AS (
        SELECT
            prop_device_id,
            first_visit_date
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_makepod_device_first_visit`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND prop_device_id IS NOT NULL
            AND prop_device_id != ''
            AND first_visit_date IS NOT NULL
    ),
    -- 首次到访变化影响的设备访问日期。
    first_visit_impacted_dates AS (
        SELECT DISTINCT {event_dt} AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_makepod_event_log` e
        INNER JOIN changed_first_visit_devices d
            ON e.prop_device_id = d.prop_device_id
        WHERE {event_dt} BETWEEN d.first_visit_date AND CURRENT_DATE('{SHANGHAI_TZ}')
    ),
    -- 最近 6 小时用户渠道维表变化影响的注册业务日。
    changed_register_channel_dates AS (
        SELECT DISTINCT register_date AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_makepod_user_channel`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND register_date IS NOT NULL
    ),
    -- 固定补最近两天，保证新自然日即使零值也能产出。
    recent_dates AS (
        SELECT DATE_SUB(CURRENT_DATE('{SHANGHAI_TZ}'), INTERVAL 1 DAY) AS dt
        UNION DISTINCT
        SELECT CURRENT_DATE('{SHANGHAI_TZ}') AS dt
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt FROM changed_event_dates
        UNION DISTINCT
        SELECT dt FROM first_visit_impacted_dates
        UNION DISTINCT
        SELECT dt FROM changed_register_channel_dates
        UNION DISTINCT
        SELECT dt FROM recent_dates
    )
    WHERE dt IS NOT NULL
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_makepod_traffic_daily(dates: list[date]) -> None:
    """
    产出 Makepod 流量主题日事实层。

    粒度：
    - dt

    指标：
    - 全量 / 新 / 老 DAU
    - 注册人数
    - 商详页访问人数
    - 加购人数
    """
    if not dates:
        logging.info("没有新的 dws_makepod_traffic_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    channels_array_sql = makepod_report_channels_array_sql()
    event_dt = makepod_event_date_expr()
    event_channel_sql = build_makepod_event_channel_case_sql(
        "e.prop_share_code",
        "e.invite_user_id",
    )

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
        update_time
    )
    WITH
    -- 本批待产出的日期清单。
    target_dates AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 渠道枚举：固定产出 7 个互斥渠道和 1 个 Overall 汇总渠道。
    channel_list AS (
        SELECT channel
        FROM UNNEST([{channels_array_sql}]) AS channel
    ),
    -- 事件基础层：统一到设备日，并保留渠道映射和行为标记。
    event_device_base AS (
        SELECT
            {event_dt} AS dt,
            {event_channel_sql} AS channel,
            e.prop_device_id,
            MAX(
                CASE
                    WHEN e.event_name = 'view'
                        AND e.args_page_key = 'product_detail'
                    THEN 1
                    ELSE 0
                END
            ) AS has_product_detail_view,
            MAX(
                CASE
                    WHEN e.event_name = 'click'
                        AND e.args_page_key = 'product_detail'
                        AND e.args_module = 'customization_panel'
                    THEN 1
                    ELSE 0
                END
            ) AS has_add_cart
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_makepod_event_log` e
        WHERE {event_dt} IN ({dates_str})
            AND e.prop_device_id IS NOT NULL
            AND e.prop_device_id != ''
            AND e.event_name NOT IN ({PASSIVE_ACTIVITY_EXCLUDED_EVENTS_SQL})
        GROUP BY dt, channel, e.prop_device_id
    ),
    -- 设备日层：按渠道统计设备行为。
    device_daily_channel AS (
        SELECT
            dt,
            channel,
            prop_device_id,
            has_product_detail_view,
            has_add_cart
        FROM event_device_base
    ),
    -- Overall 设备日层：不分渠道按设备全局去重，保证跨渠道重复设备只算 1 个。
    device_daily_overall AS (
        SELECT
            dt,
            prop_device_id,
            MAX(has_product_detail_view) AS has_product_detail_view,
            MAX(has_add_cart) AS has_add_cart
        FROM event_device_base
        GROUP BY dt, prop_device_id
    ),
    -- 首次到访维表：用于新老设备分群。
    device_first_visit AS (
        SELECT
            prop_device_id,
            first_visit_date
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_makepod_device_first_visit`
        WHERE prop_device_id IS NOT NULL
            AND prop_device_id != ''
            AND first_visit_date IS NOT NULL
    ),
    -- 分渠道流量指标：按渠道统计设备级指标。
    traffic_channel_metrics AS (
        SELECT
            d.dt,
            d.channel,
            COUNT(DISTINCT d.prop_device_id) AS dau_uv_total,
            COUNT(
                DISTINCT IF(f.first_visit_date = d.dt, d.prop_device_id, NULL)
            ) AS dau_new_uv,
            COUNT(
                DISTINCT IF(f.first_visit_date < d.dt, d.prop_device_id, NULL)
            ) AS dau_old_uv,
            COUNT(
                DISTINCT IF(d.has_product_detail_view = 1, d.prop_device_id, NULL)
            ) AS product_detail_view_uv,
            COUNT(
                DISTINCT IF(d.has_add_cart = 1, d.prop_device_id, NULL)
            ) AS add_cart_uv
        FROM device_daily_channel d
        LEFT JOIN device_first_visit f
            ON d.prop_device_id = f.prop_device_id
        GROUP BY d.dt, d.channel
    ),
    -- Overall 流量指标：不分渠道按设备全局去重。
    traffic_overall_metrics AS (
        SELECT
            d.dt,
            'Overall' AS channel,
            COUNT(DISTINCT d.prop_device_id) AS dau_uv_total,
            COUNT(
                DISTINCT IF(f.first_visit_date = d.dt, d.prop_device_id, NULL)
            ) AS dau_new_uv,
            COUNT(
                DISTINCT IF(f.first_visit_date < d.dt, d.prop_device_id, NULL)
            ) AS dau_old_uv,
            COUNT(
                DISTINCT IF(d.has_product_detail_view = 1, d.prop_device_id, NULL)
            ) AS product_detail_view_uv,
            COUNT(
                DISTINCT IF(d.has_add_cart = 1, d.prop_device_id, NULL)
            ) AS add_cart_uv
        FROM device_daily_overall d
        LEFT JOIN device_first_visit f
            ON d.prop_device_id = f.prop_device_id
        GROUP BY d.dt
    ),
    -- 流量指标汇总：合并分渠道和 Overall。
    traffic_metrics AS (
        SELECT *
        FROM traffic_channel_metrics

        UNION ALL

        SELECT *
        FROM traffic_overall_metrics
    ),
    -- 注册基础层：直接复用用户渠道维表。
    register_base AS (
        SELECT
            register_date AS dt,
            channel,
            service_user_id AS register_user_key
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_makepod_user_channel`
        WHERE register_date IN ({dates_str})
    ),
    -- 分渠道注册指标：按渠道统计注册用户数。
    register_channel_metrics AS (
        SELECT
            dt,
            channel,
            COUNT(DISTINCT register_user_key) AS register_user_count
        FROM register_base
        WHERE register_user_key IS NOT NULL
        GROUP BY dt, channel
    ),
    -- Overall 注册指标：不分渠道汇总全部注册用户。
    register_overall_metrics AS (
        SELECT
            dt,
            'Overall' AS channel,
            COUNT(DISTINCT register_user_key) AS register_user_count
        FROM register_base
        WHERE register_user_key IS NOT NULL
        GROUP BY dt
    ),
    -- 注册指标汇总：合并分渠道和 Overall。
    register_metrics AS (
        SELECT *
        FROM register_channel_metrics

        UNION ALL

        SELECT *
        FROM register_overall_metrics
    )
    SELECT
        d.dt,
        c.channel,
        COALESCE(t.dau_uv_total, 0) AS dau_uv_total,
        COALESCE(t.dau_new_uv, 0) AS dau_new_uv,
        COALESCE(t.dau_old_uv, 0) AS dau_old_uv,
        COALESCE(r.register_user_count, 0) AS register_user_count,
        COALESCE(t.product_detail_view_uv, 0) AS product_detail_view_uv,
        COALESCE(t.add_cart_uv, 0) AS add_cart_uv,
        CURRENT_TIMESTAMP() AS update_time
    FROM target_dates d
    CROSS JOIN channel_list c
    LEFT JOIN traffic_metrics t
        ON d.dt = t.dt
        AND c.channel = t.channel
    LEFT JOIN register_metrics r
        ON d.dt = r.dt
        AND c.channel = r.channel;
    """
    logging.info("开始处理: dws_makepod_traffic_daily")
    client.query(query).result()
    logging.info("dws_makepod_traffic_daily 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_makepod_traffic_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 dws_makepod_traffic_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_makepod_traffic_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dws_makepod_traffic_daily 完成, 耗时: %.1f 秒", elapsed)
