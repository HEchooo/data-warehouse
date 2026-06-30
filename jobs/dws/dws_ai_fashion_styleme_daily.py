"""DWS 日级 AI-Fashion Style Me 事实层。

落地 ``decom.dws_ai_fashion_styleme_daily``。

依赖：
- decom.dwd_event_log（Style Me 相关埋点事件）
- v3_decom.common_ext_ai_task（Style Me 服务端任务）
- decom.dws_user_daily（留存判定用的未来活跃用户）
"""

from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
V3_DATASET_ID = "v3_decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_ai_fashion_styleme_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6

STYLEME_EVENT_NAMES = (
    "c_styleme_try_btn",
    "v_styleme_page",
    "v_styleme_popup",
    "c_add_to_closet",
    "c_add_to_closet_calendar",
    "v_result_success",
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
    return ", ".join([f"'{v}'" for v in values])


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
    query = f"SELECT COUNT(*) AS row_count FROM `{target_table_id()}`"
    rows = list(client.query(query).result())
    return int(rows[0].row_count) if rows else 0


def event_ts_expr() -> str:
    return (
        "TIMESTAMP("
        "DATETIME(logAt_timestamp, 'UTC'), "
        "COALESCE(NULLIF(prop_timezone, ''), 'UTC')"
        ")"
    )


def service_ts_expr(field_name: str = "gmt_create") -> str:
    return f"TIMESTAMP({field_name}, 'UTC')"


def service_change_ts_expr() -> str:
    """
    Datastream 源表优先用 source_timestamp 判断最近变更；
    缺失时回退到 gmt_update / gmt_create，避免漏掉最近同步的新行。
    """
    return (
        "COALESCE("
        "TIMESTAMP_MILLIS(datastream_metadata.source_timestamp), "
        "TIMESTAMP(COALESCE(gmt_update, gmt_create), 'UTC')"
        ")"
    )


def get_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    Style Me DWS 依赖事件埋点、Style Me 服务端任务和 dws_user_daily 活跃用户。

    规则：
    - 目标表不存在或为空：按上游历史业务日全量初始化
    - 目标表已存在：
      - 事件或任务变化重刷当天分区
      - dws_user_daily 的活跃变化回刷 dt-1 / dt-7 / dt-30（留存回补）
    """
    event_ts = event_ts_expr()
    service_ts = service_ts_expr()
    service_change_ts = service_change_ts_expr()
    event_names_sql = string_values_to_sql_list(STYLEME_EVENT_NAMES)
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        WITH
        -- 事件历史业务日。
        event_dates AS (
            SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
            FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
            WHERE event_name IN ({event_names_sql})
                AND prop_user_id IS NOT NULL
                AND CAST(prop_user_id AS STRING) != ''
                AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
        ),
        -- Style Me 服务端任务历史业务日。
        styleme_dates AS (
            SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS dt
            FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
            WHERE task_scene = 'STYLE_ME'
                AND user_id IS NOT NULL
                AND CAST(user_id AS STRING) != ''
                AND DATE({service_ts}, '{TORONTO_TZ}') IS NOT NULL
        )
        SELECT DISTINCT dt
        FROM (
            SELECT dt FROM event_dates
            UNION DISTINCT
            SELECT dt FROM styleme_dates
        )
        WHERE dt IS NOT NULL
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    WITH
    -- 最近 6 小时事件更新映射出的业务日。
    changed_event_dates AS (
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS source_dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND event_name IN ({event_names_sql})
            AND prop_user_id IS NOT NULL
            AND CAST(prop_user_id AS STRING) != ''
            AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
    ),
    -- 最近 6 小时 Style Me 服务端任务更新映射出的业务日。
    changed_styleme_dates AS (
        SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS source_dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE {service_change_ts} >= TIMESTAMP_SUB(
                CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR
            )
            AND task_scene = 'STYLE_ME'
            AND user_id IS NOT NULL
            AND CAST(user_id AS STRING) != ''
    ),
    -- 最近 6 小时 dws_user_daily 活跃变化业务日（驱动留存回补）。
    changed_user_dates AS (
        SELECT DISTINCT dt AS source_dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND dt IS NOT NULL
    ),
    -- 当天行为变化直接重刷当天分区。
    direct_dates AS (
        SELECT source_dt AS dt
        FROM changed_event_dates
        UNION DISTINCT
        SELECT source_dt AS dt
        FROM changed_styleme_dates
    ),
    -- 次留 / 7 留 / 30 留依赖未来活跃变化，需要把未来活跃回推到更早 cohort 日期。
    retention_dates AS (
        SELECT DATE_SUB(source_dt, INTERVAL 1 DAY) AS dt
        FROM changed_user_dates
        UNION DISTINCT
        SELECT DATE_SUB(source_dt, INTERVAL 7 DAY) AS dt
        FROM changed_user_dates
        UNION DISTINCT
        SELECT DATE_SUB(source_dt, INTERVAL 30 DAY) AS dt
        FROM changed_user_dates
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt FROM direct_dates
        UNION DISTINCT
        SELECT dt FROM retention_dates
    )
    WHERE dt IS NOT NULL
        AND dt <= CURRENT_DATE('{TORONTO_TZ}')
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_ai_fashion_styleme_daily(dates: list[date]) -> None:
    """
    产出 AI-Fashion Style Me 日事实层。

    粒度：
    - dt

    指标：
    - Style Me 用户留存（次日 / 第 7 日 / 第 30 日）
    - 三场景（Quick Mode / Plus Item / Plus Outfit）的弹窗曝光、Try it 点击、
      任务成功数、Add to Closet、Add to Closet & Calendar、结果成功页曝光
    """
    if not dates:
        logging.info("没有新的 dws_ai_fashion_styleme_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    event_names_sql = string_values_to_sql_list(STYLEME_EVENT_NAMES)
    event_ts = event_ts_expr()
    service_ts = service_ts_expr()
    min_dt = min(dates)
    max_dt = max(dates)

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        styleme_user_uv,
        styleme_next_day_retained_uv,
        styleme_next_day_not_retained_uv,
        is_next_day_observation_started,
        styleme_day_7_retained_uv,
        styleme_day_7_not_retained_uv,
        is_day_7_observation_started,
        styleme_day_30_retained_uv,
        styleme_day_30_not_retained_uv,
        is_day_30_observation_started,
        popup_exposure_uv,
        quick_try_btn_uv,
        plus_item_try_btn_uv,
        plus_outfit_try_btn_uv,
        quick_task_success_count,
        plus_item_task_success_count,
        plus_outfit_task_success_count,
        quick_add_closet_uv,
        plus_item_add_closet_uv,
        plus_outfit_add_closet_uv,
        quick_add_closet_calendar_uv,
        plus_item_add_closet_calendar_uv,
        plus_outfit_add_closet_calendar_uv,
        quick_result_success_uv,
        plus_result_success_uv,
        update_time
    )
    WITH
    -- 目标日期骨架。
    date_spine AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 事件基础层：统一 Toronto 切天，提取 Style Me 相关字段。
    dwd_base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            event_name,
            CAST(prop_user_id AS STRING) AS user_id,
            JSON_VALUE(args, '$.button_type') AS button_type,
            JSON_VALUE(args, '$.task_id') AS task_id,
            CAST(args_page_key AS STRING) AS page_key
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE(logAt_timestamp) BETWEEN DATE_SUB(DATE '{min_dt}', INTERVAL 1 DAY)
            AND DATE_ADD(DATE '{max_dt}', INTERVAL 1 DAY)
            AND DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND event_name IN ({event_names_sql})
            AND prop_user_id IS NOT NULL
            AND CAST(prop_user_id AS STRING) != ''
    ),
    -- Style Me 服务端任务基础层。
    styleme_tasks AS (
        SELECT
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            CAST(task_id AS STRING) AS task_id,
            task_type,
            status,
            CAST(user_id AS STRING) AS user_id,
            IFNULL(
                ARRAY_LENGTH(JSON_QUERY_ARRAY(request_param, '$.referenceProducts')),
                0
            ) AS reference_products_count
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE task_scene = 'STYLE_ME'
            AND DATE({service_ts}, '{TORONTO_TZ}')
                BETWEEN DATE_SUB(DATE '{min_dt}', INTERVAL 30 DAY)
                AND DATE_ADD(DATE '{max_dt}', INTERVAL 1 DAY)
    ),
    -- Style Me 用户群组定义：(1) c_styleme_try_btn 事件用户；
    --                        (2) v_styleme_page 事件且当日有 STYLE_ME 任务的用户。
    styleme_cohort AS (
        SELECT DISTINCT dt, user_id
        FROM (
            SELECT dt, user_id
            FROM dwd_base
            WHERE event_name = 'c_styleme_try_btn'
            UNION DISTINCT
            SELECT e.dt, e.user_id
            FROM dwd_base e
            WHERE e.event_name = 'v_styleme_page'
                AND EXISTS (
                    SELECT 1 FROM styleme_tasks t
                    WHERE t.user_id = e.user_id AND t.dt = e.dt
                )
        )
    ),
    -- 目标日期向后 30 天内的活跃登录用户集合，用于留存判定。
    future_active_users AS (
        SELECT DISTINCT
            dt,
            prop_user_id AS user_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        WHERE dt BETWEEN DATE_ADD(DATE '{min_dt}', INTERVAL 1 DAY)
            AND LEAST(
                DATE_ADD(DATE '{max_dt}', INTERVAL 30 DAY),
                CURRENT_DATE('{TORONTO_TZ}')
            )
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
    ),
    -- 留存聚合：按 cohort 日期统计用户数及各留存窗口回访数。
    --   观察窗口未到位时输出 NULL，避免 ADS 误算 0% 留存。
    retention_daily AS (
        SELECT
            c.dt,
            COUNT(DISTINCT c.user_id) AS styleme_user_uv,
            DATE_ADD(c.dt, INTERVAL 1 DAY) <= CURRENT_DATE('{TORONTO_TZ}')
                AS is_next_day_observation_started,
            DATE_ADD(c.dt, INTERVAL 7 DAY) <= CURRENT_DATE('{TORONTO_TZ}')
                AS is_day_7_observation_started,
            DATE_ADD(c.dt, INTERVAL 30 DAY) <= CURRENT_DATE('{TORONTO_TZ}')
                AS is_day_30_observation_started,
            CASE WHEN DATE_ADD(c.dt, INTERVAL 1 DAY) <= CURRENT_DATE('{TORONTO_TZ}')
                 THEN COUNT(DISTINCT IF(n1.user_id IS NOT NULL, c.user_id, NULL))
                 ELSE NULL END
                AS styleme_next_day_retained_uv,
            CASE WHEN DATE_ADD(c.dt, INTERVAL 1 DAY) <= CURRENT_DATE('{TORONTO_TZ}')
                 THEN COUNT(DISTINCT c.user_id)
                      - COUNT(DISTINCT IF(n1.user_id IS NOT NULL, c.user_id, NULL))
                 ELSE NULL END
                AS styleme_next_day_not_retained_uv,
            CASE WHEN DATE_ADD(c.dt, INTERVAL 7 DAY) <= CURRENT_DATE('{TORONTO_TZ}')
                 THEN COUNT(DISTINCT IF(n7.user_id IS NOT NULL, c.user_id, NULL))
                 ELSE NULL END
                AS styleme_day_7_retained_uv,
            CASE WHEN DATE_ADD(c.dt, INTERVAL 7 DAY) <= CURRENT_DATE('{TORONTO_TZ}')
                 THEN COUNT(DISTINCT c.user_id)
                      - COUNT(DISTINCT IF(n7.user_id IS NOT NULL, c.user_id, NULL))
                 ELSE NULL END
                AS styleme_day_7_not_retained_uv,
            CASE WHEN DATE_ADD(c.dt, INTERVAL 30 DAY) <= CURRENT_DATE('{TORONTO_TZ}')
                 THEN COUNT(DISTINCT IF(n30.user_id IS NOT NULL, c.user_id, NULL))
                 ELSE NULL END
                AS styleme_day_30_retained_uv,
            CASE WHEN DATE_ADD(c.dt, INTERVAL 30 DAY) <= CURRENT_DATE('{TORONTO_TZ}')
                 THEN COUNT(DISTINCT c.user_id)
                      - COUNT(DISTINCT IF(n30.user_id IS NOT NULL, c.user_id, NULL))
                 ELSE NULL END
                AS styleme_day_30_not_retained_uv
        FROM styleme_cohort c
        LEFT JOIN future_active_users n1
            ON n1.user_id = c.user_id
            AND n1.dt = DATE_ADD(c.dt, INTERVAL 1 DAY)
        LEFT JOIN future_active_users n7
            ON n7.user_id = c.user_id
            AND n7.dt = DATE_ADD(c.dt, INTERVAL 7 DAY)
        LEFT JOIN future_active_users n30
            ON n30.user_id = c.user_id
            AND n30.dt = DATE_ADD(c.dt, INTERVAL 30 DAY)
        GROUP BY c.dt
    ),
    -- 场景事件指标：弹窗曝光、Try it 按钮点击、结果成功页曝光。
    event_metric_daily AS (
        SELECT
            dt,
            COUNT(DISTINCT IF(
                event_name = 'v_styleme_popup',
                user_id, NULL
            )) AS popup_exposure_uv,
            COUNT(DISTINCT IF(
                event_name = 'c_styleme_try_btn' AND button_type = 'quick_signle',
                user_id, NULL
            )) AS quick_try_btn_uv,
            COUNT(DISTINCT IF(
                event_name = 'c_styleme_try_btn' AND button_type = 'plus_single',
                user_id, NULL
            )) AS plus_item_try_btn_uv,
            COUNT(DISTINCT IF(
                event_name = 'c_styleme_try_btn' AND button_type = 'plus_outfit',
                user_id, NULL
            )) AS plus_outfit_try_btn_uv,
            COUNT(DISTINCT IF(
                event_name = 'v_result_success' AND page_key = 'quick_result',
                user_id, NULL
            )) AS quick_result_success_uv,
            COUNT(DISTINCT IF(
                event_name = 'v_result_success' AND page_key = 'plus_result',
                user_id, NULL
            )) AS plus_result_success_uv
        FROM dwd_base
        GROUP BY dt
    ),
    -- 服务端任务成功数：按场景分类统计当日成功任务数。
    task_success_daily AS (
        SELECT
            dt,
            COUNT(DISTINCT IF(
                task_type = 'QUICK_TRY_ON' AND status = 'SUCCESS',
                task_id, NULL
            )) AS quick_task_success_count,
            COUNT(DISTINCT IF(
                reference_products_count > 0 AND status = 'SUCCESS',
                task_id, NULL
            )) AS plus_item_task_success_count,
            COUNT(DISTINCT IF(
                reference_products_count = 0 AND task_type != 'QUICK_TRY_ON'
                AND status = 'SUCCESS',
                task_id, NULL
            )) AS plus_outfit_task_success_count
        FROM styleme_tasks
        WHERE dt IN (SELECT dt FROM date_spine)
        GROUP BY dt
    ),
    -- Add to Closet / Calendar 事件与服务端任务关联，按场景分类统计 UV。
    closet_event_daily AS (
        SELECT
            e.dt,
            COUNT(DISTINCT IF(
                e.event_name = 'c_add_to_closet'
                AND t.task_type = 'QUICK_TRY_ON',
                e.user_id, NULL
            )) AS quick_add_closet_uv,
            COUNT(DISTINCT IF(
                e.event_name = 'c_add_to_closet'
                AND t.reference_products_count > 0,
                e.user_id, NULL
            )) AS plus_item_add_closet_uv,
            COUNT(DISTINCT IF(
                e.event_name = 'c_add_to_closet'
                AND t.reference_products_count = 0
                AND t.task_type != 'QUICK_TRY_ON',
                e.user_id, NULL
            )) AS plus_outfit_add_closet_uv,
            COUNT(DISTINCT IF(
                e.event_name = 'c_add_to_closet_calendar'
                AND t.task_type = 'QUICK_TRY_ON',
                e.user_id, NULL
            )) AS quick_add_closet_calendar_uv,
            COUNT(DISTINCT IF(
                e.event_name = 'c_add_to_closet_calendar'
                AND t.reference_products_count > 0,
                e.user_id, NULL
            )) AS plus_item_add_closet_calendar_uv,
            COUNT(DISTINCT IF(
                e.event_name = 'c_add_to_closet_calendar'
                AND t.reference_products_count = 0
                AND t.task_type != 'QUICK_TRY_ON',
                e.user_id, NULL
            )) AS plus_outfit_add_closet_calendar_uv
        FROM dwd_base e
        INNER JOIN styleme_tasks t
            ON e.task_id = t.task_id
        WHERE e.event_name IN ('c_add_to_closet', 'c_add_to_closet_calendar')
            AND e.task_id IS NOT NULL
        GROUP BY e.dt
    )
    SELECT
        d.dt,
        COALESCE(r.styleme_user_uv, 0) AS styleme_user_uv,
        r.styleme_next_day_retained_uv,
        r.styleme_next_day_not_retained_uv,
        COALESCE(r.is_next_day_observation_started, FALSE) AS is_next_day_observation_started,
        r.styleme_day_7_retained_uv,
        r.styleme_day_7_not_retained_uv,
        COALESCE(r.is_day_7_observation_started, FALSE) AS is_day_7_observation_started,
        r.styleme_day_30_retained_uv,
        r.styleme_day_30_not_retained_uv,
        COALESCE(r.is_day_30_observation_started, FALSE) AS is_day_30_observation_started,
        COALESCE(e.popup_exposure_uv, 0) AS popup_exposure_uv,
        COALESCE(e.quick_try_btn_uv, 0) AS quick_try_btn_uv,
        COALESCE(e.plus_item_try_btn_uv, 0) AS plus_item_try_btn_uv,
        COALESCE(e.plus_outfit_try_btn_uv, 0) AS plus_outfit_try_btn_uv,
        COALESCE(ts.quick_task_success_count, 0) AS quick_task_success_count,
        COALESCE(ts.plus_item_task_success_count, 0) AS plus_item_task_success_count,
        COALESCE(ts.plus_outfit_task_success_count, 0) AS plus_outfit_task_success_count,
        COALESCE(c.quick_add_closet_uv, 0) AS quick_add_closet_uv,
        COALESCE(c.plus_item_add_closet_uv, 0) AS plus_item_add_closet_uv,
        COALESCE(c.plus_outfit_add_closet_uv, 0) AS plus_outfit_add_closet_uv,
        COALESCE(c.quick_add_closet_calendar_uv, 0) AS quick_add_closet_calendar_uv,
        COALESCE(c.plus_item_add_closet_calendar_uv, 0) AS plus_item_add_closet_calendar_uv,
        COALESCE(c.plus_outfit_add_closet_calendar_uv, 0) AS plus_outfit_add_closet_calendar_uv,
        COALESCE(e.quick_result_success_uv, 0) AS quick_result_success_uv,
        COALESCE(e.plus_result_success_uv, 0) AS plus_result_success_uv,
        CURRENT_TIMESTAMP() AS update_time
    FROM date_spine d
    LEFT JOIN retention_daily r USING (dt)
    LEFT JOIN event_metric_daily e USING (dt)
    LEFT JOIN task_success_daily ts USING (dt)
    LEFT JOIN closet_event_daily c USING (dt)
    ORDER BY d.dt;
    """
    logging.info("开始处理: dws_ai_fashion_styleme_daily")
    job = client.query(query)
    job.result()
    logging.info("dws_ai_fashion_styleme_daily 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 dws_ai_fashion_styleme_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_ai_fashion_styleme_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dws_ai_fashion_styleme_daily 完成, 耗时: %.1f 秒", elapsed)
