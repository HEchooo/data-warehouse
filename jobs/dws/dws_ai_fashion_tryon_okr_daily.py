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
TABLE_NAME = "dws_ai_fashion_tryon_okr_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6

POST_SCENE_EXPOSURE_EVENTS = (
    "v_star_post_feeds",
    "v_star_post_detail",
    "v_magazine_post_detail",
    "v_magazine_post_feeds",
    "v_brand_post_feeds",
    "v_brand_post_detail",
    "v_kol_post_feeds",
    "v_kol_post_detail",
)
TRYON_TASK_TYPES = (
    "QUICK_TRY_ON",
    "TRY_ON",
    "OUTFIT",
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


def service_ts_expr(field_name: str = "gmt_create") -> str:
    """v3 服务表的 gmt_create 是 UTC DATETIME，这里统一转成 TIMESTAMP。"""
    return f"TIMESTAMP({field_name}, 'UTC')"


def get_ai_fashion_tryon_okr_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    AI-Fashion 试穿 OKR DWS 依赖事件行为、试穿服务端行为和 styleme 服务端行为。

    规则：
    - 目标表不存在或为空时，按事件与服务端行为历史业务日全量初始化
    - 目标表已存在时，只取最近 6 小时事件、试穿或 styleme 行为变更映射出的业务日
    """
    event_ts = event_ts_expr()
    service_ts = service_ts_expr()
    tryon_task_types_sql = string_values_to_sql_list(TRYON_TASK_TYPES)
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        WITH
        -- 事件历史业务日。
        event_dates AS (
            SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
            FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
            WHERE prop_user_id IS NOT NULL
                AND CAST(prop_user_id AS STRING) != ''
                AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
        ),
        -- 服务端试穿历史业务日。
        tryon_dates AS (
            SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS dt
            FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
            WHERE task_type IN ({tryon_task_types_sql})
                AND DATE({service_ts}, '{TORONTO_TZ}') IS NOT NULL
        ),
        -- styleme 历史业务日。
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
            SELECT dt FROM tryon_dates
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
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND prop_user_id IS NOT NULL
            AND CAST(prop_user_id AS STRING) != ''
            AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
    ),
    -- 最近 6 小时试穿服务端行为更新映射出的业务日。
    changed_tryon_dates AS (
        SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE {service_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND task_type IN ({tryon_task_types_sql})
            AND DATE({service_ts}, '{TORONTO_TZ}') IS NOT NULL
    ),
    -- 最近 6 小时 styleme 行为更新映射出的业务日。
    changed_styleme_dates AS (
        SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE {service_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND task_scene = 'STYLE_ME'
            AND user_id IS NOT NULL
            AND CAST(user_id AS STRING) != ''
            AND DATE({service_ts}, '{TORONTO_TZ}') IS NOT NULL
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt FROM changed_event_dates
        UNION DISTINCT
        SELECT dt FROM changed_tryon_dates
        UNION DISTINCT
        SELECT dt FROM changed_styleme_dates
    )
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_ai_fashion_tryon_okr_daily(dates: list[date]) -> None:
    """
    产出 AI-Fashion 试穿 OKR 日事实层。

    粒度：
    - dt

    指标：
    - 新老用户 DAU 与服务端试穿 UV
    - 首页/商详试穿 UV 及其分母 UV
    - 帖子服务端试穿 UV 及帖子曝光分母 UV
    - styleme 试穿 UV
    - 服务端试穿 UV 与 PV（用于人均每日试穿次数分布）
    - 前端 tryon 点击 UV、商城/衣橱试穿 UV、styleme 曝光 UV
    - quick / plus 试穿任务数、成功任务数、成功任务总时长
    - 全试穿成功任务有效时长分子分母（用于周级 AI 试穿时长分布）
    - 商品点击/曝光 UV 与分场景分子分母
    - 商品点击 PV(raw_event_id 去重，用于人均每日点击次数分布)
    """
    if not dates:
        logging.info("没有新的 dws_ai_fashion_tryon_okr_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    post_events_sql = string_values_to_sql_list(POST_SCENE_EXPOSURE_EVENTS)
    tryon_task_types_sql = string_values_to_sql_list(TRYON_TASK_TYPES)
    event_ts = event_ts_expr()
    service_ts = service_ts_expr()

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        dau_uv_total,
        dau_new_uv,
        dau_old_uv,
        tryon_new_uv,
        tryon_old_uv,
        home_tryon_uv,
        home_feeds_uv,
        post_tryon_uv,
        post_scene_exposure_uv,
        product_detail_tryon_uv,
        product_detail_uv,
        styleme_uv,
        front_tryon_click_uv,
        shop_tryon_uv,
        closet_tryon_uv,
        closet_feeds_uv,
        styleme_exposure_uv,
        all_tryon_success_duration_task_count,
        all_tryon_success_total_duration_ms,
        quick_tryon_task_total_count,
        quick_tryon_task_success_count,
        quick_tryon_success_duration_task_count,
        quick_tryon_success_total_duration_ms,
        plus_tryon_task_total_count,
        plus_tryon_task_success_count,
        plus_tryon_success_duration_task_count,
        plus_tryon_success_total_duration_ms,
        product_click_uv,
        product_exposure_uv,
        home_product_click_uv,
        home_product_exposure_uv,
        tryon_result_click_uv,
        tryon_result_exposure_uv,
        shop_click_uv,
        shop_exposure_uv,
        tryon_uv_total,
        tryon_pv,
        product_click_pv_raw,
        update_time
    )
    WITH
    -- 目标日期骨架：保证目标日期即使没有事件也会产出 0 值行。
    date_spine AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 新老用户首活维表：用于新老用户分群。
    user_first_active AS (
        SELECT
            CAST(prop_user_id AS STRING) AS user_id,
            first_active_date
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_user_first_active`
        WHERE prop_user_id IS NOT NULL
            AND CAST(prop_user_id AS STRING) != ''
    ),
    -- 事件基础层：统一 Toronto 切天、统一 user_id，并保留后续计算所需字段。
    dwd_base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            event_name,
            CAST(prop_user_id AS STRING) AS user_id,
            CAST(args_page_key AS STRING) AS args_page_key,
            NULLIF(product_code, '') AS product_code,
            raw_event_id,
            hash_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND prop_user_id IS NOT NULL
            AND CAST(prop_user_id AS STRING) != ''
    ),
    -- 每日活跃用户 UV（DAU）。
    active_users_daily AS (
        SELECT
            dt,
            COUNT(DISTINCT user_id) AS dau_uv_total
        FROM dwd_base
        GROUP BY dt
    ),
    -- 服务端试穿任务基础层：三种试穿任务统一作为整体试穿用户与次数口径。
    service_tryon_task_base AS (
        SELECT
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            CAST(task_id AS STRING) AS task_id,
            task_type,
            status,
            NULLIF(JSON_VALUE(request_param, '$.postId'), '') AS post_id,
            SAFE_CAST(JSON_VALUE(output, '$.totalDuration') AS INT64) AS total_duration_ms,
            COALESCE(
                NULLIF(JSON_VALUE(request_param, '$.user_id'), ''),
                NULLIF(JSON_VALUE(request_param, '$.userId'), ''),
                NULLIF(CAST(user_id AS STRING), '')
            ) AS user_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE DATE({service_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND task_type IN ({tryon_task_types_sql})
    ),
    -- 服务端试穿日聚合：产出整体试穿 PV 与 UV，供人均试穿次数分布使用。
    service_tryon_daily AS (
        SELECT
            dt,
            COUNT(1) AS tryon_pv,
            COUNT(DISTINCT IF(user_id IS NOT NULL, user_id, NULL)) AS tryon_uv_total
        FROM service_tryon_task_base
        GROUP BY dt
    ),
    -- quick / plus 试穿任务日聚合：沉淀成功率与平均完成时长所需分子分母。
    service_tryon_quality_daily AS (
        SELECT
            dt,
            COUNT(DISTINCT IF(
                status = 'SUCCESS'
                AND total_duration_ms IS NOT NULL,
                task_id,
                NULL
            )) AS all_tryon_success_duration_task_count,
            SUM(IF(
                status = 'SUCCESS',
                COALESCE(total_duration_ms, 0),
                0
            )) AS all_tryon_success_total_duration_ms,
            COUNT(DISTINCT IF(task_type = 'QUICK_TRY_ON', task_id, NULL))
                AS quick_tryon_task_total_count,
            COUNT(DISTINCT IF(
                task_type = 'QUICK_TRY_ON' AND status = 'SUCCESS',
                task_id,
                NULL
            )) AS quick_tryon_task_success_count,
            COUNT(DISTINCT IF(
                task_type = 'QUICK_TRY_ON'
                AND status = 'SUCCESS'
                AND total_duration_ms IS NOT NULL,
                task_id,
                NULL
            )) AS quick_tryon_success_duration_task_count,
            SUM(IF(
                task_type = 'QUICK_TRY_ON' AND status = 'SUCCESS',
                COALESCE(total_duration_ms, 0),
                0
            )) AS quick_tryon_success_total_duration_ms,
            COUNT(DISTINCT IF(task_type = 'TRY_ON', task_id, NULL))
                AS plus_tryon_task_total_count,
            COUNT(DISTINCT IF(
                task_type = 'TRY_ON' AND status = 'SUCCESS',
                task_id,
                NULL
            )) AS plus_tryon_task_success_count,
            COUNT(DISTINCT IF(
                task_type = 'TRY_ON'
                AND status = 'SUCCESS'
                AND total_duration_ms IS NOT NULL,
                task_id,
                NULL
            )) AS plus_tryon_success_duration_task_count,
            SUM(IF(
                task_type = 'TRY_ON' AND status = 'SUCCESS',
                COALESCE(total_duration_ms, 0),
                0
            )) AS plus_tryon_success_total_duration_ms
        FROM service_tryon_task_base
        GROUP BY dt
    ),
    -- 帖子维表：用于把服务端 postId 试穿任务归因到内容帖子。
    post_dim AS (
        SELECT
            CAST(post_id AS STRING) AS post_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_post`
        WHERE post_id IS NOT NULL
            AND post_code IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY CAST(post_id AS STRING)
            ORDER BY post_code
        ) = 1
    ),
    -- 服务端帖子试穿日聚合：只统计带 postId 且能映射到帖子维表的试穿用户。
    service_post_tryon_daily AS (
        SELECT
            t.dt,
            COUNT(DISTINCT IF(t.user_id IS NOT NULL, t.user_id, NULL)) AS post_tryon_uv
        FROM service_tryon_task_base t
        JOIN post_dim p
            ON t.post_id = p.post_id
        WHERE t.post_id IS NOT NULL
        GROUP BY t.dt
    ),
    -- 新老用户日聚合：同时输出新老用户 DAU 与试穿 UV。
    user_group_daily AS (
        SELECT
            a.dt,
            COUNT(DISTINCT IF(f.first_active_date = a.dt, a.user_id, NULL)) AS dau_new_uv,
            COUNT(DISTINCT IF(f.first_active_date < a.dt, a.user_id, NULL)) AS dau_old_uv,
            COUNT(DISTINCT IF(
                f.first_active_date = a.dt AND t.user_id IS NOT NULL,
                a.user_id,
                NULL
            )) AS tryon_new_uv,
            COUNT(DISTINCT IF(
                f.first_active_date < a.dt AND t.user_id IS NOT NULL,
                a.user_id,
                NULL
            )) AS tryon_old_uv
        FROM (
            SELECT DISTINCT dt, user_id
            FROM dwd_base
        ) a
        LEFT JOIN user_first_active f
            ON f.user_id = a.user_id
        LEFT JOIN service_tryon_task_base t
            ON t.dt = a.dt
            AND t.user_id = a.user_id
        GROUP BY a.dt
    ),
    -- 每日事件指标：保留依赖页面场景的首页/商详试穿和曝光、商品点击等前端行为字段。
    event_metric_daily AS (
        SELECT
            dt,
            COUNT(DISTINCT IF(
                event_name = 'c_tryon' AND args_page_key = 'home_feeds',
                user_id,
                NULL
            )) AS home_tryon_uv,
            COUNT(DISTINCT IF(event_name = 'v_home_feeds', user_id, NULL)) AS home_feeds_uv,
            COUNT(DISTINCT IF(event_name IN ({post_events_sql}), user_id, NULL))
                AS post_scene_exposure_uv,
            COUNT(DISTINCT IF(
                event_name = 'c_tryon' AND args_page_key = 'product_detail',
                user_id,
                NULL
            )) AS product_detail_tryon_uv,
            COUNT(DISTINCT IF(event_name = 'v_product_detail', user_id, NULL)) AS product_detail_uv,
            COUNT(DISTINCT IF(event_name = 'c_tryon', user_id, NULL)) AS front_tryon_click_uv,
            COUNT(DISTINCT IF(
                event_name = 'c_tryon' AND args_page_key = 'shop_feeds',
                user_id,
                NULL
            )) AS shop_tryon_uv,
            COUNT(DISTINCT IF(
                event_name = 'c_tryon' AND args_page_key = 'closet_feeds',
                user_id,
                NULL
            )) AS closet_tryon_uv,
            COUNT(DISTINCT IF(event_name = 'v_closet_feeds', user_id, NULL)) AS closet_feeds_uv,
            COUNT(DISTINCT IF(
                event_name IN ('v_styleme_popup', 'v_styleme_page'),
                user_id,
                NULL
            )) AS styleme_exposure_uv,
            COUNT(DISTINCT IF(
                STARTS_WITH(event_name, 'c') AND product_code IS NOT NULL,
                user_id,
                NULL
            )) AS product_click_uv,
            COUNT(DISTINCT IF(
                STARTS_WITH(event_name, 'v') AND product_code IS NOT NULL,
                user_id,
                NULL
            )) AS product_exposure_uv,
            COUNT(DISTINCT IF(
                event_name = 'c_home_feeds' AND product_code IS NOT NULL,
                user_id,
                NULL
            )) AS home_product_click_uv,
            COUNT(DISTINCT IF(
                event_name = 'v_home_feeds' AND product_code IS NOT NULL,
                user_id,
                NULL
            )) AS home_product_exposure_uv,
            COUNT(DISTINCT IF(event_name = 'c_tryon_result_list', user_id, NULL))
                AS tryon_result_click_uv,
            COUNT(DISTINCT IF(
                event_name IN ('v_tryon_result_detail', 'v_tryon_result_list'),
                user_id,
                NULL
            )) AS tryon_result_exposure_uv,
            COUNT(DISTINCT IF(event_name = 'c_shop_feeds', user_id, NULL)) AS shop_click_uv,
            COUNT(DISTINCT IF(event_name = 'v_shop_feeds', user_id, NULL)) AS shop_exposure_uv,
            COUNT(DISTINCT IF(
                STARTS_WITH(event_name, 'c') AND product_code IS NOT NULL,
                raw_event_id,
                NULL
            )) AS product_click_pv_raw
        FROM dwd_base
        GROUP BY dt
    ),
    -- styleme 日 UV：按服务端 gmt_create 切 Toronto 日聚合。
    styleme_daily AS (
        SELECT
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            COUNT(DISTINCT CAST(user_id AS STRING)) AS styleme_uv
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE DATE({service_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND task_scene = 'STYLE_ME'
            AND user_id IS NOT NULL
            AND CAST(user_id AS STRING) != ''
        GROUP BY dt
    )
    SELECT
        d.dt,
        COALESCE(a.dau_uv_total, 0) AS dau_uv_total,
        COALESCE(g.dau_new_uv, 0) AS dau_new_uv,
        COALESCE(g.dau_old_uv, 0) AS dau_old_uv,
        COALESCE(g.tryon_new_uv, 0) AS tryon_new_uv,
        COALESCE(g.tryon_old_uv, 0) AS tryon_old_uv,
        COALESCE(e.home_tryon_uv, 0) AS home_tryon_uv,
        COALESCE(e.home_feeds_uv, 0) AS home_feeds_uv,
        COALESCE(pt.post_tryon_uv, 0) AS post_tryon_uv,
        COALESCE(e.post_scene_exposure_uv, 0) AS post_scene_exposure_uv,
        COALESCE(e.product_detail_tryon_uv, 0) AS product_detail_tryon_uv,
        COALESCE(e.product_detail_uv, 0) AS product_detail_uv,
        COALESCE(s.styleme_uv, 0) AS styleme_uv,
        COALESCE(e.front_tryon_click_uv, 0) AS front_tryon_click_uv,
        COALESCE(e.shop_tryon_uv, 0) AS shop_tryon_uv,
        COALESCE(e.closet_tryon_uv, 0) AS closet_tryon_uv,
        COALESCE(e.closet_feeds_uv, 0) AS closet_feeds_uv,
        COALESCE(e.styleme_exposure_uv, 0) AS styleme_exposure_uv,
        COALESCE(q.all_tryon_success_duration_task_count, 0)
            AS all_tryon_success_duration_task_count,
        COALESCE(q.all_tryon_success_total_duration_ms, 0)
            AS all_tryon_success_total_duration_ms,
        COALESCE(q.quick_tryon_task_total_count, 0) AS quick_tryon_task_total_count,
        COALESCE(q.quick_tryon_task_success_count, 0) AS quick_tryon_task_success_count,
        COALESCE(q.quick_tryon_success_duration_task_count, 0)
            AS quick_tryon_success_duration_task_count,
        COALESCE(q.quick_tryon_success_total_duration_ms, 0)
            AS quick_tryon_success_total_duration_ms,
        COALESCE(q.plus_tryon_task_total_count, 0) AS plus_tryon_task_total_count,
        COALESCE(q.plus_tryon_task_success_count, 0) AS plus_tryon_task_success_count,
        COALESCE(q.plus_tryon_success_duration_task_count, 0)
            AS plus_tryon_success_duration_task_count,
        COALESCE(q.plus_tryon_success_total_duration_ms, 0)
            AS plus_tryon_success_total_duration_ms,
        COALESCE(e.product_click_uv, 0) AS product_click_uv,
        COALESCE(e.product_exposure_uv, 0) AS product_exposure_uv,
        COALESCE(e.home_product_click_uv, 0) AS home_product_click_uv,
        COALESCE(e.home_product_exposure_uv, 0) AS home_product_exposure_uv,
        COALESCE(e.tryon_result_click_uv, 0) AS tryon_result_click_uv,
        COALESCE(e.tryon_result_exposure_uv, 0) AS tryon_result_exposure_uv,
        COALESCE(e.shop_click_uv, 0) AS shop_click_uv,
        COALESCE(e.shop_exposure_uv, 0) AS shop_exposure_uv,
        COALESCE(st.tryon_uv_total, 0) AS tryon_uv_total,
        COALESCE(st.tryon_pv, 0) AS tryon_pv,
        COALESCE(e.product_click_pv_raw, 0) AS product_click_pv_raw,
        CURRENT_TIMESTAMP() AS update_time
    FROM date_spine d
    LEFT JOIN active_users_daily a USING (dt)
    LEFT JOIN user_group_daily g USING (dt)
    LEFT JOIN event_metric_daily e USING (dt)
    LEFT JOIN service_tryon_daily st USING (dt)
    LEFT JOIN service_tryon_quality_daily q USING (dt)
    LEFT JOIN service_post_tryon_daily pt USING (dt)
    LEFT JOIN styleme_daily s USING (dt)
    ORDER BY d.dt;
    """
    logging.info("开始处理: dws_ai_fashion_tryon_okr_daily")
    job = client.query(query)
    job.result()
    logging.info("dws_ai_fashion_tryon_okr_daily 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_ai_fashion_tryon_okr_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 dws_ai_fashion_tryon_okr_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_ai_fashion_tryon_okr_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dws_ai_fashion_tryon_okr_daily 完成, 耗时: %.1f 秒", elapsed)
