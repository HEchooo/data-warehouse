"""DWS 日级素材用户事实层。

落地 ``decom.dws_asset_user_daily``，粒度：``dt × asset_code × user_id``。

依赖：
- decom.dwd_event_log（帖子详情页素材曝光、素材负反馈、素材下方商品点击埋点）
- v3_decom.common_ext_ai_task（素材试穿服务端任务）

硬性条件：所有 dwd_event_log 来源只统计 prop_user_id IS NOT NULL AND prop_user_id != '' 的数据。
"""

from datetime import date, datetime, timezone
import logging
import os
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
V3_DATASET_ID = "v3_decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_asset_user_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = int(os.environ.get("ASSET_RECOVERY_WINDOW_HOURS", "6"))
if INCREMENTAL_WINDOW_HOURS <= 0:
    raise ValueError("ASSET_RECOVERY_WINDOW_HOURS 必须是正整数")

# 帖子详情页素材曝光事件（asset-mapping.md 之外新增的详情页曝光口径）。
EXPOSURE_EVENT_NAMES = (
    "v_brand_post_detail",
    "v_magazine_post_detail",
    "v_star_post_detail",
)
# 素材负反馈埋点。
FEEDBACK_EVENT_NAMES = ("c_feedback_entry",)
# 素材下方商品点击埋点（args.asset_code 自 2026-07-25 起上报，标量字符串）。
# 注意与 dws_column_interaction_daily.py 的同名常量不是同一口径：那里是专栏级商品点击。
PRODUCT_CLICK_EVENT_NAMES = ("c_product",)
# 平台默认试穿任务类型（tryon-mapping.md）。
TRYON_TASK_TYPES = ("QUICK_TRY_ON", "TRY_ON", "OUTFIT")

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


def asset_codes_unnest_expr(json_field: str) -> str:
    """
    兼容 args.asset_code 为标量或数组两种形态，统一拆到 asset_code 粒度。
    """
    return (
        f"UNNEST(CASE WHEN JSON_TYPE({json_field}) = 'array' "
        f"THEN JSON_QUERY_ARRAY({json_field}) "
        f"ELSE [{json_field}] END)"
    )


def get_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    素材用户事实依赖帖子详情页曝光埋点、素材负反馈埋点和试穿服务端任务。

    规则：
    - 目标表不存在或为空：按上游历史业务日全量初始化
    - 目标表已存在：按最近变更窗口重刷当天分区
    """
    event_ts = event_ts_expr()
    service_ts = service_ts_expr()
    service_change_ts = service_change_ts_expr()
    exposure_event_names_sql = string_values_to_sql_list(EXPOSURE_EVENT_NAMES)
    feedback_event_names_sql = string_values_to_sql_list(FEEDBACK_EVENT_NAMES)
    product_click_event_names_sql = string_values_to_sql_list(PRODUCT_CLICK_EVENT_NAMES)
    tryon_task_types_sql = string_values_to_sql_list(TRYON_TASK_TYPES)
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        WITH
        -- 曝光 / 负反馈 / 商品点击事件历史业务日。
        event_dates AS (
            SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
            FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
            WHERE event_name IN (
                    {exposure_event_names_sql}, {feedback_event_names_sql},
                    {product_click_event_names_sql}
                )
                AND prop_user_id IS NOT NULL
                AND CAST(prop_user_id AS STRING) != ''
                AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
        ),
        -- 试穿服务端任务历史业务日。
        tryon_dates AS (
            SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS dt
            FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
            WHERE task_type IN ({tryon_task_types_sql})
                AND user_id IS NOT NULL
                AND CAST(user_id AS STRING) != ''
                AND JSON_VALUE(request_param, '$.assetCode') IS NOT NULL
                AND DATE({service_ts}, '{TORONTO_TZ}') IS NOT NULL
        )
        SELECT DISTINCT dt
        FROM (
            SELECT dt FROM event_dates
            UNION DISTINCT
            SELECT dt FROM tryon_dates
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
            AND event_name IN (
                {exposure_event_names_sql}, {feedback_event_names_sql},
                {product_click_event_names_sql}
            )
            AND prop_user_id IS NOT NULL
            AND CAST(prop_user_id AS STRING) != ''
            AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
    ),
    -- 最近 6 小时试穿服务端任务更新映射出的业务日。
    changed_tryon_dates AS (
        SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS source_dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE {service_change_ts} >= TIMESTAMP_SUB(
                CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR
            )
            AND task_type IN ({tryon_task_types_sql})
            AND user_id IS NOT NULL
            AND CAST(user_id AS STRING) != ''
            AND JSON_VALUE(request_param, '$.assetCode') IS NOT NULL
    )
    SELECT DISTINCT dt
    FROM (
        SELECT source_dt AS dt FROM changed_event_dates
        UNION DISTINCT
        SELECT source_dt AS dt FROM changed_tryon_dates
    )
    WHERE dt IS NOT NULL
        AND dt <= CURRENT_DATE('{TORONTO_TZ}')
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_asset_user_daily(dates: list[date]) -> None:
    """
    产出素材用户日事实层。

    粒度：
    - dt × asset_code × user_id

    指标：
    - 曝光 PV（按 raw_event_id 去重）
    - 试穿次数（按 task_id 去重）
    - 负反馈 PV（按 raw_event_id 去重）
    - 商品点击 PV（按 raw_event_id 去重）
    """
    if not dates:
        logging.info("没有新的 dws_asset_user_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    exposure_event_names_sql = string_values_to_sql_list(EXPOSURE_EVENT_NAMES)
    feedback_event_names_sql = string_values_to_sql_list(FEEDBACK_EVENT_NAMES)
    product_click_event_names_sql = string_values_to_sql_list(PRODUCT_CLICK_EVENT_NAMES)
    tryon_task_types_sql = string_values_to_sql_list(TRYON_TASK_TYPES)
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
        asset_code,
        user_id,
        exposure_pv,
        tryon_count,
        negative_feedback_pv,
        product_click_pv,
        update_time
    )
    WITH
    -- 帖子详情页素材曝光：v_brand/magazine/star_post_detail，只看已登录用户。
    exposure_base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            JSON_VALUE(asset_code_json) AS asset_code,
            CAST(prop_user_id AS STRING) AS user_id,
            raw_event_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`,
        {asset_codes_unnest_expr("args.asset_code")} AS asset_code_json
        WHERE DATE(logAt_timestamp) BETWEEN DATE_SUB(DATE '{min_dt}', INTERVAL 1 DAY)
            AND DATE_ADD(DATE '{max_dt}', INTERVAL 1 DAY)
            AND DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND event_name IN ({exposure_event_names_sql})
            AND prop_user_id IS NOT NULL
            AND CAST(prop_user_id AS STRING) != ''
            AND args.asset_code IS NOT NULL
    ),
    exposure_daily AS (
        SELECT
            dt,
            asset_code,
            user_id,
            COUNT(DISTINCT raw_event_id) AS exposure_pv
        FROM exposure_base
        WHERE asset_code IS NOT NULL AND asset_code != ''
        GROUP BY dt, asset_code, user_id
    ),
    -- 素材负反馈：c_feedback_entry，只看已登录用户。
    feedback_base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            JSON_VALUE(asset_code_json) AS asset_code,
            CAST(prop_user_id AS STRING) AS user_id,
            raw_event_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`,
        {asset_codes_unnest_expr("args.asset_code")} AS asset_code_json
        WHERE DATE(logAt_timestamp) BETWEEN DATE_SUB(DATE '{min_dt}', INTERVAL 1 DAY)
            AND DATE_ADD(DATE '{max_dt}', INTERVAL 1 DAY)
            AND DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND event_name IN ({feedback_event_names_sql})
            AND prop_user_id IS NOT NULL
            AND CAST(prop_user_id AS STRING) != ''
            AND args.asset_code IS NOT NULL
    ),
    feedback_daily AS (
        SELECT
            dt,
            asset_code,
            user_id,
            COUNT(DISTINCT raw_event_id) AS negative_feedback_pv
        FROM feedback_base
        WHERE asset_code IS NOT NULL AND asset_code != ''
        GROUP BY dt, asset_code, user_id
    ),
    -- 素材下方商品点击：c_product，只看已登录用户。
    product_click_base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            JSON_VALUE(asset_code_json) AS asset_code,
            CAST(prop_user_id AS STRING) AS user_id,
            raw_event_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`,
        {asset_codes_unnest_expr("args.asset_code")} AS asset_code_json
        WHERE DATE(logAt_timestamp) BETWEEN DATE_SUB(DATE '{min_dt}', INTERVAL 1 DAY)
            AND DATE_ADD(DATE '{max_dt}', INTERVAL 1 DAY)
            AND DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND event_name IN ({product_click_event_names_sql})
            AND prop_user_id IS NOT NULL
            AND CAST(prop_user_id AS STRING) != ''
            AND args.asset_code IS NOT NULL
    ),
    product_click_daily AS (
        SELECT
            dt,
            asset_code,
            user_id,
            COUNT(DISTINCT raw_event_id) AS product_click_pv
        FROM product_click_base
        WHERE asset_code IS NOT NULL AND asset_code != ''
        GROUP BY dt, asset_code, user_id
    ),
    -- 素材试穿：request_param.$.assetCode，限定平台默认试穿任务类型。
    tryon_daily AS (
        SELECT
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            JSON_VALUE(request_param, '$.assetCode') AS asset_code,
            CAST(user_id AS STRING) AS user_id,
            COUNT(DISTINCT task_id) AS tryon_count
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE task_type IN ({tryon_task_types_sql})
            AND user_id IS NOT NULL
            AND CAST(user_id AS STRING) != ''
            AND JSON_VALUE(request_param, '$.assetCode') IS NOT NULL
            AND DATE({service_ts}, '{TORONTO_TZ}') IN ({dates_str})
        GROUP BY dt, asset_code, user_id
    ),
    -- 目标日期骨架：确保四份来源的 dt × asset_code × user_id 都能对齐补 0。
    unioned_keys AS (
        SELECT dt, asset_code, user_id FROM exposure_daily
        UNION DISTINCT
        SELECT dt, asset_code, user_id FROM tryon_daily
        UNION DISTINCT
        SELECT dt, asset_code, user_id FROM feedback_daily
        UNION DISTINCT
        SELECT dt, asset_code, user_id FROM product_click_daily
    )
    SELECT
        k.dt,
        k.asset_code,
        k.user_id,
        COALESCE(e.exposure_pv, 0) AS exposure_pv,
        COALESCE(t.tryon_count, 0) AS tryon_count,
        COALESCE(f.negative_feedback_pv, 0) AS negative_feedback_pv,
        COALESCE(p.product_click_pv, 0) AS product_click_pv,
        CURRENT_TIMESTAMP() AS update_time
    FROM unioned_keys k
    LEFT JOIN exposure_daily e USING (dt, asset_code, user_id)
    LEFT JOIN tryon_daily t USING (dt, asset_code, user_id)
    LEFT JOIN feedback_daily f USING (dt, asset_code, user_id)
    LEFT JOIN product_click_daily p USING (dt, asset_code, user_id)
    ORDER BY k.dt, k.asset_code, k.user_id;
    """
    logging.info("开始处理: dws_asset_user_daily")
    job = client.query(query)
    job.result()
    logging.info("dws_asset_user_daily 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 dws_asset_user_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_asset_user_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dws_asset_user_daily 完成, 耗时: %.1f 秒", elapsed)
