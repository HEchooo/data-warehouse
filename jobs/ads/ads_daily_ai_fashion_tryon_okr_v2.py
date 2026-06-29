"""ADS 日级 AI-Fashion 试穿 / 商品 / 留存 OKR 宽表 v2。

落地 ``decom.ads_daily_ai_fashion_tryon_okr_v2``。

设计要点：
- 口径与周表 ``ads_weekly_ai_fashion_tryon_okr_v2`` 完全一致，仅把粒度从「周」改为「单日 dt」。
- 复用周表相同的 4 张 DWS 源 CTE，日粒度下：
    * 周内 UV 求和 / 天数 -> 当日 UV 值
    * 率指标 SUM(分子)/SUM(分母) -> 当日分子 / 当日分母
    * 留存观察未到位的天，留存率与留存数为 NULL（由 DWS is_*_observation_started 驱动）
- 已按需求去掉：三个分布图相关列、各 *_observed_day_count 列、week_* 维度列。
"""

from datetime import date, datetime, timedelta
import argparse
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_daily_ai_fashion_tryon_okr_v2"
TRYON_SOURCE_TABLE_NAME = "dws_ai_fashion_tryon_okr_daily"
USER_ACTIVITY_SOURCE_TABLE_NAME = "dws_user_activity_daily"
USER_DAILY_SOURCE_TABLE_NAME = "dws_user_daily"
RETENTION_FEATURE_SOURCE_TABLE_NAME = "dws_user_retention_feature_daily"
DATA_START_DATE = date(2026, 4, 27)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "ads" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6
TARGET_COLUMNS = (
    "dt",
    "new_user_tryon_rate",
    "new_user_tryon_uv",
    "old_user_tryon_rate",
    "old_user_tryon_uv",
    "home_tryon_uv",
    "home_tryon_penetration_rate",
    "post_tryon_uv",
    "post_tryon_penetration_rate",
    "product_detail_tryon_uv",
    "product_detail_tryon_penetration_rate",
    "styleme_tryon_uv",
    "platform_tryon_rate",
    "tryon_avg_count_per_tryon_user",
    "avg_daily_tryon_uv",
    "front_tryon_click_uv",
    "shop_feeds_tryon_penetration_rate",
    "shop_feeds_tryon_uv",
    "closet_feeds_tryon_penetration_rate",
    "closet_feeds_tryon_uv",
    "styleme_tryon_penetration_rate",
    "quick_tryon_success_rate",
    "plus_tryon_success_rate",
    "quick_tryon_avg_duration_min",
    "plus_tryon_avg_duration_min",
    "product_click_uv",
    "product_exposure_uv",
    "product_exposure_click_rate",
    "product_click_avg_count_per_click_user",
    "home_product_click_uv",
    "home_product_exposure_click_rate",
    "tryon_result_click_uv",
    "tryon_result_exposure_click_rate",
    "shop_click_uv",
    "shop_exposure_click_rate",
    "platform_dau",
    "next_day_retention_rate",
    "next_day_retained_user_count",
    "day_7_retention_rate",
    "day_7_retained_user_count",
    "day_30_retention_rate",
    "day_30_retained_user_count",
    "home_feeds_exposure_click_rate",
    "platform_avg_duration_min",
    "platform_avg_content_consume",
    "new_user_dau",
    "new_user_next_day_retention_rate",
    "new_user_next_day_retained_user_count",
    "new_user_avg_duration_min",
    "new_user_avg_content_consume",
    "closet_user_next_day_retention_rate",
    "closet_user_next_day_retained_user_count",
    "product_detail_user_next_day_retention_rate",
    "product_detail_user_next_day_retained_user_count",
    "ai_tryon_user_next_day_retention_rate",
    "ai_tryon_user_next_day_retained_user_count",
    "follow_user_next_day_retention_rate",
    "follow_user_next_day_retained_user_count",
    "model_user_next_day_retention_rate",
    "model_user_next_day_retained_user_count",
    "styleme_user_next_day_retention_rate",
    "styleme_user_next_day_retained_user_count",
    "update_time",
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


def target_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


def tryon_source_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{TRYON_SOURCE_TABLE_NAME}"


def user_activity_source_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{USER_ACTIVITY_SOURCE_TABLE_NAME}"


def user_daily_source_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{USER_DAILY_SOURCE_TABLE_NAME}"


def retention_feature_source_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{RETENTION_FEATURE_SOURCE_TABLE_NAME}"


def target_columns_sql(indent: str = "        ") -> str:
    return ",\n".join(f"{indent}{column}" for column in TARGET_COLUMNS)


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


def get_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """决定要重算哪些日期。

    - 目标表不存在或为空：按上游已落地的全部历史日全量初始化（>= DATA_START_DATE）
    - 目标表已存在：只重算最近 window_hours 小时内上游变更涉及的业务日
      （含留存观察窗口到位后回写的旧日，update_time 会被上游一并更新）
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM (
            SELECT dt FROM `{tryon_source_table_id()}`
            UNION DISTINCT SELECT dt FROM `{user_activity_source_table_id()}`
            UNION DISTINCT SELECT dt FROM `{user_daily_source_table_id()}`
            UNION DISTINCT SELECT dt FROM `{retention_feature_source_table_id()}`
        )
        WHERE dt >= DATE '{DATA_START_DATE}'
        ORDER BY dt
        """
        return [row.dt for row in client.query(query).result()]

    query = f"""
    WITH changed_dates AS (
        SELECT DISTINCT dt FROM `{tryon_source_table_id()}`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
        UNION DISTINCT
        SELECT DISTINCT dt FROM `{user_activity_source_table_id()}`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
        UNION DISTINCT
        SELECT DISTINCT dt FROM `{user_daily_source_table_id()}`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
        UNION DISTINCT
        SELECT DISTINCT dt FROM `{retention_feature_source_table_id()}`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    )
    SELECT DISTINCT dt FROM changed_dates
    WHERE dt >= DATE '{DATA_START_DATE}'
    ORDER BY dt
    """
    return [row.dt for row in client.query(query).result()]


def build_select_sql(day_dts: list[date]) -> str:
    """构造日级宽表的 SELECT（不含 DELETE / INSERT），便于 dry-run 校验与复用。"""
    day_values = ", ".join([f"DATE '{d}'" for d in day_dts])
    min_dt = min(day_dts)
    max_dt = max(day_dts)

    return f"""
    WITH
    -- 仅保留本次需要重算的业务日。
    day_spine AS (
        SELECT dt FROM UNNEST([{day_values}]) AS dt
    ),
    -- 试穿 / 商品 OKR 的日级分子分母。
    tryon_daily AS (
        SELECT
            dt,
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
            product_click_pv_raw
        FROM `{tryon_source_table_id()}`
        WHERE dt BETWEEN DATE '{min_dt}' AND DATE '{max_dt}'
    ),
    -- 全量与新用户 DAU、时长、留存观察日级事实。
    user_activity_daily AS (
        SELECT
            dt,
            COUNT(1) AS all_user_dau,
            SUM(duration_sec) AS all_user_duration_sum_sec,
            COUNTIF(is_new_user) AS new_user_dau,
            SUM(IF(is_new_user, duration_sec, 0)) AS new_user_duration_sum_sec,
            CASE WHEN MAX(CAST(is_next_day_observation_started AS INT64)) = 0 THEN NULL
                 ELSE COUNTIF(is_next_day_retained) END AS all_next_day_retained_user_count,
            CASE WHEN MAX(CAST(is_next_day_observation_started AS INT64)) = 0 THEN NULL
                 ELSE COUNTIF(user_id IS NOT NULL AND NOT COALESCE(is_next_day_retained, FALSE))
                 END AS all_next_day_not_retained_user_count,
            CASE WHEN MAX(CAST(is_next_day_observation_started AS INT64)) = 0 THEN NULL
                 ELSE COUNTIF(is_new_user AND is_next_day_retained) END AS new_next_day_retained_user_count,
            CASE WHEN MAX(CAST(is_next_day_observation_started AS INT64)) = 0 THEN NULL
                 ELSE COUNTIF(is_new_user AND user_id IS NOT NULL AND NOT COALESCE(is_next_day_retained, FALSE))
                 END AS new_next_day_not_retained_user_count,
            CASE WHEN MAX(CAST(is_day_7_observation_started AS INT64)) = 0 THEN NULL
                 ELSE COUNTIF(is_day_7_retained) END AS all_day_7_retained_user_count,
            CASE WHEN MAX(CAST(is_day_7_observation_started AS INT64)) = 0 THEN NULL
                 ELSE COUNTIF(user_id IS NOT NULL AND NOT COALESCE(is_day_7_retained, FALSE))
                 END AS all_day_7_not_retained_user_count,
            CASE WHEN MAX(CAST(is_day_30_observation_started AS INT64)) = 0 THEN NULL
                 ELSE COUNTIF(is_day_30_retained) END AS all_day_30_retained_user_count,
            CASE WHEN MAX(CAST(is_day_30_observation_started AS INT64)) = 0 THEN NULL
                 ELSE COUNTIF(user_id IS NOT NULL AND NOT COALESCE(is_day_30_retained, FALSE))
                 END AS all_day_30_not_retained_user_count,
            COUNTIF(exposed_home_feeds = 1) AS home_feeds_exposure_user_count,
            COUNTIF(clicked_home_feeds = 1) AS home_feeds_click_user_count,
            CASE WHEN MAX(CAST(is_next_day_observation_started AS INT64)) = 0 THEN NULL
                 ELSE COUNTIF(is_next_day_retained AND used_styleme = 1)
                 END AS styleme_next_day_retained_user_count,
            CASE WHEN MAX(CAST(is_next_day_observation_started AS INT64)) = 0 THEN NULL
                 ELSE COUNTIF(user_id IS NOT NULL AND used_styleme = 1 AND NOT COALESCE(is_next_day_retained, FALSE))
                 END AS styleme_next_day_not_retained_user_count
        FROM `{user_activity_source_table_id()}`
        WHERE dt BETWEEN DATE '{min_dt}' AND DATE '{max_dt}'
        GROUP BY dt
    ),
    -- 用户日表先按用户去重，再聚合内容消费，避免一人多行放大。
    user_daily_deduplicated AS (
        SELECT
            dt,
            prop_user_id AS user_id,
            LOGICAL_OR(is_new_user) AS is_new_user,
            SUM(content_consume_count) AS content_consume_count
        FROM `{user_daily_source_table_id()}`
        WHERE dt BETWEEN DATE '{min_dt}' AND DATE '{max_dt}'
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
        GROUP BY dt, user_id
    ),
    -- 内容消费相关分子分母。
    user_content_daily AS (
        SELECT
            dt,
            COUNT(1) AS all_user_content_dau,
            SUM(content_consume_count) AS all_content_consume_sum,
            COUNTIF(is_new_user) AS new_user_content_dau,
            SUM(IF(is_new_user, content_consume_count, 0)) AS new_content_consume_sum
        FROM user_daily_deduplicated
        GROUP BY dt
    ),
    -- 留存特征分组（衣橱 / 商详 / AI试穿 / 关注 / 创建模特）日级事实。
    retention_feature_daily AS (
        SELECT
            dt,
            MAX(IF(feature_name = '衣橱渗透', next_day_retained_value, NULL))
                AS closet_next_day_retained_user_count,
            MAX(IF(feature_name = '衣橱渗透', next_day_not_retained_value, NULL))
                AS closet_next_day_not_retained_user_count,
            MAX(IF(feature_name = '浏览商详', next_day_retained_value, NULL))
                AS product_detail_next_day_retained_user_count,
            MAX(IF(feature_name = '浏览商详', next_day_not_retained_value, NULL))
                AS product_detail_next_day_not_retained_user_count,
            MAX(IF(feature_name = 'AI试穿', next_day_retained_value, NULL))
                AS ai_tryon_next_day_retained_user_count,
            MAX(IF(feature_name = 'AI试穿', next_day_not_retained_value, NULL))
                AS ai_tryon_next_day_not_retained_user_count,
            MAX(IF(feature_name = '关注账号', next_day_retained_value, NULL))
                AS follow_next_day_retained_user_count,
            MAX(IF(feature_name = '关注账号', next_day_not_retained_value, NULL))
                AS follow_next_day_not_retained_user_count,
            MAX(IF(feature_name = '创建模特', next_day_retained_value, NULL))
                AS model_next_day_retained_user_count,
            MAX(IF(feature_name = '创建模特', next_day_not_retained_value, NULL))
                AS model_next_day_not_retained_user_count
        FROM `{retention_feature_source_table_id()}`
        WHERE dt BETWEEN DATE '{min_dt}' AND DATE '{max_dt}'
            AND user_group = '全量登录用户'
            AND feature_name IN ('衣橱渗透', '浏览商详', 'AI试穿', '关注账号', '创建模特')
        GROUP BY dt
    ),
    -- 汇总各主题日级分子分母，统一补齐缺失日。
    day_fact AS (
        SELECT
            s.dt,
            COALESCE(t.dau_new_uv, 0) AS dau_new_uv,
            COALESCE(t.dau_old_uv, 0) AS dau_old_uv,
            COALESCE(t.tryon_new_uv, 0) AS tryon_new_uv,
            COALESCE(t.tryon_old_uv, 0) AS tryon_old_uv,
            COALESCE(t.home_tryon_uv, 0) AS home_tryon_uv,
            COALESCE(t.home_feeds_uv, 0) AS home_feeds_uv,
            COALESCE(t.post_tryon_uv, 0) AS post_tryon_uv,
            COALESCE(t.post_scene_exposure_uv, 0) AS post_scene_exposure_uv,
            COALESCE(t.product_detail_tryon_uv, 0) AS product_detail_tryon_uv,
            COALESCE(t.product_detail_uv, 0) AS product_detail_uv,
            COALESCE(t.styleme_uv, 0) AS styleme_uv,
            COALESCE(t.front_tryon_click_uv, 0) AS front_tryon_click_uv,
            COALESCE(t.shop_tryon_uv, 0) AS shop_tryon_uv,
            COALESCE(t.closet_tryon_uv, 0) AS closet_tryon_uv,
            COALESCE(t.closet_feeds_uv, 0) AS closet_feeds_uv,
            COALESCE(t.styleme_exposure_uv, 0) AS styleme_exposure_uv,
            COALESCE(t.all_tryon_success_duration_task_count, 0) AS all_tryon_success_duration_task_count,
            COALESCE(t.all_tryon_success_total_duration_ms, 0) AS all_tryon_success_total_duration_ms,
            COALESCE(t.quick_tryon_task_total_count, 0) AS quick_tryon_task_total_count,
            COALESCE(t.quick_tryon_task_success_count, 0) AS quick_tryon_task_success_count,
            COALESCE(t.quick_tryon_success_duration_task_count, 0) AS quick_tryon_success_duration_task_count,
            COALESCE(t.quick_tryon_success_total_duration_ms, 0) AS quick_tryon_success_total_duration_ms,
            COALESCE(t.plus_tryon_task_total_count, 0) AS plus_tryon_task_total_count,
            COALESCE(t.plus_tryon_task_success_count, 0) AS plus_tryon_task_success_count,
            COALESCE(t.plus_tryon_success_duration_task_count, 0) AS plus_tryon_success_duration_task_count,
            COALESCE(t.plus_tryon_success_total_duration_ms, 0) AS plus_tryon_success_total_duration_ms,
            COALESCE(t.product_click_uv, 0) AS product_click_uv,
            COALESCE(t.product_exposure_uv, 0) AS product_exposure_uv,
            COALESCE(t.home_product_click_uv, 0) AS home_product_click_uv,
            COALESCE(t.home_product_exposure_uv, 0) AS home_product_exposure_uv,
            COALESCE(t.tryon_result_click_uv, 0) AS tryon_result_click_uv,
            COALESCE(t.tryon_result_exposure_uv, 0) AS tryon_result_exposure_uv,
            COALESCE(t.shop_click_uv, 0) AS shop_click_uv,
            COALESCE(t.shop_exposure_uv, 0) AS shop_exposure_uv,
            COALESCE(t.tryon_uv_total, 0) AS tryon_uv_total,
            COALESCE(t.tryon_pv, 0) AS tryon_pv,
            COALESCE(t.product_click_pv_raw, 0) AS product_click_pv_raw,
            COALESCE(a.all_user_dau, 0) AS all_user_dau,
            COALESCE(a.all_user_duration_sum_sec, 0) AS all_user_duration_sum_sec,
            COALESCE(a.new_user_dau, 0) AS new_user_dau,
            COALESCE(a.new_user_duration_sum_sec, 0) AS new_user_duration_sum_sec,
            a.all_next_day_retained_user_count,
            a.all_next_day_not_retained_user_count,
            a.new_next_day_retained_user_count,
            a.new_next_day_not_retained_user_count,
            a.all_day_7_retained_user_count,
            a.all_day_7_not_retained_user_count,
            a.all_day_30_retained_user_count,
            a.all_day_30_not_retained_user_count,
            COALESCE(a.home_feeds_exposure_user_count, 0) AS home_feeds_exposure_user_count,
            COALESCE(a.home_feeds_click_user_count, 0) AS home_feeds_click_user_count,
            a.styleme_next_day_retained_user_count,
            a.styleme_next_day_not_retained_user_count,
            COALESCE(u.all_user_content_dau, 0) AS all_user_content_dau,
            COALESCE(u.all_content_consume_sum, 0) AS all_content_consume_sum,
            COALESCE(u.new_user_content_dau, 0) AS new_user_content_dau,
            COALESCE(u.new_content_consume_sum, 0) AS new_content_consume_sum,
            r.closet_next_day_retained_user_count,
            r.closet_next_day_not_retained_user_count,
            r.product_detail_next_day_retained_user_count,
            r.product_detail_next_day_not_retained_user_count,
            r.ai_tryon_next_day_retained_user_count,
            r.ai_tryon_next_day_not_retained_user_count,
            r.follow_next_day_retained_user_count,
            r.follow_next_day_not_retained_user_count,
            r.model_next_day_retained_user_count,
            r.model_next_day_not_retained_user_count
        FROM day_spine s
        LEFT JOIN tryon_daily t ON t.dt = s.dt
        LEFT JOIN user_activity_daily a ON a.dt = s.dt
        LEFT JOIN user_content_daily u ON u.dt = s.dt
        LEFT JOIN retention_feature_daily r ON r.dt = s.dt
    )
    SELECT
        dt,
        -- 试穿 OKR 核心指标
        SAFE_DIVIDE(tryon_new_uv, NULLIF(dau_new_uv, 0)) AS new_user_tryon_rate,
        CAST(tryon_new_uv AS NUMERIC) AS new_user_tryon_uv,
        SAFE_DIVIDE(tryon_old_uv, NULLIF(dau_old_uv, 0)) AS old_user_tryon_rate,
        CAST(tryon_old_uv AS NUMERIC) AS old_user_tryon_uv,
        CAST(home_tryon_uv AS NUMERIC) AS home_tryon_uv,
        SAFE_DIVIDE(home_tryon_uv, NULLIF(home_feeds_uv, 0)) AS home_tryon_penetration_rate,
        CAST(post_tryon_uv AS NUMERIC) AS post_tryon_uv,
        SAFE_DIVIDE(post_tryon_uv, NULLIF(post_scene_exposure_uv, 0)) AS post_tryon_penetration_rate,
        CAST(product_detail_tryon_uv AS NUMERIC) AS product_detail_tryon_uv,
        SAFE_DIVIDE(product_detail_tryon_uv, NULLIF(product_detail_uv, 0)) AS product_detail_tryon_penetration_rate,
        CAST(styleme_uv AS NUMERIC) AS styleme_tryon_uv,
        SAFE_DIVIDE(tryon_uv_total, NULLIF(all_user_dau, 0)) AS platform_tryon_rate,
        SAFE_DIVIDE(tryon_pv, NULLIF(tryon_uv_total, 0)) AS tryon_avg_count_per_tryon_user,
        CAST(tryon_uv_total AS NUMERIC) AS avg_daily_tryon_uv,
        CAST(front_tryon_click_uv AS NUMERIC) AS front_tryon_click_uv,
        SAFE_DIVIDE(shop_tryon_uv, NULLIF(shop_exposure_uv, 0)) AS shop_feeds_tryon_penetration_rate,
        CAST(shop_tryon_uv AS NUMERIC) AS shop_feeds_tryon_uv,
        SAFE_DIVIDE(closet_tryon_uv, NULLIF(closet_feeds_uv, 0)) AS closet_feeds_tryon_penetration_rate,
        CAST(closet_tryon_uv AS NUMERIC) AS closet_feeds_tryon_uv,
        SAFE_DIVIDE(styleme_uv, NULLIF(styleme_exposure_uv, 0)) AS styleme_tryon_penetration_rate,
        SAFE_DIVIDE(quick_tryon_task_success_count, NULLIF(quick_tryon_task_total_count, 0)) AS quick_tryon_success_rate,
        SAFE_DIVIDE(plus_tryon_task_success_count, NULLIF(plus_tryon_task_total_count, 0)) AS plus_tryon_success_rate,
        SAFE_DIVIDE(quick_tryon_success_total_duration_ms, NULLIF(quick_tryon_success_duration_task_count, 0)) / 60000.0
            AS quick_tryon_avg_duration_min,
        SAFE_DIVIDE(plus_tryon_success_total_duration_ms, NULLIF(plus_tryon_success_duration_task_count, 0)) / 60000.0
            AS plus_tryon_avg_duration_min,
        -- 商品 OKR 核心指标
        CAST(product_click_uv AS NUMERIC) AS product_click_uv,
        CAST(product_exposure_uv AS NUMERIC) AS product_exposure_uv,
        SAFE_DIVIDE(product_click_uv, NULLIF(product_exposure_uv, 0)) AS product_exposure_click_rate,
        SAFE_DIVIDE(product_click_pv_raw, NULLIF(product_click_uv, 0)) AS product_click_avg_count_per_click_user,
        CAST(home_product_click_uv AS NUMERIC) AS home_product_click_uv,
        SAFE_DIVIDE(home_product_click_uv, NULLIF(home_product_exposure_uv, 0)) AS home_product_exposure_click_rate,
        CAST(tryon_result_click_uv AS NUMERIC) AS tryon_result_click_uv,
        SAFE_DIVIDE(tryon_result_click_uv, NULLIF(tryon_result_exposure_uv, 0)) AS tryon_result_exposure_click_rate,
        CAST(shop_click_uv AS NUMERIC) AS shop_click_uv,
        SAFE_DIVIDE(shop_click_uv, NULLIF(shop_exposure_uv, 0)) AS shop_exposure_click_rate,
        -- 留存 OKR 核心指标
        CAST(all_user_dau AS NUMERIC) AS platform_dau,
        SAFE_DIVIDE(all_next_day_retained_user_count,
            NULLIF(all_next_day_retained_user_count + all_next_day_not_retained_user_count, 0)) AS next_day_retention_rate,
        CAST(all_next_day_retained_user_count AS NUMERIC) AS next_day_retained_user_count,
        SAFE_DIVIDE(all_day_7_retained_user_count,
            NULLIF(all_day_7_retained_user_count + all_day_7_not_retained_user_count, 0)) AS day_7_retention_rate,
        CAST(all_day_7_retained_user_count AS NUMERIC) AS day_7_retained_user_count,
        SAFE_DIVIDE(all_day_30_retained_user_count,
            NULLIF(all_day_30_retained_user_count + all_day_30_not_retained_user_count, 0)) AS day_30_retention_rate,
        CAST(all_day_30_retained_user_count AS NUMERIC) AS day_30_retained_user_count,
        SAFE_DIVIDE(home_feeds_click_user_count, NULLIF(home_feeds_exposure_user_count, 0)) AS home_feeds_exposure_click_rate,
        SAFE_DIVIDE(all_user_duration_sum_sec, NULLIF(all_user_dau, 0)) / 60.0 AS platform_avg_duration_min,
        SAFE_DIVIDE(all_content_consume_sum, NULLIF(all_user_content_dau, 0)) AS platform_avg_content_consume,
        CAST(new_user_dau AS NUMERIC) AS new_user_dau,
        SAFE_DIVIDE(new_next_day_retained_user_count,
            NULLIF(new_next_day_retained_user_count + new_next_day_not_retained_user_count, 0)) AS new_user_next_day_retention_rate,
        CAST(new_next_day_retained_user_count AS NUMERIC) AS new_user_next_day_retained_user_count,
        SAFE_DIVIDE(new_user_duration_sum_sec, NULLIF(new_user_dau, 0)) / 60.0 AS new_user_avg_duration_min,
        SAFE_DIVIDE(new_content_consume_sum, NULLIF(new_user_content_dau, 0)) AS new_user_avg_content_consume,
        SAFE_DIVIDE(closet_next_day_retained_user_count,
            NULLIF(closet_next_day_retained_user_count + closet_next_day_not_retained_user_count, 0)) AS closet_user_next_day_retention_rate,
        CAST(closet_next_day_retained_user_count AS NUMERIC) AS closet_user_next_day_retained_user_count,
        SAFE_DIVIDE(product_detail_next_day_retained_user_count,
            NULLIF(product_detail_next_day_retained_user_count + product_detail_next_day_not_retained_user_count, 0)) AS product_detail_user_next_day_retention_rate,
        CAST(product_detail_next_day_retained_user_count AS NUMERIC) AS product_detail_user_next_day_retained_user_count,
        SAFE_DIVIDE(ai_tryon_next_day_retained_user_count,
            NULLIF(ai_tryon_next_day_retained_user_count + ai_tryon_next_day_not_retained_user_count, 0)) AS ai_tryon_user_next_day_retention_rate,
        CAST(ai_tryon_next_day_retained_user_count AS NUMERIC) AS ai_tryon_user_next_day_retained_user_count,
        SAFE_DIVIDE(follow_next_day_retained_user_count,
            NULLIF(follow_next_day_retained_user_count + follow_next_day_not_retained_user_count, 0)) AS follow_user_next_day_retention_rate,
        CAST(follow_next_day_retained_user_count AS NUMERIC) AS follow_user_next_day_retained_user_count,
        SAFE_DIVIDE(model_next_day_retained_user_count,
            NULLIF(model_next_day_retained_user_count + model_next_day_not_retained_user_count, 0)) AS model_user_next_day_retention_rate,
        CAST(model_next_day_retained_user_count AS NUMERIC) AS model_user_next_day_retained_user_count,
        SAFE_DIVIDE(styleme_next_day_retained_user_count,
            NULLIF(styleme_next_day_retained_user_count + styleme_next_day_not_retained_user_count, 0)) AS styleme_user_next_day_retention_rate,
        CAST(styleme_next_day_retained_user_count AS NUMERIC) AS styleme_user_next_day_retained_user_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM day_fact
    """


def build_write_sql(day_dts: list[date]) -> str:
    """DELETE + INSERT 脚本。粒度为 dt，按 dt 覆盖重写。"""
    day_list = dates_to_sql_list(day_dts)
    select_sql = build_select_sql(day_dts)
    return f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({day_list});

    INSERT INTO `{target_table_id()}`
    (
{target_columns_sql()}
    )
    {select_sql}
    """


def dry_run(day_dts: list[date]) -> None:
    """仅校验 SELECT SQL 是否合法（不写数据、不消耗计费）。"""
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = client.query(build_select_sql(day_dts), job_config=job_config)
    logging.info(
        "dry-run 通过：SQL 合法，预计扫描 %.2f MB",
        job.total_bytes_processed / 1024 / 1024,
    )


def run(day_dts: list[date]) -> None:
    if not day_dts:
        logging.info("没有需要处理的日期")
        return
    logging.info("开始处理 %d 天：%s ~ %s", len(day_dts), min(day_dts), max(day_dts))
    client.query(build_write_sql(day_dts)).result()
    logging.info("写入完成")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=str, default=None, help="回刷起始日（含），YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, default=None, help="回刷结束日（含），YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true", help="仅校验 SQL，不写数据")
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = _parse_args()

    if args.start_date and args.end_date:
        start = date.fromisoformat(args.start_date)
        end = date.fromisoformat(args.end_date)
        if start > end:
            raise ValueError("start-date 不能晚于 end-date")
        day_dts = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    else:
        day_dts = get_dates_to_process()

    if not day_dts:
        logging.info("没有需要处理的日期")
        return

    if args.dry_run:
        dry_run(day_dts)
    else:
        run(day_dts)


if __name__ == "__main__":
    main()
