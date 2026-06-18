from datetime import date, datetime, timedelta, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_weekly_ai_fashion_tryon_okr_v2"
TRYON_SOURCE_TABLE_NAME = "dws_ai_fashion_tryon_okr_daily"
USER_ACTIVITY_SOURCE_TABLE_NAME = "dws_user_activity_daily"
USER_DAILY_SOURCE_TABLE_NAME = "dws_user_daily"
RETENTION_FEATURE_SOURCE_TABLE_NAME = "dws_user_retention_feature_daily"
WEEK_BUCKET_START_DATE = date(2026, 4, 27)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "ads" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6

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


def get_week_starts_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    周级 AI-Fashion OKR ADS 同时消费多张 DWS。

    规则：
    - 目标表不存在或为空时，按上游已落地历史周全量初始化
    - 目标表已存在时，只重算最近 6 小时上游变更映射出的业务周
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT DATE_TRUNC(dt, WEEK(MONDAY)) AS week_start_dt
        FROM (
            SELECT dt FROM `{tryon_source_table_id()}`
            UNION DISTINCT
            SELECT dt FROM `{user_activity_source_table_id()}`
            UNION DISTINCT
            SELECT dt FROM `{user_daily_source_table_id()}`
            UNION DISTINCT
            SELECT dt FROM `{retention_feature_source_table_id()}`
        )
        WHERE dt >= DATE '{WEEK_BUCKET_START_DATE}'
        ORDER BY week_start_dt
        """
        results = client.query(query).result()
        return [row.week_start_dt for row in results]

    query = f"""
    WITH
    -- 最近 6 小时试穿商品 OKR 日事实变更。
    changed_tryon_dates AS (
        SELECT DISTINCT dt
        FROM `{tryon_source_table_id()}`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 最近 6 小时留存用户活动日事实变更。
    changed_activity_dates AS (
        SELECT DISTINCT dt
        FROM `{user_activity_source_table_id()}`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 最近 6 小时注册用户日事实变更。
    changed_user_daily_dates AS (
        SELECT DISTINCT dt
        FROM `{user_daily_source_table_id()}`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 最近 6 小时留存特征日事实变更。
    changed_retention_feature_dates AS (
        SELECT DISTINCT dt
        FROM `{retention_feature_source_table_id()}`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 汇总所有会影响周报的业务日。
    changed_dates AS (
        SELECT dt FROM changed_tryon_dates
        UNION DISTINCT
        SELECT dt FROM changed_activity_dates
        UNION DISTINCT
        SELECT dt FROM changed_user_daily_dates
        UNION DISTINCT
        SELECT dt FROM changed_retention_feature_dates
    )
    SELECT DISTINCT DATE_TRUNC(dt, WEEK(MONDAY)) AS week_start_dt
    FROM changed_dates
    WHERE dt >= DATE '{WEEK_BUCKET_START_DATE}'
    ORDER BY week_start_dt
    """
    results = client.query(query).result()
    return [row.week_start_dt for row in results]


def run_ads_weekly_ai_fashion_tryon_okr_v2(week_starts: list[date]) -> None:
    """
    产出 AI-Fashion 试穿 / 商品 / 留存 OKR v2 周报宽表。

    粒度：
    - week_start_dt

    说明：
    - 周窗口固定按周一到周日
    - 周级 UV 字段统一采用“周内日值求和 / 天数”的日均口径
    - 周级率指标统一采用“周内分子求和 / 周内分母求和”口径
    - v2 将 v1 的 metric_name / metric_value 形态列化，便于 Superset 直接取字段作图
    """
    if not week_starts:
        logging.info("没有新的 ads_weekly_ai_fashion_tryon_okr_v2 周需要处理")
        return

    week_starts_str = dates_to_sql_list(week_starts)
    min_week_start = min(week_starts)
    max_week_end = max(week_starts) + timedelta(days=6)

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE week_start_dt IN ({week_starts_str});

    INSERT INTO `{target_table_id()}`
    (
        week_start_dt,
        week_end_dt,
        week_label,
        week_day_count,
        all_next_day_observed_day_count,
        new_next_day_observed_day_count,
        closet_next_day_observed_day_count,
        product_detail_next_day_observed_day_count,
        ai_tryon_next_day_observed_day_count,
        follow_next_day_observed_day_count,
        model_next_day_observed_day_count,
        all_day_7_observed_day_count,
        all_day_30_observed_day_count,
        styleme_next_day_observed_day_count,
        new_user_tryon_rate,
        new_user_tryon_uv,
        old_user_tryon_rate,
        old_user_tryon_uv,
        home_tryon_uv,
        home_tryon_penetration_rate,
        post_tryon_uv,
        post_tryon_penetration_rate,
        product_detail_tryon_uv,
        product_detail_tryon_penetration_rate,
        styleme_tryon_uv,
        product_click_uv,
        product_exposure_uv,
        home_product_click_uv,
        home_product_exposure_click_rate,
        tryon_result_click_uv,
        tryon_result_exposure_click_rate,
        shop_click_uv,
        shop_exposure_click_rate,
        next_day_retention_rate,
        next_day_retained_user_count,
        platform_dau,
        day_7_retention_rate,
        day_7_retained_user_count,
        day_30_retention_rate,
        day_30_retained_user_count,
        home_feeds_exposure_click_rate,
        platform_avg_duration_min,
        platform_avg_content_consume,
        new_user_dau,
        new_user_next_day_retention_rate,
        new_user_avg_duration_min,
        new_user_avg_content_consume,
        closet_user_next_day_retention_rate,
        closet_user_next_day_retained_user_count,
        product_detail_user_next_day_retention_rate,
        product_detail_user_next_day_retained_user_count,
        ai_tryon_user_next_day_retention_rate,
        ai_tryon_user_next_day_retained_user_count,
        follow_user_next_day_retention_rate,
        follow_user_next_day_retained_user_count,
        model_user_next_day_retention_rate,
        model_user_next_day_retained_user_count,
        styleme_user_next_day_retention_rate,
        styleme_user_next_day_retained_user_count,
        tryon_avg_daily_count_bucket_0_3_day_count,
        tryon_avg_daily_count_bucket_0_3_day_ratio,
        tryon_avg_daily_count_bucket_3_6_day_count,
        tryon_avg_daily_count_bucket_3_6_day_ratio,
        tryon_avg_daily_count_bucket_6_10_day_count,
        tryon_avg_daily_count_bucket_6_10_day_ratio,
        tryon_avg_daily_count_bucket_10_15_day_count,
        tryon_avg_daily_count_bucket_10_15_day_ratio,
        tryon_avg_daily_count_bucket_gt_15_day_count,
        tryon_avg_daily_count_bucket_gt_15_day_ratio,
        tryon_avg_daily_count_bucket_zero_or_null_uv_day_count,
        tryon_avg_daily_count_bucket_zero_or_null_uv_day_ratio,
        product_click_avg_daily_count_bucket_0_3_day_count,
        product_click_avg_daily_count_bucket_3_5_day_count,
        product_click_avg_daily_count_bucket_5_10_day_count,
        product_click_avg_daily_count_bucket_10_15_day_count,
        product_click_avg_daily_count_bucket_gt_15_day_count,
        product_click_avg_daily_count_bucket_zero_or_null_uv_day_count,
        update_time
    )
    WITH
    -- 目标周骨架：按待刷新周补齐周起止日期与展示标签。
    week_spine AS (
        SELECT
            week_start_dt,
            DATE_ADD(week_start_dt, INTERVAL 6 DAY) AS week_end_dt,
            CONCAT(
                FORMAT_DATE('%m%d', week_start_dt),
                '-',
                FORMAT_DATE('%m%d', DATE_ADD(week_start_dt, INTERVAL 6 DAY))
            ) AS week_label
        FROM UNNEST([{', '.join([f"DATE '{d}'" for d in week_starts])}]) AS week_start_dt
    ),
    -- 周日历骨架：将每个业务周展开到天，支撑“周内日均”口径。
    week_calendar AS (
        SELECT
            w.week_start_dt,
            w.week_end_dt,
            w.week_label,
            dt
        FROM week_spine w
        CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(w.week_start_dt, w.week_end_dt)) AS dt
    ),
    -- 试穿与商品 OKR 日事实。
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
        WHERE dt BETWEEN DATE '{min_week_start}' AND DATE '{max_week_end}'
    ),
    -- 已登录 cohort 用户日事实（用于留存、新用户时长）。
    user_activity_daily AS (
        SELECT
            dt,
            COUNT(1) AS all_user_dau,
            SUM(duration_sec) AS all_user_duration_sum_sec,
            COUNTIF(is_new_user) AS new_user_dau,
            SUM(IF(is_new_user, duration_sec, 0)) AS new_user_duration_sum_sec,
            CASE
                WHEN MAX(CAST(is_next_day_observation_started AS INT64)) = 0
                    THEN NULL
                ELSE COUNTIF(is_next_day_retained)
            END AS all_next_day_retained_user_count,
            CASE
                WHEN MAX(CAST(is_next_day_observation_started AS INT64)) = 0
                    THEN NULL
                ELSE COUNTIF(
                    user_id IS NOT NULL
                    AND NOT COALESCE(is_next_day_retained, FALSE)
                )
            END AS all_next_day_not_retained_user_count,
            CASE
                WHEN MAX(CAST(is_next_day_observation_started AS INT64)) = 0
                    THEN NULL
                ELSE COUNTIF(is_new_user AND is_next_day_retained)
            END AS new_next_day_retained_user_count,
            CASE
                WHEN MAX(CAST(is_next_day_observation_started AS INT64)) = 0
                    THEN NULL
                ELSE COUNTIF(
                    is_new_user
                    AND user_id IS NOT NULL
                    AND NOT COALESCE(is_next_day_retained, FALSE)
                )
            END AS new_next_day_not_retained_user_count,
            CASE
                WHEN MAX(CAST(is_day_7_observation_started AS INT64)) = 0
                    THEN NULL
                ELSE COUNTIF(is_day_7_retained)
            END AS all_day_7_retained_user_count,
            CASE
                WHEN MAX(CAST(is_day_7_observation_started AS INT64)) = 0
                    THEN NULL
                ELSE COUNTIF(
                    user_id IS NOT NULL
                    AND NOT COALESCE(is_day_7_retained, FALSE)
                )
            END AS all_day_7_not_retained_user_count,
            CASE
                WHEN MAX(CAST(is_day_30_observation_started AS INT64)) = 0
                    THEN NULL
                ELSE COUNTIF(is_day_30_retained)
            END AS all_day_30_retained_user_count,
            CASE
                WHEN MAX(CAST(is_day_30_observation_started AS INT64)) = 0
                    THEN NULL
                ELSE COUNTIF(
                    user_id IS NOT NULL
                    AND NOT COALESCE(is_day_30_retained, FALSE)
                )
            END AS all_day_30_not_retained_user_count,
            COUNTIF(exposed_home_feeds = 1) AS home_feeds_exposure_user_count,
            COUNTIF(clicked_home_feeds = 1) AS home_feeds_click_user_count,
            CASE
                WHEN MAX(CAST(is_next_day_observation_started AS INT64)) = 0
                    THEN NULL
                ELSE COUNTIF(is_next_day_retained AND used_styleme = 1)
            END AS styleme_next_day_retained_user_count,
            CASE
                WHEN MAX(CAST(is_next_day_observation_started AS INT64)) = 0
                    THEN NULL
                ELSE COUNTIF(
                    user_id IS NOT NULL
                    AND used_styleme = 1
                    AND NOT COALESCE(is_next_day_retained, FALSE)
                )
            END AS styleme_next_day_not_retained_user_count
        FROM `{user_activity_source_table_id()}`
        WHERE dt BETWEEN DATE '{min_week_start}' AND DATE '{max_week_end}'
        GROUP BY dt
    ),
    -- 按 dt × user_id 去重后的内容消费中间层，避免跨平台用户重复计入分母。
    user_daily_deduplicated AS (
        SELECT
            dt,
            prop_user_id AS user_id,
            LOGICAL_OR(is_new_user) AS is_new_user,
            SUM(content_consume_count) AS content_consume_count
        FROM `{user_daily_source_table_id()}`
        WHERE dt BETWEEN DATE '{min_week_start}' AND DATE '{max_week_end}'
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
        GROUP BY dt, user_id
    ),
    -- 内容消费日事实：支持“平台人均内容消费”和“新用户人均消费内容”。
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
    -- 全量登录用户下的行为留存特征日事实（衣橱/商详/AI试穿/关注/创建模特）。
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
        WHERE dt BETWEEN DATE '{min_week_start}' AND DATE '{max_week_end}'
            AND user_group = '全量登录用户'
            AND feature_name IN ('衣橱渗透', '浏览商详', 'AI试穿', '关注账号', '创建模特')
        GROUP BY dt
    ),
    -- 周内日事实：从多张 DWS 读取分子分母并补 0，避免缺天漏算。
    week_daily_fact AS (
        SELECT
            c.week_start_dt,
            c.week_end_dt,
            c.week_label,
            c.dt,
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
        FROM week_calendar c
        LEFT JOIN tryon_daily t
            ON t.dt = c.dt
        LEFT JOIN user_activity_daily a
            ON a.dt = c.dt
        LEFT JOIN user_content_daily u
            ON u.dt = c.dt
        LEFT JOIN retention_feature_daily r
            ON r.dt = c.dt
    ),
    -- 周聚合层：先聚合分子分母和天数，供核心指标统一复用。
    weekly_agg AS (
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            COUNT(*) AS day_count,
            SUM(dau_new_uv) AS sum_dau_new_uv,
            SUM(dau_old_uv) AS sum_dau_old_uv,
            SUM(tryon_new_uv) AS sum_tryon_new_uv,
            SUM(tryon_old_uv) AS sum_tryon_old_uv,
            SUM(home_tryon_uv) AS sum_home_tryon_uv,
            SUM(home_feeds_uv) AS sum_home_feeds_uv,
            SUM(post_tryon_uv) AS sum_post_tryon_uv,
            SUM(post_scene_exposure_uv) AS sum_post_scene_exposure_uv,
            SUM(product_detail_tryon_uv) AS sum_product_detail_tryon_uv,
            SUM(product_detail_uv) AS sum_product_detail_uv,
            SUM(styleme_uv) AS sum_styleme_uv,
            SUM(product_click_uv) AS sum_product_click_uv,
            SUM(product_exposure_uv) AS sum_product_exposure_uv,
            SUM(home_product_click_uv) AS sum_home_product_click_uv,
            SUM(home_product_exposure_uv) AS sum_home_product_exposure_uv,
            SUM(tryon_result_click_uv) AS sum_tryon_result_click_uv,
            SUM(tryon_result_exposure_uv) AS sum_tryon_result_exposure_uv,
            SUM(shop_click_uv) AS sum_shop_click_uv,
            SUM(shop_exposure_uv) AS sum_shop_exposure_uv,
            SUM(all_user_dau) AS sum_all_user_dau,
            SUM(all_user_duration_sum_sec) AS sum_all_user_duration_sum_sec,
            SUM(all_user_content_dau) AS sum_all_user_content_dau,
            SUM(all_content_consume_sum) AS sum_all_content_consume_sum,
            SUM(new_user_dau) AS sum_new_user_dau,
            SUM(new_user_duration_sum_sec) AS sum_new_user_duration_sum_sec,
            SUM(new_user_content_dau) AS sum_new_user_content_dau,
            SUM(new_content_consume_sum) AS sum_new_content_consume_sum,
            SUM(COALESCE(all_next_day_retained_user_count, 0)) AS sum_all_next_day_retained_user_count,
            SUM(COALESCE(all_next_day_not_retained_user_count, 0))
                AS sum_all_next_day_not_retained_user_count,
            SUM(COALESCE(new_next_day_retained_user_count, 0)) AS sum_new_next_day_retained_user_count,
            SUM(COALESCE(new_next_day_not_retained_user_count, 0))
                AS sum_new_next_day_not_retained_user_count,
            SUM(COALESCE(all_day_7_retained_user_count, 0)) AS sum_all_day_7_retained_user_count,
            SUM(COALESCE(all_day_7_not_retained_user_count, 0))
                AS sum_all_day_7_not_retained_user_count,
            SUM(COALESCE(all_day_30_retained_user_count, 0)) AS sum_all_day_30_retained_user_count,
            SUM(COALESCE(all_day_30_not_retained_user_count, 0))
                AS sum_all_day_30_not_retained_user_count,
            SUM(home_feeds_exposure_user_count) AS sum_home_feeds_exposure_user_count,
            SUM(home_feeds_click_user_count) AS sum_home_feeds_click_user_count,
            SUM(COALESCE(styleme_next_day_retained_user_count, 0))
                AS sum_styleme_next_day_retained_user_count,
            SUM(COALESCE(styleme_next_day_not_retained_user_count, 0))
                AS sum_styleme_next_day_not_retained_user_count,
            COUNTIF(
                all_next_day_retained_user_count IS NOT NULL
                AND all_next_day_not_retained_user_count IS NOT NULL
            ) AS all_next_day_observed_day_count,
            COUNTIF(
                new_next_day_retained_user_count IS NOT NULL
                AND new_next_day_not_retained_user_count IS NOT NULL
            ) AS new_next_day_observed_day_count,
            COUNTIF(
                all_day_7_retained_user_count IS NOT NULL
                AND all_day_7_not_retained_user_count IS NOT NULL
            ) AS all_day_7_observed_day_count,
            COUNTIF(
                all_day_30_retained_user_count IS NOT NULL
                AND all_day_30_not_retained_user_count IS NOT NULL
            ) AS all_day_30_observed_day_count,
            COUNTIF(
                styleme_next_day_retained_user_count IS NOT NULL
                AND styleme_next_day_not_retained_user_count IS NOT NULL
            ) AS styleme_next_day_observed_day_count,
            SUM(COALESCE(closet_next_day_retained_user_count, 0))
                AS sum_closet_next_day_retained_user_count,
            SUM(COALESCE(closet_next_day_not_retained_user_count, 0))
                AS sum_closet_next_day_not_retained_user_count,
            SUM(COALESCE(product_detail_next_day_retained_user_count, 0))
                AS sum_product_detail_next_day_retained_user_count,
            SUM(COALESCE(product_detail_next_day_not_retained_user_count, 0))
                AS sum_product_detail_next_day_not_retained_user_count,
            SUM(COALESCE(ai_tryon_next_day_retained_user_count, 0))
                AS sum_ai_tryon_next_day_retained_user_count,
            SUM(COALESCE(ai_tryon_next_day_not_retained_user_count, 0))
                AS sum_ai_tryon_next_day_not_retained_user_count,
            SUM(COALESCE(follow_next_day_retained_user_count, 0))
                AS sum_follow_next_day_retained_user_count,
            SUM(COALESCE(follow_next_day_not_retained_user_count, 0))
                AS sum_follow_next_day_not_retained_user_count,
            SUM(COALESCE(model_next_day_retained_user_count, 0))
                AS sum_model_next_day_retained_user_count,
            SUM(COALESCE(model_next_day_not_retained_user_count, 0))
                AS sum_model_next_day_not_retained_user_count,
            COUNTIF(
                closet_next_day_retained_user_count IS NOT NULL
                AND closet_next_day_not_retained_user_count IS NOT NULL
            ) AS closet_next_day_observed_day_count,
            COUNTIF(
                product_detail_next_day_retained_user_count IS NOT NULL
                AND product_detail_next_day_not_retained_user_count IS NOT NULL
            ) AS product_detail_next_day_observed_day_count,
            COUNTIF(
                ai_tryon_next_day_retained_user_count IS NOT NULL
                AND ai_tryon_next_day_not_retained_user_count IS NOT NULL
            ) AS ai_tryon_next_day_observed_day_count,
            COUNTIF(
                follow_next_day_retained_user_count IS NOT NULL
                AND follow_next_day_not_retained_user_count IS NOT NULL
            ) AS follow_next_day_observed_day_count,
            COUNTIF(
                model_next_day_retained_user_count IS NOT NULL
                AND model_next_day_not_retained_user_count IS NOT NULL
            ) AS model_next_day_observed_day_count
        FROM week_daily_fact
        GROUP BY week_start_dt, week_end_dt, week_label
    ),
    -- 试穿核心指标：字段名与需求一致，不带编号。
    core_metrics AS (
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '试穿OKR核心指标' AS metric_type,
            '新用户试穿率' AS metric_name,
            CAST(NULL AS INT64) AS metric_bucket_order,
            CAST(NULL AS STRING) AS metric_bucket,
            day_count AS metric_day_count,
            SAFE_DIVIDE(sum_tryon_new_uv, NULLIF(sum_dau_new_uv, 0)) AS metric_value
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '试穿OKR核心指标',
            '新用户试穿uv',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_tryon_new_uv, day_count)
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '试穿OKR核心指标',
            '老用户试穿率',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_tryon_old_uv, NULLIF(sum_dau_old_uv, 0))
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '试穿OKR核心指标',
            '老用户试穿uv',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_tryon_old_uv, day_count)
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '试穿OKR核心指标',
            '首页信息流试穿 UV',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_home_tryon_uv, day_count)
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '试穿OKR核心指标',
            '首页试穿渗透率',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_home_tryon_uv, NULLIF(sum_home_feeds_uv, 0))
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '试穿OKR核心指标',
            '帖子试穿UV',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_post_tryon_uv, day_count)
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '试穿OKR核心指标',
            '帖子试穿渗透率',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_post_tryon_uv, NULLIF(sum_post_scene_exposure_uv, 0))
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '试穿OKR核心指标',
            '商品详情页试穿 UV',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_product_detail_tryon_uv, day_count)
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '试穿OKR核心指标',
            '商品详情页试穿渗透率',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_product_detail_tryon_uv, NULLIF(sum_product_detail_uv, 0))
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '试穿OKR核心指标',
            'styleme 试穿 UV',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_styleme_uv, day_count)
        FROM weekly_agg
    ),
    -- 商品核心指标：口径对齐 query.sql 11~18，UV 指标按周内日均，率指标按周分子分母汇总后相除。
    product_core_metrics AS (
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '商品OKR核心指标' AS metric_type,
            '商品点击 UV' AS metric_name,
            CAST(NULL AS INT64) AS metric_bucket_order,
            CAST(NULL AS STRING) AS metric_bucket,
            day_count AS metric_day_count,
            SAFE_DIVIDE(sum_product_click_uv, day_count) AS metric_value
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '商品OKR核心指标',
            '商品曝光 UV',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_product_exposure_uv, day_count)
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '商品OKR核心指标',
            '首页商品点击 UV',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_home_product_click_uv, day_count)
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '商品OKR核心指标',
            '首页商品曝光点击率',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_home_product_click_uv, NULLIF(sum_home_product_exposure_uv, 0))
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '商品OKR核心指标',
            '试穿结果页点击 UV',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_tryon_result_click_uv, day_count)
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '商品OKR核心指标',
            '试穿结果页曝光点击率',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_tryon_result_click_uv, NULLIF(sum_tryon_result_exposure_uv, 0))
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '商品OKR核心指标',
            '商城商品点击 UV',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_shop_click_uv, day_count)
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '商品OKR核心指标',
            '商城商品曝光点击率',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_shop_click_uv, NULLIF(sum_shop_exposure_uv, 0))
        FROM weekly_agg
    ),
    -- 留存核心指标：按 prop_user_id 口径输出整体、新用户和行为特征次日留存表现。
    retention_core_metrics AS (
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标' AS metric_type,
            '次日留存率' AS metric_name,
            CAST(NULL AS INT64) AS metric_bucket_order,
            CAST(NULL AS STRING) AS metric_bucket,
            all_next_day_observed_day_count AS metric_day_count,
            SAFE_DIVIDE(
                sum_all_next_day_retained_user_count,
                NULLIF(
                    sum_all_next_day_retained_user_count
                    + sum_all_next_day_not_retained_user_count,
                    0
                )
            ) AS metric_value
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '次日留存用户数',
            NULL,
            NULL,
            all_next_day_observed_day_count,
            SAFE_DIVIDE(
                sum_all_next_day_retained_user_count,
                NULLIF(all_next_day_observed_day_count, 0)
            )
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '平台整体DAU',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_all_user_dau, day_count)
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '7日留存率',
            NULL,
            NULL,
            all_day_7_observed_day_count,
            SAFE_DIVIDE(
                sum_all_day_7_retained_user_count,
                NULLIF(
                    sum_all_day_7_retained_user_count
                    + sum_all_day_7_not_retained_user_count,
                    0
                )
            )
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '7日留存用户数',
            NULL,
            NULL,
            all_day_7_observed_day_count,
            SAFE_DIVIDE(
                sum_all_day_7_retained_user_count,
                NULLIF(all_day_7_observed_day_count, 0)
            )
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '30日留存率',
            NULL,
            NULL,
            all_day_30_observed_day_count,
            SAFE_DIVIDE(
                sum_all_day_30_retained_user_count,
                NULLIF(
                    sum_all_day_30_retained_user_count
                    + sum_all_day_30_not_retained_user_count,
                    0
                )
            )
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '30日留存用户数',
            NULL,
            NULL,
            all_day_30_observed_day_count,
            SAFE_DIVIDE(
                sum_all_day_30_retained_user_count,
                NULLIF(all_day_30_observed_day_count, 0)
            )
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '平台首页曝光点击率',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(
                sum_home_feeds_click_user_count,
                NULLIF(sum_home_feeds_exposure_user_count, 0)
            )
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '平台人均停留时长（min）',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_all_user_duration_sum_sec, NULLIF(sum_all_user_dau, 0)) / 60.0
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '平台人均内容消费',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_all_content_consume_sum, NULLIF(sum_all_user_content_dau, 0))
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '新用户 DAU',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_new_user_dau, day_count)
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '新用户次日留存',
            NULL,
            NULL,
            new_next_day_observed_day_count,
            SAFE_DIVIDE(
                sum_new_next_day_retained_user_count,
                NULLIF(
                    sum_new_next_day_retained_user_count
                    + sum_new_next_day_not_retained_user_count,
                    0
                )
            )
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '新用户人均停留时长（min）',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_new_user_duration_sum_sec, NULLIF(sum_new_user_dau, 0)) / 60.0
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '新用户人均消费内容',
            NULL,
            NULL,
            day_count,
            SAFE_DIVIDE(sum_new_content_consume_sum, NULLIF(sum_new_user_content_dau, 0))
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '使用衣橱用户次日留存率',
            NULL,
            NULL,
            closet_next_day_observed_day_count,
            SAFE_DIVIDE(
                sum_closet_next_day_retained_user_count,
                NULLIF(
                    sum_closet_next_day_retained_user_count
                    + sum_closet_next_day_not_retained_user_count,
                    0
                )
            )
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '使用衣橱用户次日留存用户数',
            NULL,
            NULL,
            closet_next_day_observed_day_count,
            SAFE_DIVIDE(
                sum_closet_next_day_retained_user_count,
                NULLIF(closet_next_day_observed_day_count, 0)
            )
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '浏览商详次日留存率',
            NULL,
            NULL,
            product_detail_next_day_observed_day_count,
            SAFE_DIVIDE(
                sum_product_detail_next_day_retained_user_count,
                NULLIF(
                    sum_product_detail_next_day_retained_user_count
                    + sum_product_detail_next_day_not_retained_user_count,
                    0
                )
            )
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '浏览商详次日留存用户数',
            NULL,
            NULL,
            product_detail_next_day_observed_day_count,
            SAFE_DIVIDE(
                sum_product_detail_next_day_retained_user_count,
                NULLIF(product_detail_next_day_observed_day_count, 0)
            )
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '使用AI试穿次日留存率',
            NULL,
            NULL,
            ai_tryon_next_day_observed_day_count,
            SAFE_DIVIDE(
                sum_ai_tryon_next_day_retained_user_count,
                NULLIF(
                    sum_ai_tryon_next_day_retained_user_count
                    + sum_ai_tryon_next_day_not_retained_user_count,
                    0
                )
            )
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '使用AI试穿次日留存用户数',
            NULL,
            NULL,
            ai_tryon_next_day_observed_day_count,
            SAFE_DIVIDE(
                sum_ai_tryon_next_day_retained_user_count,
                NULLIF(ai_tryon_next_day_observed_day_count, 0)
            )
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '关注账号次日留存率',
            NULL,
            NULL,
            follow_next_day_observed_day_count,
            SAFE_DIVIDE(
                sum_follow_next_day_retained_user_count,
                NULLIF(
                    sum_follow_next_day_retained_user_count
                    + sum_follow_next_day_not_retained_user_count,
                    0
                )
            )
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '关注账号次日留存用户数',
            NULL,
            NULL,
            follow_next_day_observed_day_count,
            SAFE_DIVIDE(
                sum_follow_next_day_retained_user_count,
                NULLIF(follow_next_day_observed_day_count, 0)
            )
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '创建模特次日留存率',
            NULL,
            NULL,
            model_next_day_observed_day_count,
            SAFE_DIVIDE(
                sum_model_next_day_retained_user_count,
                NULLIF(
                    sum_model_next_day_retained_user_count
                    + sum_model_next_day_not_retained_user_count,
                    0
                )
            )
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '创建模特次日留存用户数',
            NULL,
            NULL,
            model_next_day_observed_day_count,
            SAFE_DIVIDE(
                sum_model_next_day_retained_user_count,
                NULLIF(model_next_day_observed_day_count, 0)
            )
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '使用style me用户次日留存率',
            NULL,
            NULL,
            styleme_next_day_observed_day_count,
            SAFE_DIVIDE(
                sum_styleme_next_day_retained_user_count,
                NULLIF(
                    sum_styleme_next_day_retained_user_count
                    + sum_styleme_next_day_not_retained_user_count,
                    0
                )
            )
        FROM weekly_agg
        UNION ALL
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '留存OKR核心指标',
            '使用style me用户次日留存用户数',
            NULL,
            NULL,
            styleme_next_day_observed_day_count,
            SAFE_DIVIDE(
                sum_styleme_next_day_retained_user_count,
                NULLIF(styleme_next_day_observed_day_count, 0)
            )
        FROM weekly_agg
    ),
    -- 人均每日试穿次数分桶：按 query.sql 同口径分桶并统计周内天数占比。
    distribution_daily AS (
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            CASE
                WHEN SAFE_DIVIDE(tryon_pv, NULLIF(tryon_uv_total, 0)) > 0
                    AND SAFE_DIVIDE(tryon_pv, NULLIF(tryon_uv_total, 0)) <= 3 THEN '(0~3]'
                WHEN SAFE_DIVIDE(tryon_pv, NULLIF(tryon_uv_total, 0)) > 3
                    AND SAFE_DIVIDE(tryon_pv, NULLIF(tryon_uv_total, 0)) <= 6 THEN '(3~6]'
                WHEN SAFE_DIVIDE(tryon_pv, NULLIF(tryon_uv_total, 0)) > 6
                    AND SAFE_DIVIDE(tryon_pv, NULLIF(tryon_uv_total, 0)) <= 10 THEN '(6~10]'
                WHEN SAFE_DIVIDE(tryon_pv, NULLIF(tryon_uv_total, 0)) > 10
                    AND SAFE_DIVIDE(tryon_pv, NULLIF(tryon_uv_total, 0)) <= 15 THEN '(10~15]'
                WHEN SAFE_DIVIDE(tryon_pv, NULLIF(tryon_uv_total, 0)) > 15 THEN '15次以上'
                ELSE '0或无试穿UV'
            END AS metric_bucket,
            CASE
                WHEN SAFE_DIVIDE(tryon_pv, NULLIF(tryon_uv_total, 0)) > 0
                    AND SAFE_DIVIDE(tryon_pv, NULLIF(tryon_uv_total, 0)) <= 3 THEN 1
                WHEN SAFE_DIVIDE(tryon_pv, NULLIF(tryon_uv_total, 0)) > 3
                    AND SAFE_DIVIDE(tryon_pv, NULLIF(tryon_uv_total, 0)) <= 6 THEN 2
                WHEN SAFE_DIVIDE(tryon_pv, NULLIF(tryon_uv_total, 0)) > 6
                    AND SAFE_DIVIDE(tryon_pv, NULLIF(tryon_uv_total, 0)) <= 10 THEN 3
                WHEN SAFE_DIVIDE(tryon_pv, NULLIF(tryon_uv_total, 0)) > 10
                    AND SAFE_DIVIDE(tryon_pv, NULLIF(tryon_uv_total, 0)) <= 15 THEN 4
                WHEN SAFE_DIVIDE(tryon_pv, NULLIF(tryon_uv_total, 0)) > 15 THEN 5
                ELSE 9
            END AS metric_bucket_order
        FROM week_daily_fact
    ),
    -- 分桶周聚合层：输出每周各分桶天数与占比。
    distribution_metrics AS (
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '人均每日试穿次数分布' AS metric_type,
            '人均每日试穿次数分布' AS metric_name,
            metric_bucket_order,
            metric_bucket,
            COUNT(*) AS metric_day_count,
            SAFE_DIVIDE(
                COUNT(*),
                SUM(COUNT(*)) OVER (PARTITION BY week_start_dt)
            ) AS metric_value
        FROM distribution_daily
        GROUP BY
            week_start_dt,
            week_end_dt,
            week_label,
            metric_bucket_order,
            metric_bucket
    ),
    -- 人均每日点击次数分桶：口径对齐 query.sql 指标 19，按商品点击 PV(raw_event_id 去重)/商品点击 UV 分桶。
    click_distribution_daily AS (
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            CASE
                WHEN SAFE_DIVIDE(product_click_pv_raw, NULLIF(product_click_uv, 0)) > 0
                    AND SAFE_DIVIDE(product_click_pv_raw, NULLIF(product_click_uv, 0)) <= 3 THEN '(0~3]'
                WHEN SAFE_DIVIDE(product_click_pv_raw, NULLIF(product_click_uv, 0)) > 3
                    AND SAFE_DIVIDE(product_click_pv_raw, NULLIF(product_click_uv, 0)) <= 5 THEN '(3~5]'
                WHEN SAFE_DIVIDE(product_click_pv_raw, NULLIF(product_click_uv, 0)) > 5
                    AND SAFE_DIVIDE(product_click_pv_raw, NULLIF(product_click_uv, 0)) <= 10 THEN '(5~10]'
                WHEN SAFE_DIVIDE(product_click_pv_raw, NULLIF(product_click_uv, 0)) > 10
                    AND SAFE_DIVIDE(product_click_pv_raw, NULLIF(product_click_uv, 0)) <= 15 THEN '(10~15]'
                WHEN SAFE_DIVIDE(product_click_pv_raw, NULLIF(product_click_uv, 0)) > 15 THEN '15次以上'
                ELSE '0或无点击UV'
            END AS metric_bucket,
            CASE
                WHEN SAFE_DIVIDE(product_click_pv_raw, NULLIF(product_click_uv, 0)) > 0
                    AND SAFE_DIVIDE(product_click_pv_raw, NULLIF(product_click_uv, 0)) <= 3 THEN 1
                WHEN SAFE_DIVIDE(product_click_pv_raw, NULLIF(product_click_uv, 0)) > 3
                    AND SAFE_DIVIDE(product_click_pv_raw, NULLIF(product_click_uv, 0)) <= 5 THEN 2
                WHEN SAFE_DIVIDE(product_click_pv_raw, NULLIF(product_click_uv, 0)) > 5
                    AND SAFE_DIVIDE(product_click_pv_raw, NULLIF(product_click_uv, 0)) <= 10 THEN 3
                WHEN SAFE_DIVIDE(product_click_pv_raw, NULLIF(product_click_uv, 0)) > 10
                    AND SAFE_DIVIDE(product_click_pv_raw, NULLIF(product_click_uv, 0)) <= 15 THEN 4
                WHEN SAFE_DIVIDE(product_click_pv_raw, NULLIF(product_click_uv, 0)) > 15 THEN 5
                ELSE 9
            END AS metric_bucket_order
        FROM week_daily_fact
    ),
    -- 点击次数分桶周聚合：按你的要求仅输出“次数”，不输出占比。
    click_distribution_metrics AS (
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            '人均商品点击次数分布' AS metric_type,
            '人均商品点击次数分布' AS metric_name,
            metric_bucket_order,
            metric_bucket,
            COUNT(*) AS metric_day_count,
            CAST(COUNT(*) AS NUMERIC) AS metric_value
        FROM click_distribution_daily
        GROUP BY
            week_start_dt,
            week_end_dt,
            week_label,
            metric_bucket_order,
            metric_bucket
    ),
    -- 核心指标合集：复用 v1 已验证的 metric_name 计算结果，后续统一透视成字段。
    all_core_metrics AS (
        SELECT * FROM core_metrics
        UNION ALL
        SELECT * FROM product_core_metrics
        UNION ALL
        SELECT * FROM retention_core_metrics
    ),
    -- 核心指标宽表：每个 metric_name 固定映射到一个 v2 字段。
    pivoted_core_metrics AS (
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            MAX(IF(metric_name = '新用户试穿率', CAST(metric_value AS NUMERIC), NULL))
                AS new_user_tryon_rate,
            MAX(IF(metric_name = '新用户试穿uv', CAST(metric_value AS NUMERIC), NULL))
                AS new_user_tryon_uv,
            MAX(IF(metric_name = '老用户试穿率', CAST(metric_value AS NUMERIC), NULL))
                AS old_user_tryon_rate,
            MAX(IF(metric_name = '老用户试穿uv', CAST(metric_value AS NUMERIC), NULL))
                AS old_user_tryon_uv,
            MAX(IF(metric_name = '首页信息流试穿 UV', CAST(metric_value AS NUMERIC), NULL))
                AS home_tryon_uv,
            MAX(IF(metric_name = '首页试穿渗透率', CAST(metric_value AS NUMERIC), NULL))
                AS home_tryon_penetration_rate,
            MAX(IF(metric_name = '帖子试穿UV', CAST(metric_value AS NUMERIC), NULL))
                AS post_tryon_uv,
            MAX(IF(metric_name = '帖子试穿渗透率', CAST(metric_value AS NUMERIC), NULL))
                AS post_tryon_penetration_rate,
            MAX(IF(metric_name = '商品详情页试穿 UV', CAST(metric_value AS NUMERIC), NULL))
                AS product_detail_tryon_uv,
            MAX(IF(metric_name = '商品详情页试穿渗透率', CAST(metric_value AS NUMERIC), NULL))
                AS product_detail_tryon_penetration_rate,
            MAX(IF(metric_name = 'styleme 试穿 UV', CAST(metric_value AS NUMERIC), NULL))
                AS styleme_tryon_uv,
            MAX(IF(metric_name = '商品点击 UV', CAST(metric_value AS NUMERIC), NULL))
                AS product_click_uv,
            MAX(IF(metric_name = '商品曝光 UV', CAST(metric_value AS NUMERIC), NULL))
                AS product_exposure_uv,
            MAX(IF(metric_name = '首页商品点击 UV', CAST(metric_value AS NUMERIC), NULL))
                AS home_product_click_uv,
            MAX(IF(metric_name = '首页商品曝光点击率', CAST(metric_value AS NUMERIC), NULL))
                AS home_product_exposure_click_rate,
            MAX(IF(metric_name = '试穿结果页点击 UV', CAST(metric_value AS NUMERIC), NULL))
                AS tryon_result_click_uv,
            MAX(IF(metric_name = '试穿结果页曝光点击率', CAST(metric_value AS NUMERIC), NULL))
                AS tryon_result_exposure_click_rate,
            MAX(IF(metric_name = '商城商品点击 UV', CAST(metric_value AS NUMERIC), NULL))
                AS shop_click_uv,
            MAX(IF(metric_name = '商城商品曝光点击率', CAST(metric_value AS NUMERIC), NULL))
                AS shop_exposure_click_rate,
            MAX(IF(metric_name = '次日留存率', CAST(metric_value AS NUMERIC), NULL))
                AS next_day_retention_rate,
            MAX(IF(metric_name = '次日留存用户数', CAST(metric_value AS NUMERIC), NULL))
                AS next_day_retained_user_count,
            MAX(IF(metric_name = '平台整体DAU', CAST(metric_value AS NUMERIC), NULL))
                AS platform_dau,
            MAX(IF(metric_name = '7日留存率', CAST(metric_value AS NUMERIC), NULL))
                AS day_7_retention_rate,
            MAX(IF(metric_name = '7日留存用户数', CAST(metric_value AS NUMERIC), NULL))
                AS day_7_retained_user_count,
            MAX(IF(metric_name = '30日留存率', CAST(metric_value AS NUMERIC), NULL))
                AS day_30_retention_rate,
            MAX(IF(metric_name = '30日留存用户数', CAST(metric_value AS NUMERIC), NULL))
                AS day_30_retained_user_count,
            MAX(IF(metric_name = '平台首页曝光点击率', CAST(metric_value AS NUMERIC), NULL))
                AS home_feeds_exposure_click_rate,
            MAX(IF(metric_name = '平台人均停留时长（min）', CAST(metric_value AS NUMERIC), NULL))
                AS platform_avg_duration_min,
            MAX(IF(metric_name = '平台人均内容消费', CAST(metric_value AS NUMERIC), NULL))
                AS platform_avg_content_consume,
            MAX(IF(metric_name = '新用户 DAU', CAST(metric_value AS NUMERIC), NULL))
                AS new_user_dau,
            MAX(IF(metric_name = '新用户次日留存', CAST(metric_value AS NUMERIC), NULL))
                AS new_user_next_day_retention_rate,
            MAX(IF(metric_name = '新用户人均停留时长（min）', CAST(metric_value AS NUMERIC), NULL))
                AS new_user_avg_duration_min,
            MAX(IF(metric_name = '新用户人均消费内容', CAST(metric_value AS NUMERIC), NULL))
                AS new_user_avg_content_consume,
            MAX(IF(metric_name = '使用衣橱用户次日留存率', CAST(metric_value AS NUMERIC), NULL))
                AS closet_user_next_day_retention_rate,
            MAX(IF(metric_name = '使用衣橱用户次日留存用户数', CAST(metric_value AS NUMERIC), NULL))
                AS closet_user_next_day_retained_user_count,
            MAX(IF(metric_name = '浏览商详次日留存率', CAST(metric_value AS NUMERIC), NULL))
                AS product_detail_user_next_day_retention_rate,
            MAX(IF(metric_name = '浏览商详次日留存用户数', CAST(metric_value AS NUMERIC), NULL))
                AS product_detail_user_next_day_retained_user_count,
            MAX(IF(metric_name = '使用AI试穿次日留存率', CAST(metric_value AS NUMERIC), NULL))
                AS ai_tryon_user_next_day_retention_rate,
            MAX(IF(metric_name = '使用AI试穿次日留存用户数', CAST(metric_value AS NUMERIC), NULL))
                AS ai_tryon_user_next_day_retained_user_count,
            MAX(IF(metric_name = '关注账号次日留存率', CAST(metric_value AS NUMERIC), NULL))
                AS follow_user_next_day_retention_rate,
            MAX(IF(metric_name = '关注账号次日留存用户数', CAST(metric_value AS NUMERIC), NULL))
                AS follow_user_next_day_retained_user_count,
            MAX(IF(metric_name = '创建模特次日留存率', CAST(metric_value AS NUMERIC), NULL))
                AS model_user_next_day_retention_rate,
            MAX(IF(metric_name = '创建模特次日留存用户数', CAST(metric_value AS NUMERIC), NULL))
                AS model_user_next_day_retained_user_count,
            MAX(IF(metric_name = '使用style me用户次日留存率', CAST(metric_value AS NUMERIC), NULL))
                AS styleme_user_next_day_retention_rate,
            MAX(IF(metric_name = '使用style me用户次日留存用户数', CAST(metric_value AS NUMERIC), NULL))
                AS styleme_user_next_day_retained_user_count
        FROM all_core_metrics
        GROUP BY week_start_dt, week_end_dt, week_label
    ),
    -- 试穿次数分布宽表：每个桶同时保留天数和周内天数占比。
    pivoted_tryon_distribution AS (
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            MAX(IF(metric_bucket = '(0~3]', metric_day_count, NULL))
                AS tryon_avg_daily_count_bucket_0_3_day_count,
            MAX(IF(metric_bucket = '(0~3]', CAST(metric_value AS NUMERIC), NULL))
                AS tryon_avg_daily_count_bucket_0_3_day_ratio,
            MAX(IF(metric_bucket = '(3~6]', metric_day_count, NULL))
                AS tryon_avg_daily_count_bucket_3_6_day_count,
            MAX(IF(metric_bucket = '(3~6]', CAST(metric_value AS NUMERIC), NULL))
                AS tryon_avg_daily_count_bucket_3_6_day_ratio,
            MAX(IF(metric_bucket = '(6~10]', metric_day_count, NULL))
                AS tryon_avg_daily_count_bucket_6_10_day_count,
            MAX(IF(metric_bucket = '(6~10]', CAST(metric_value AS NUMERIC), NULL))
                AS tryon_avg_daily_count_bucket_6_10_day_ratio,
            MAX(IF(metric_bucket = '(10~15]', metric_day_count, NULL))
                AS tryon_avg_daily_count_bucket_10_15_day_count,
            MAX(IF(metric_bucket = '(10~15]', CAST(metric_value AS NUMERIC), NULL))
                AS tryon_avg_daily_count_bucket_10_15_day_ratio,
            MAX(IF(metric_bucket = '15次以上', metric_day_count, NULL))
                AS tryon_avg_daily_count_bucket_gt_15_day_count,
            MAX(IF(metric_bucket = '15次以上', CAST(metric_value AS NUMERIC), NULL))
                AS tryon_avg_daily_count_bucket_gt_15_day_ratio,
            MAX(IF(metric_bucket = '0或无试穿UV', metric_day_count, NULL))
                AS tryon_avg_daily_count_bucket_zero_or_null_uv_day_count,
            MAX(IF(metric_bucket = '0或无试穿UV', CAST(metric_value AS NUMERIC), NULL))
                AS tryon_avg_daily_count_bucket_zero_or_null_uv_day_ratio
        FROM distribution_metrics
        GROUP BY week_start_dt, week_end_dt, week_label
    ),
    -- 商品点击次数分布宽表：v1 该分布的 metric_value 即周内桶天数。
    pivoted_product_click_distribution AS (
        SELECT
            week_start_dt,
            week_end_dt,
            week_label,
            MAX(IF(metric_bucket = '(0~3]', metric_day_count, NULL))
                AS product_click_avg_daily_count_bucket_0_3_day_count,
            MAX(IF(metric_bucket = '(3~5]', metric_day_count, NULL))
                AS product_click_avg_daily_count_bucket_3_5_day_count,
            MAX(IF(metric_bucket = '(5~10]', metric_day_count, NULL))
                AS product_click_avg_daily_count_bucket_5_10_day_count,
            MAX(IF(metric_bucket = '(10~15]', metric_day_count, NULL))
                AS product_click_avg_daily_count_bucket_10_15_day_count,
            MAX(IF(metric_bucket = '15次以上', metric_day_count, NULL))
                AS product_click_avg_daily_count_bucket_gt_15_day_count,
            MAX(IF(metric_bucket = '0或无点击UV', metric_day_count, NULL))
                AS product_click_avg_daily_count_bucket_zero_or_null_uv_day_count
        FROM click_distribution_metrics
        GROUP BY week_start_dt, week_end_dt, week_label
    )
    SELECT
        p.week_start_dt,
        p.week_end_dt,
        p.week_label,
        w.day_count AS week_day_count,
        w.all_next_day_observed_day_count,
        w.new_next_day_observed_day_count,
        w.closet_next_day_observed_day_count,
        w.product_detail_next_day_observed_day_count,
        w.ai_tryon_next_day_observed_day_count,
        w.follow_next_day_observed_day_count,
        w.model_next_day_observed_day_count,
        w.all_day_7_observed_day_count,
        w.all_day_30_observed_day_count,
        w.styleme_next_day_observed_day_count,
        p.new_user_tryon_rate,
        p.new_user_tryon_uv,
        p.old_user_tryon_rate,
        p.old_user_tryon_uv,
        p.home_tryon_uv,
        p.home_tryon_penetration_rate,
        p.post_tryon_uv,
        p.post_tryon_penetration_rate,
        p.product_detail_tryon_uv,
        p.product_detail_tryon_penetration_rate,
        p.styleme_tryon_uv,
        p.product_click_uv,
        p.product_exposure_uv,
        p.home_product_click_uv,
        p.home_product_exposure_click_rate,
        p.tryon_result_click_uv,
        p.tryon_result_exposure_click_rate,
        p.shop_click_uv,
        p.shop_exposure_click_rate,
        p.next_day_retention_rate,
        p.next_day_retained_user_count,
        p.platform_dau,
        p.day_7_retention_rate,
        p.day_7_retained_user_count,
        p.day_30_retention_rate,
        p.day_30_retained_user_count,
        p.home_feeds_exposure_click_rate,
        p.platform_avg_duration_min,
        p.platform_avg_content_consume,
        p.new_user_dau,
        p.new_user_next_day_retention_rate,
        p.new_user_avg_duration_min,
        p.new_user_avg_content_consume,
        p.closet_user_next_day_retention_rate,
        p.closet_user_next_day_retained_user_count,
        p.product_detail_user_next_day_retention_rate,
        p.product_detail_user_next_day_retained_user_count,
        p.ai_tryon_user_next_day_retention_rate,
        p.ai_tryon_user_next_day_retained_user_count,
        p.follow_user_next_day_retention_rate,
        p.follow_user_next_day_retained_user_count,
        p.model_user_next_day_retention_rate,
        p.model_user_next_day_retained_user_count,
        p.styleme_user_next_day_retention_rate,
        p.styleme_user_next_day_retained_user_count,
        COALESCE(td.tryon_avg_daily_count_bucket_0_3_day_count, 0)
            AS tryon_avg_daily_count_bucket_0_3_day_count,
        COALESCE(td.tryon_avg_daily_count_bucket_0_3_day_ratio, 0)
            AS tryon_avg_daily_count_bucket_0_3_day_ratio,
        COALESCE(td.tryon_avg_daily_count_bucket_3_6_day_count, 0)
            AS tryon_avg_daily_count_bucket_3_6_day_count,
        COALESCE(td.tryon_avg_daily_count_bucket_3_6_day_ratio, 0)
            AS tryon_avg_daily_count_bucket_3_6_day_ratio,
        COALESCE(td.tryon_avg_daily_count_bucket_6_10_day_count, 0)
            AS tryon_avg_daily_count_bucket_6_10_day_count,
        COALESCE(td.tryon_avg_daily_count_bucket_6_10_day_ratio, 0)
            AS tryon_avg_daily_count_bucket_6_10_day_ratio,
        COALESCE(td.tryon_avg_daily_count_bucket_10_15_day_count, 0)
            AS tryon_avg_daily_count_bucket_10_15_day_count,
        COALESCE(td.tryon_avg_daily_count_bucket_10_15_day_ratio, 0)
            AS tryon_avg_daily_count_bucket_10_15_day_ratio,
        COALESCE(td.tryon_avg_daily_count_bucket_gt_15_day_count, 0)
            AS tryon_avg_daily_count_bucket_gt_15_day_count,
        COALESCE(td.tryon_avg_daily_count_bucket_gt_15_day_ratio, 0)
            AS tryon_avg_daily_count_bucket_gt_15_day_ratio,
        COALESCE(td.tryon_avg_daily_count_bucket_zero_or_null_uv_day_count, 0)
            AS tryon_avg_daily_count_bucket_zero_or_null_uv_day_count,
        COALESCE(td.tryon_avg_daily_count_bucket_zero_or_null_uv_day_ratio, 0)
            AS tryon_avg_daily_count_bucket_zero_or_null_uv_day_ratio,
        COALESCE(cd.product_click_avg_daily_count_bucket_0_3_day_count, 0)
            AS product_click_avg_daily_count_bucket_0_3_day_count,
        COALESCE(cd.product_click_avg_daily_count_bucket_3_5_day_count, 0)
            AS product_click_avg_daily_count_bucket_3_5_day_count,
        COALESCE(cd.product_click_avg_daily_count_bucket_5_10_day_count, 0)
            AS product_click_avg_daily_count_bucket_5_10_day_count,
        COALESCE(cd.product_click_avg_daily_count_bucket_10_15_day_count, 0)
            AS product_click_avg_daily_count_bucket_10_15_day_count,
        COALESCE(cd.product_click_avg_daily_count_bucket_gt_15_day_count, 0)
            AS product_click_avg_daily_count_bucket_gt_15_day_count,
        COALESCE(cd.product_click_avg_daily_count_bucket_zero_or_null_uv_day_count, 0)
            AS product_click_avg_daily_count_bucket_zero_or_null_uv_day_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM pivoted_core_metrics p
    JOIN weekly_agg w
        USING (week_start_dt, week_end_dt, week_label)
    LEFT JOIN pivoted_tryon_distribution td
        USING (week_start_dt, week_end_dt, week_label)
    LEFT JOIN pivoted_product_click_distribution cd
        USING (week_start_dt, week_end_dt, week_label);
    """
    logging.info("开始处理: ads_weekly_ai_fashion_tryon_okr_v2")
    job = client.query(query)
    job.result()
    logging.info("ads_weekly_ai_fashion_tryon_okr_v2 刷新完成, 处理周起始日: %s", week_starts)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    week_starts = get_week_starts_to_process()
    logging.info("待处理周起始日: %s", week_starts if week_starts else "[]")

    if not week_starts:
        logging.info("没有新的 ads_weekly_ai_fashion_tryon_okr_v2 周需要处理")
        raise SystemExit(0)

    run_ads_weekly_ai_fashion_tryon_okr_v2(week_starts)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("ads_weekly_ai_fashion_tryon_okr_v2 完成, 耗时: %.1f 秒", elapsed)
