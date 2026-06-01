from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_weekly_ai_fashion_tryon_okr"
SOURCE_TABLE_NAME = "dws_ai_fashion_tryon_okr_daily"
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


def get_week_starts_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    周级试穿 OKR ADS 只消费 dws_ai_fashion_tryon_okr_daily。

    规则：
    - 目标表不存在或为空时，按 DWS 已落地历史周全量初始化
    - 目标表已存在时，只重算最近 6 小时 DWS 变更映射出的业务周
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT DATE_TRUNC(dt, WEEK(MONDAY)) AS week_start_dt
        FROM `{source_table_id()}`
        ORDER BY week_start_dt
        """
        results = client.query(query).result()
        return [row.week_start_dt for row in results]

    query = f"""
    SELECT DISTINCT DATE_TRUNC(dt, WEEK(MONDAY)) AS week_start_dt
    FROM `{source_table_id()}`
    WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ORDER BY week_start_dt
    """
    results = client.query(query).result()
    return [row.week_start_dt for row in results]


def run_ads_weekly_ai_fashion_tryon_okr(week_starts: list[date]) -> None:
    """
    产出 AI-Fashion 试穿 OKR 周报表。

    粒度：
    - week_start_dt × metric_name
    - 人均每日试穿次数分布额外展开 bucket 维度

    说明：
    - 周窗口固定按周一到周日
    - 周级 UV 字段统一采用“周内日值求和 / 天数”的日均口径
    - 周级率指标统一采用“周内分子求和 / 周内分母求和”口径
    """
    if not week_starts:
        logging.info("没有新的 ads_weekly_ai_fashion_tryon_okr 周需要处理")
        return

    week_starts_str = dates_to_sql_list(week_starts)

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE week_start_dt IN ({week_starts_str});

    INSERT INTO `{target_table_id()}`
    (
        week_start_dt,
        week_end_dt,
        week_label,
        metric_type,
        metric_name,
        metric_bucket_order,
        metric_bucket,
        metric_day_count,
        metric_value,
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
    -- 周内日事实：从 DWS 读取每天分子分母并补 0，避免缺天漏算。
    week_daily_fact AS (
        SELECT
            c.week_start_dt,
            c.week_end_dt,
            c.week_label,
            c.dt,
            COALESCE(f.dau_new_uv, 0) AS dau_new_uv,
            COALESCE(f.dau_old_uv, 0) AS dau_old_uv,
            COALESCE(f.tryon_new_uv, 0) AS tryon_new_uv,
            COALESCE(f.tryon_old_uv, 0) AS tryon_old_uv,
            COALESCE(f.home_tryon_uv, 0) AS home_tryon_uv,
            COALESCE(f.home_feeds_uv, 0) AS home_feeds_uv,
            COALESCE(f.post_tryon_uv, 0) AS post_tryon_uv,
            COALESCE(f.post_scene_exposure_uv, 0) AS post_scene_exposure_uv,
            COALESCE(f.product_detail_tryon_uv, 0) AS product_detail_tryon_uv,
            COALESCE(f.product_detail_uv, 0) AS product_detail_uv,
            COALESCE(f.styleme_uv, 0) AS styleme_uv,
            COALESCE(f.tryon_uv_total, 0) AS tryon_uv_total,
            COALESCE(f.tryon_pv, 0) AS tryon_pv
        FROM week_calendar c
        LEFT JOIN `{source_table_id()}` f
            ON f.dt = c.dt
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
            SUM(styleme_uv) AS sum_styleme_uv
        FROM week_daily_fact
        GROUP BY week_start_dt, week_end_dt, week_label
    ),
    -- 核心指标展开：字段名与需求一致，不带编号。
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
    )
    SELECT
        week_start_dt,
        week_end_dt,
        week_label,
        metric_type,
        metric_name,
        metric_bucket_order,
        metric_bucket,
        metric_day_count,
        CAST(metric_value AS NUMERIC) AS metric_value,
        CURRENT_TIMESTAMP() AS update_time
    FROM core_metrics
    UNION ALL
    SELECT
        week_start_dt,
        week_end_dt,
        week_label,
        metric_type,
        metric_name,
        metric_bucket_order,
        metric_bucket,
        metric_day_count,
        CAST(metric_value AS NUMERIC) AS metric_value,
        CURRENT_TIMESTAMP() AS update_time
    FROM distribution_metrics;
    """
    logging.info("开始处理: ads_weekly_ai_fashion_tryon_okr")
    job = client.query(query)
    job.result()
    logging.info("ads_weekly_ai_fashion_tryon_okr 刷新完成, 处理周起始日: %s", week_starts)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    week_starts = get_week_starts_to_process()
    logging.info("待处理周起始日: %s", week_starts if week_starts else "[]")

    if not week_starts:
        logging.info("没有新的 ads_weekly_ai_fashion_tryon_okr 周需要处理")
        raise SystemExit(0)

    run_ads_weekly_ai_fashion_tryon_okr(week_starts)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("ads_weekly_ai_fashion_tryon_okr 完成, 耗时: %.1f 秒", elapsed)
