import argparse
from datetime import date, datetime, timedelta, timezone
import logging
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "dws_social_attribute_daily_v2"
SOURCE_TABLE_NAME = "dws_social_video_state_daily_v2"
ATTRIBUTE_DIM_TABLE_NAME = "dim_social_video_attribute"
TORONTO_TZ = "America/Toronto"
ATTRIBUTE_START_DATE = date(2026, 7, 15)
REBUILD_DAYS = 30
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="构建 Social Content 属性维度日事实")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="从属性数据起点全量重刷，而不是默认最近 30 天",
    )
    return parser.parse_args()


def target_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


def source_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{SOURCE_TABLE_NAME}"


def attribute_dim_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{ATTRIBUTE_DIM_TABLE_NAME}"


def target_table_exists() -> bool:
    try:
        client.get_table(target_table_id())
        return True
    except NotFound:
        return False


def dates_to_sql_list(dates: list[date]) -> str:
    return ", ".join(f"DATE '{value}'" for value in dates)


def get_incremental_dates() -> list[date]:
    end_date = datetime.now(ZoneInfo(TORONTO_TZ)).date()
    start_date = max(ATTRIBUTE_START_DATE, end_date - timedelta(days=REBUILD_DAYS - 1))
    return [
        start_date + timedelta(days=offset)
        for offset in range((end_date - start_date).days + 1)
    ]


def get_full_dates() -> list[date]:
    query = f"""
    SELECT DISTINCT s.dt
    FROM `{source_table_id()}` s
    INNER JOIN `{attribute_dim_table_id()}` a USING (video_key)
    WHERE s.dt >= DATE '{ATTRIBUTE_START_DATE}'
    ORDER BY s.dt
    """
    return [row.dt for row in client.query(query).result()]


def build_select_sql(dates: list[date]) -> str:
    dates_sql = dates_to_sql_list(dates)
    max_date = max(dates)
    return f"""
    WITH
    -- 读取属性数据起点到本批上限的稠密视频日状态；任务 JSON 已在 DIM 层解析。
    attribute_state AS (
        SELECT
            s.dt,
            s.channel,
            s.creator_id,
            s.video_key,
            s.published_dt,
            s.published_ts_utc,
            s.day_end_views,
            s.day_end_likes,
            a.unified_video_key,
            a.video_attribute,
            a.video_brand,
            a.blogger_attribute
        FROM `{source_table_id()}` s
        INNER JOIN `{attribute_dim_table_id()}` a USING (video_key)
        WHERE s.dt BETWEEN DATE '{ATTRIBUTE_START_DATE}' AND DATE '{max_date}'
    ),
    -- 视频指标同时展开真实渠道与全部渠道口径。
    video_scoped_state_raw AS (
        SELECT
            dt,
            channel,
            video_key AS reporting_video_key,
            published_dt,
            day_end_views,
            day_end_likes,
            video_attribute,
            video_brand
        FROM attribute_state

        UNION ALL

        SELECT
            dt,
            '全部渠道' AS channel,
            unified_video_key AS reporting_video_key,
            published_dt,
            day_end_views,
            day_end_likes,
            video_attribute,
            video_brand
        FROM attribute_state
    ),
    -- 全部渠道先按统一上传任务折叠跨平台视频。
    video_scoped_state AS (
        SELECT
            dt,
            channel,
            reporting_video_key,
            MIN(published_dt) AS published_dt,
            SUM(day_end_views) AS day_end_views,
            SUM(day_end_likes) AS day_end_likes,
            video_attribute,
            video_brand
        FROM video_scoped_state_raw
        GROUP BY dt, channel, reporting_video_key, video_attribute, video_brand
    ),
    -- 在逻辑视频粒度计算日增量与新增视频标记。
    video_scoped_daily AS (
        SELECT
            *,
            day_end_views - LAG(day_end_views, 1, 0) OVER (
                PARTITION BY channel, reporting_video_key
                ORDER BY dt
            ) AS daily_view_increment,
            day_end_likes - LAG(day_end_likes, 1, 0) OVER (
                PARTITION BY channel, reporting_video_key
                ORDER BY dt
            ) AS daily_like_increment,
            day_end_views > 0
                AND LAG(day_end_views, 1, 0) OVER (
                    PARTITION BY channel, reporting_video_key
                    ORDER BY dt
                ) = 0 AS is_new_video
        FROM video_scoped_state
    ),
    -- 汇总视频属性每日指标的可加总分子分母。
    video_daily_metrics AS (
        SELECT
            dt,
            channel,
            video_attribute,
            video_brand,
            SUM(daily_view_increment) AS daily_view_increment,
            SUM(daily_like_increment) AS daily_like_increment,
            COUNT(DISTINCT IF(is_new_video, reporting_video_key, NULL))
                AS new_video_count,
            SUM(IF(is_new_video, day_end_views, 0))
                AS new_video_contributed_view_count
        FROM video_scoped_daily
        WHERE dt IN ({dates_sql})
        GROUP BY dt, channel, video_attribute, video_brand
    ),
    -- 计算各展示口径下逻辑视频首采正播放日。
    first_positive_view AS (
        SELECT
            channel,
            reporting_video_key,
            MIN(IF(day_end_views > 0, dt, NULL)) AS first_positive_view_dt
        FROM video_scoped_state
        GROUP BY channel, reporting_video_key
    ),
    -- 汇总首采正播放满 7 天 cohort 的播放分子与视频分母。
    published_7d_metrics AS (
        SELECT
            s.dt,
            s.channel,
            s.video_attribute,
            s.video_brand,
            COUNT(DISTINCT s.reporting_video_key) AS published_7d_cohort_video_count,
            SUM(s.day_end_views) AS published_7d_cohort_view_count
        FROM video_scoped_state s
        INNER JOIN first_positive_view f
            ON s.channel = f.channel
            AND s.reporting_video_key = f.reporting_video_key
        WHERE s.dt IN ({dates_sql})
            AND f.first_positive_view_dt = DATE_SUB(s.dt, INTERVAL 6 DAY)
        GROUP BY s.dt, s.channel, s.video_attribute, s.video_brand
    ),
    -- 汇总发布时间 cohort 各观察窗的累计播放与视频数。
    published_horizon_metrics AS (
        SELECT
            published_dt AS dt,
            channel,
            video_attribute,
            video_brand,
            DATE_DIFF(dt, published_dt, DAY) AS horizon_day,
            SUM(day_end_views) AS view_count,
            COUNT(DISTINCT reporting_video_key) AS video_count
        FROM video_scoped_state
        WHERE published_dt IN ({dates_sql})
            AND DATE_DIFF(dt, published_dt, DAY) IN (0, 1, 3, 7, 14)
        GROUP BY published_dt, channel, video_attribute, video_brand, horizon_day
    ),
    -- 将 cohort 观察窗转成宽表，保留各平均值的分子分母。
    published_horizon_pivot AS (
        SELECT
            dt,
            channel,
            video_attribute,
            video_brand,
            SUM(IF(horizon_day = 0, view_count, 0)) AS published_day0_view_count,
            SUM(IF(horizon_day = 0, video_count, 0)) AS published_day0_video_count,
            SUM(IF(horizon_day = 1, view_count, 0)) AS published_day1_view_count,
            SUM(IF(horizon_day = 1, video_count, 0)) AS published_day1_video_count,
            SUM(IF(horizon_day = 3, view_count, 0)) AS published_day3_view_count,
            SUM(IF(horizon_day = 3, video_count, 0)) AS published_day3_video_count,
            SUM(IF(horizon_day = 7, view_count, 0)) AS published_day7_view_count,
            SUM(IF(horizon_day = 7, video_count, 0)) AS published_day7_video_count,
            SUM(IF(horizon_day = 14, view_count, 0)) AS published_day14_view_count,
            SUM(IF(horizon_day = 14, video_count, 0)) AS published_day14_video_count
        FROM published_horizon_metrics
        GROUP BY dt, channel, video_attribute, video_brand
    ),
    -- 视频指标输出键以每日状态为主，兼容只出现 cohort 指标的组合。
    video_output_keys AS (
        SELECT dt, channel, video_attribute, video_brand FROM video_daily_metrics
        UNION DISTINCT
        SELECT dt, channel, video_attribute, video_brand FROM published_7d_metrics
        UNION DISTINCT
        SELECT dt, channel, video_attribute, video_brand FROM published_horizon_pivot
    ),
    -- 组装 metric_scope=video 的统一 DWS 行。
    video_output AS (
        SELECT
            k.dt,
            k.channel,
            'video' AS metric_scope,
            k.video_attribute,
            k.video_brand,
            CAST(NULL AS STRING) AS blogger_attribute,
            COALESCE(d.daily_view_increment, 0) AS daily_view_increment,
            COALESCE(d.daily_like_increment, 0) AS daily_like_increment,
            COALESCE(d.new_video_count, 0) AS new_video_count,
            COALESCE(d.new_video_contributed_view_count, 0)
                AS new_video_contributed_view_count,
            COALESCE(p7.published_7d_cohort_video_count, 0)
                AS published_7d_cohort_video_count,
            COALESCE(p7.published_7d_cohort_view_count, 0)
                AS published_7d_cohort_view_count,
            ph.published_day0_view_count,
            ph.published_day0_video_count,
            ph.published_day1_view_count,
            ph.published_day1_video_count,
            ph.published_day3_view_count,
            ph.published_day3_video_count,
            ph.published_day7_view_count,
            ph.published_day7_video_count,
            ph.published_day14_view_count,
            ph.published_day14_video_count,
            CAST(NULL AS INT64) AS creator_count_to_prev_day,
            CAST(NULL AS INT64) AS latest_3_sample_creator_count,
            CAST(NULL AS INT64) AS latest_3_sample_video_count,
            CAST(NULL AS INT64) AS latest_3_sample_like_count,
            CAST(NULL AS INT64) AS latest_3_sample_view_count
        FROM video_output_keys k
        LEFT JOIN video_daily_metrics d
            USING (dt, channel, video_attribute, video_brand)
        LEFT JOIN published_7d_metrics p7
            USING (dt, channel, video_attribute, video_brand)
        LEFT JOIN published_horizon_pivot ph
            USING (dt, channel, video_attribute, video_brand)
    ),
    -- 博主指标先在原始视频粒度计算日增量。
    creator_video_daily AS (
        SELECT
            *,
            day_end_views - LAG(day_end_views, 1, 0) OVER (
                PARTITION BY video_key
                ORDER BY dt
            ) AS daily_view_increment
        FROM attribute_state
    ),
    -- 博主指标展开真实渠道与全部渠道；全部渠道按 channel + creator_id 区分博主。
    creator_scoped_daily AS (
        SELECT
            dt,
            channel,
            creator_id AS reporting_creator_key,
            video_key,
            published_dt,
            published_ts_utc,
            day_end_views,
            day_end_likes,
            daily_view_increment,
            blogger_attribute
        FROM creator_video_daily

        UNION ALL

        SELECT
            dt,
            '全部渠道' AS channel,
            CONCAT(channel, ':', creator_id) AS reporting_creator_key,
            video_key,
            published_dt,
            published_ts_utc,
            day_end_views,
            day_end_likes,
            daily_view_increment,
            blogger_attribute
        FROM creator_video_daily
    ),
    -- 汇总新增 Views / 截至前一天博主数的分子分母。
    creator_daily_metrics AS (
        SELECT
            dt,
            channel,
            blogger_attribute,
            SUM(daily_view_increment) AS daily_view_increment,
            COUNT(DISTINCT IF(published_dt < dt, reporting_creator_key, NULL))
                AS creator_count_to_prev_day
        FROM creator_scoped_daily
        WHERE dt IN ({dates_sql})
        GROUP BY dt, channel, blogger_attribute
    ),
    -- 每个日期、渠道、博主属性与博主内取已有正播放的最新 3 条。
    latest_3_pool AS (
        SELECT * EXCEPT(rn)
        FROM (
            SELECT
                dt,
                channel,
                blogger_attribute,
                reporting_creator_key,
                video_key,
                day_end_views,
                day_end_likes,
                ROW_NUMBER() OVER (
                    PARTITION BY dt, channel, blogger_attribute, reporting_creator_key
                    ORDER BY published_ts_utc DESC, video_key DESC
                ) AS rn
            FROM creator_scoped_daily
            WHERE dt IN ({dates_sql})
                AND reporting_creator_key IS NOT NULL
                AND day_end_views > 0
        )
        WHERE rn <= 3
    ),
    -- 汇总最新 3 条样本池的 likes/views 与样本量。
    latest_3_metrics AS (
        SELECT
            dt,
            channel,
            blogger_attribute,
            COUNT(DISTINCT reporting_creator_key) AS latest_3_sample_creator_count,
            COUNT(*) AS latest_3_sample_video_count,
            SUM(day_end_likes) AS latest_3_sample_like_count,
            SUM(day_end_views) AS latest_3_sample_view_count
        FROM latest_3_pool
        GROUP BY dt, channel, blogger_attribute
    ),
    -- 组装 metric_scope=creator 的统一 DWS 行。
    creator_output AS (
        SELECT
            d.dt,
            d.channel,
            'creator' AS metric_scope,
            CAST(NULL AS STRING) AS video_attribute,
            CAST(NULL AS STRING) AS video_brand,
            d.blogger_attribute,
            d.daily_view_increment,
            CAST(NULL AS INT64) AS daily_like_increment,
            CAST(NULL AS INT64) AS new_video_count,
            CAST(NULL AS INT64) AS new_video_contributed_view_count,
            CAST(NULL AS INT64) AS published_7d_cohort_video_count,
            CAST(NULL AS INT64) AS published_7d_cohort_view_count,
            CAST(NULL AS INT64) AS published_day0_view_count,
            CAST(NULL AS INT64) AS published_day0_video_count,
            CAST(NULL AS INT64) AS published_day1_view_count,
            CAST(NULL AS INT64) AS published_day1_video_count,
            CAST(NULL AS INT64) AS published_day3_view_count,
            CAST(NULL AS INT64) AS published_day3_video_count,
            CAST(NULL AS INT64) AS published_day7_view_count,
            CAST(NULL AS INT64) AS published_day7_video_count,
            CAST(NULL AS INT64) AS published_day14_view_count,
            CAST(NULL AS INT64) AS published_day14_video_count,
            d.creator_count_to_prev_day,
            COALESCE(l.latest_3_sample_creator_count, 0)
                AS latest_3_sample_creator_count,
            COALESCE(l.latest_3_sample_video_count, 0)
                AS latest_3_sample_video_count,
            COALESCE(l.latest_3_sample_like_count, 0)
                AS latest_3_sample_like_count,
            COALESCE(l.latest_3_sample_view_count, 0)
                AS latest_3_sample_view_count
        FROM creator_daily_metrics d
        LEFT JOIN latest_3_metrics l
            USING (dt, channel, blogger_attribute)
    )
    SELECT *, CURRENT_TIMESTAMP() AS update_time
    FROM video_output

    UNION ALL

    SELECT *, CURRENT_TIMESTAMP() AS update_time
    FROM creator_output
    """


def run_dws_social_attribute_daily(dates: list[date]) -> None:
    if not dates:
        logging.info("没有新的 %s 日期需要处理", TABLE_NAME)
        return
    if not target_table_exists():
        raise RuntimeError(f"目标表不存在，请先执行 DDL: {target_table_id()}")

    min_date = min(dates)
    max_date = max(dates)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    target_table = target_table_id()
    staging_table = f"{target_table}_tmp_{run_id}"
    changed_table = f"{target_table}_changed_{run_id}"
    target_dropped = False
    key_expr = "CONCAT(CAST(dt AS STRING), '|', channel, '|', metric_scope, '|', COALESCE(video_attribute, ''), '|', COALESCE(video_brand, ''), '|', COALESCE(blogger_attribute, ''))"

    logging.info("开始处理 %s, 日期: %s", TABLE_NAME, dates)
    try:
        client.query(
            f"""
            CREATE TABLE `{staging_table}` AS
            SELECT *
            FROM `{target_table}`
            WHERE 1 = 0
            """
        ).result()
        client.query(
            f"""
            INSERT INTO `{staging_table}`
            {build_select_sql(dates)}
            """
        ).result()
        validation_row = list(
            client.query(
                f"""
                SELECT
                    COUNT(*) AS row_count,
                    COUNT(DISTINCT {key_expr}) AS unique_key_count,
                    COUNTIF(metric_scope NOT IN ('video', 'creator')) AS invalid_scope_count
                FROM `{staging_table}`
                """
            ).result()
        )[0]
        if validation_row.row_count != validation_row.unique_key_count:
            raise ValueError("dws_social_attribute_daily_v2 校验失败：业务键重复")
        if validation_row.invalid_scope_count != 0:
            raise ValueError("dws_social_attribute_daily_v2 校验失败：metric_scope 非法")

        client.query(
            f"""
            CREATE TABLE `{changed_table}`
            PARTITION BY dt
            CLUSTER BY metric_scope, channel, video_attribute, blogger_attribute AS
            SELECT *
            FROM `{target_table}`
            WHERE dt < DATE '{min_date}' OR dt > DATE '{max_date}'

            UNION ALL

            SELECT *
            FROM `{staging_table}`
            """
        ).result()
        client.query(f"DROP TABLE `{target_table}`").result()
        target_dropped = True
        client.query(
            f"ALTER TABLE `{changed_table}` RENAME TO {TABLE_NAME}"
        ).result()
        target_dropped = False
        client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
    except Exception:
        if target_dropped:
            try:
                client.query(
                    f"ALTER TABLE `{changed_table}` RENAME TO {TABLE_NAME}"
                ).result()
            except Exception:
                logging.exception("目标表恢复失败，保留替换表: %s", changed_table)
        else:
            client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
            client.query(f"DROP TABLE IF EXISTS `{changed_table}`").result()
        raise

    logging.info("%s 刷新完成", TABLE_NAME)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)
    args = parse_args()
    dates = get_full_dates() if args.full_refresh else get_incremental_dates()
    start_time = datetime.now(timezone.utc)
    run_dws_social_attribute_daily(dates)
    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
