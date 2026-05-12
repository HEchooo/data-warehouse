from datetime import date, datetime, timedelta, timezone
import logging
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_daily_formal_video_increment"
DIM_VIDEO_TABLE_NAME = "dim_video"
SOCIAL_VIDEO_STATE_TABLE_NAME = "dws_social_video_state_daily"
EASTERN_TZ = "America/New_York"
REPORT_START_DATE = date(2026, 3, 30)
LTV_DAYS = 7
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "ads" / TABLE_NAME

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


def target_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


def dim_video_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{DIM_VIDEO_TABLE_NAME}"


def social_video_state_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{SOCIAL_VIDEO_STATE_TABLE_NAME}"


def target_table_exists() -> bool:
    try:
        client.get_table(target_table_id())
        return True
    except NotFound:
        return False


def get_dates_to_process() -> list[date]:
    end_date = datetime.now(ZoneInfo(EASTERN_TZ)).date()
    if end_date < REPORT_START_DATE:
        return []

    total_days = (end_date - REPORT_START_DATE).days + 1
    return [REPORT_START_DATE + timedelta(days=offset) for offset in range(total_days)]


def build_select_sql(dates: list[date]) -> str:
    min_date = min(dates)
    max_date = max(dates)
    seed_date = min_date - timedelta(days=1)
    ltv_offset_days = LTV_DAYS - 1

    return f"""
    WITH
    -- 当前报表固定从 2026-03-30 美东日期开始展示。
    params AS (
        SELECT
            DATE('{min_date}') AS min_dt,
            DATE('{max_date}') AS max_dt,
            DATE('{seed_date}') AS seed_dt
    ),
    -- 目标日期列表，保证没有新增数据的日期也产出 0。
    date_list AS (
        SELECT dt
        FROM UNNEST(GENERATE_DATE_ARRAY(
            (SELECT min_dt FROM params),
            (SELECT max_dt FROM params)
        )) AS dt
    ),
    -- 正式号视频集合，正式号只由 dim_video 的 tag 标记承接。
    formal_video_dim AS (
        SELECT
            channel,
            creator_id,
            video_key,
            published_dt,
            DATE(formal_tag_created_ts_utc, '{EASTERN_TZ}') AS formal_tag_created_dt
        FROM `{dim_video_table_id()}`
        WHERE is_formal_video
            AND formal_tag_created_ts_utc IS NOT NULL
    ),
    -- 正式号视频每日状态；多取 seed_dt 用于首日差分。
    formal_video_state AS (
        SELECT
            s.dt,
            s.channel,
            s.creator_id,
            s.video_key,
            d.published_dt,
            d.formal_tag_created_dt,
            s.day_end_views,
            s.day_end_likes
        FROM `{social_video_state_table_id()}` s
        INNER JOIN formal_video_dim d
            ON s.video_key = d.video_key
        WHERE s.dt BETWEEN (SELECT seed_dt FROM params) AND (SELECT max_dt FROM params)
    ),
    -- 对正式号视频做今日对比昨日的累计值差分。
    formal_video_daily AS (
        SELECT
            dt,
            channel,
            creator_id,
            video_key,
            published_dt,
            formal_tag_created_dt,
            day_end_views,
            day_end_likes,
            day_end_views - LAG(day_end_views, 1, 0) OVER (
                PARTITION BY video_key
                ORDER BY dt
            ) AS daily_view_increment,
            day_end_likes - LAG(day_end_likes, 1, 0) OVER (
                PARTITION BY video_key
                ORDER BY dt
            ) AS daily_like_increment
        FROM formal_video_state
    ),
    -- 计算每个正式号视频的首次采集正播放日期。
    video_first_positive_view AS (
        SELECT
            video_key,
            MIN(dt) AS first_positive_view_dt
        FROM formal_video_state
        WHERE day_end_views > 0
        GROUP BY video_key
    ),
    -- 汇总图 1 和图 5 所需的每日新增 views / likes。
    daily_increment_metrics AS (
        SELECT
            dt,
            COALESCE(SUM(daily_view_increment), 0) AS daily_view_increment,
            COALESCE(SUM(daily_like_increment), 0) AS daily_like_increment
        FROM formal_video_daily
        WHERE dt >= (SELECT min_dt FROM params)
        GROUP BY dt
    ),
    -- 汇总图 2 所需的首次采集正播放视频数。
    new_video_metrics AS (
        SELECT
            first_positive_view_dt AS dt,
            COUNT(DISTINCT video_key) AS new_video_count
        FROM video_first_positive_view
        WHERE first_positive_view_dt BETWEEN (SELECT min_dt FROM params)
            AND (SELECT max_dt FROM params)
        GROUP BY dt
    ),
    -- 汇总图 3 所需的 7 天 LTV cohort。
    ltv_7d_metrics AS (
        SELECT
            d.dt,
            COUNT(DISTINCT d.video_key) AS ltv_7d_video_count,
            COALESCE(SUM(d.day_end_views), 0) AS ltv_7d_view_count
        FROM formal_video_daily d
        INNER JOIN video_first_positive_view f
            ON d.video_key = f.video_key
        WHERE f.first_positive_view_dt = DATE_SUB(d.dt, INTERVAL {ltv_offset_days} DAY)
            AND d.dt BETWEEN (SELECT min_dt FROM params) AND (SELECT max_dt FROM params)
        GROUP BY d.dt
    ),
    -- 每个账号首次拥有正式号视频的日期，按视频正式号 tag 创建日期累计。
    creator_first_formal_video AS (
        SELECT
            CONCAT(channel, ':', creator_id) AS creator_key,
            MIN(formal_tag_created_dt) AS first_formal_video_dt
        FROM formal_video_dim
        WHERE creator_id IS NOT NULL
            AND formal_tag_created_dt IS NOT NULL
        GROUP BY creator_key
    ),
    -- 每个视频首次成为正式号视频的日期，同样按 tag 创建日期累计。
    formal_video_tag_base AS (
        SELECT
            video_key,
            MIN(formal_tag_created_dt) AS first_formal_video_dt
        FROM formal_video_dim
        WHERE formal_tag_created_dt IS NOT NULL
        GROUP BY video_key
    ),
    -- 汇总图 4 所需的内容账号累计数，以及辅助视频累计数。
    cumulative_metrics AS (
        SELECT
            d.dt,
            COUNT(DISTINCT IF(c.first_formal_video_dt <= d.dt, c.creator_key, NULL))
                AS creator_count,
            COUNT(DISTINCT IF(v.first_formal_video_dt <= d.dt, v.video_key, NULL))
                AS formal_video_count_to_date
        FROM date_list d
        LEFT JOIN creator_first_formal_video c
            ON c.first_formal_video_dt <= d.dt
        LEFT JOIN formal_video_tag_base v
            ON v.first_formal_video_dt <= d.dt
        GROUP BY d.dt
    )
    SELECT
        d.dt,
        COALESCE(i.daily_view_increment, 0) AS daily_view_increment,
        COALESCE(i.daily_like_increment, 0) AS daily_like_increment,
        COALESCE(n.new_video_count, 0) AS new_video_count,
        COALESCE(l.ltv_7d_video_count, 0) AS ltv_7d_video_count,
        COALESCE(l.ltv_7d_view_count, 0) AS ltv_7d_view_count,
        CAST(CASE
            WHEN COALESCE(l.ltv_7d_video_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(l.ltv_7d_view_count AS NUMERIC)
                / CAST(l.ltv_7d_video_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_ltv_7d_views,
        COALESCE(c.creator_count, 0) AS creator_count,
        COALESCE(c.formal_video_count_to_date, 0) AS formal_video_count_to_date,
        CURRENT_TIMESTAMP() AS update_time
    FROM date_list d
    LEFT JOIN daily_increment_metrics i
        ON d.dt = i.dt
    LEFT JOIN new_video_metrics n
        ON d.dt = n.dt
    LEFT JOIN ltv_7d_metrics l
        ON d.dt = l.dt
    LEFT JOIN cumulative_metrics c
        ON d.dt = c.dt
    """


def run_ads_daily_formal_video_increment(dates: list[date]) -> None:
    if not dates:
        logging.info("没有新的 ads_daily_formal_video_increment 日期需要处理")
        return

    target_table = target_table_id()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    staging_table = f"{target_table}_tmp_{run_id}"
    changed_table = f"{target_table}_changed_{run_id}"
    select_sql = build_select_sql(dates)
    target_dropped = False

    logging.info(
        "开始处理 %s, 目标表: %s, 临时表: %s, 替换表: %s",
        TABLE_NAME,
        target_table,
        staging_table,
        changed_table,
    )

    try:
        client.query(
            f"""
            CREATE TABLE `{staging_table}`
            PARTITION BY dt AS
            {select_sql}
            """
        ).result()

        validation_row = list(client.query(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT dt) AS dt_count
            FROM `{staging_table}`
            """
        ).result())[0]
        if validation_row.row_count != len(dates):
            raise ValueError(
                "ads_daily_formal_video_increment staging 行数校验失败: "
                f"expected={len(dates)}, actual={validation_row.row_count}"
            )
        if validation_row.dt_count != len(dates):
            raise ValueError(
                "ads_daily_formal_video_increment staging 日期数校验失败: "
                f"expected={len(dates)}, actual={validation_row.dt_count}"
            )

        if target_table_exists():
            client.query(
                f"""
                CREATE TABLE `{changed_table}`
                PARTITION BY dt AS
                SELECT *
                FROM `{target_table}`
                WHERE dt < DATE('{min(dates)}') OR dt > DATE('{max(dates)}')

                UNION ALL

                SELECT *
                FROM `{staging_table}`
                """
            ).result()
            client.query(f"DROP TABLE `{target_table}`").result()
            target_dropped = True
            client.query(f"ALTER TABLE `{changed_table}` RENAME TO {TABLE_NAME}").result()
        else:
            client.query(
                f"""
                CREATE TABLE `{target_table}`
                PARTITION BY dt AS
                SELECT *
                FROM `{staging_table}`
                """
            ).result()

        client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
    except Exception:
        if not target_dropped:
            client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
            client.query(f"DROP TABLE IF EXISTS `{changed_table}`").result()
        raise

    logging.info(
        "%s 刷新完成, 处理日期: %s ~ %s, 共 %s 天",
        TABLE_NAME,
        min(dates),
        max(dates),
        len(dates),
    )


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")
    if not dates:
        logging.info("没有新的 ads_daily_formal_video_increment 日期需要处理")
        raise SystemExit(0)

    run_ads_daily_formal_video_increment(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
