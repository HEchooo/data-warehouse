import argparse
from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_daily_social_content"
SOURCE_TABLE_NAME = "dws_social_channel_daily"
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="构建 Social Content 社媒表现数据日报（dt × 展示渠道）"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="按 dws_social_channel_daily 已落地日期全量重刷，而不是默认最近增量日期",
    )
    return parser.parse_args()


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


def get_social_content_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    Social Content ADS 只消费 dws_social_channel_daily。

    规则：
    - 目标表不存在或为空时，按渠道事实已落地日期全量初始化
    - 目标表已存在时，只读取最近 6 小时内渠道事实变更的 dt
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM `{source_table_id()}`
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT dt
    FROM `{source_table_id()}`
    WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def get_social_content_full_dates_to_process() -> list[date]:
    query = f"""
    SELECT DISTINCT dt
    FROM `{source_table_id()}`
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def build_select_sql(dates: list[date]) -> str:
    dates_str = dates_to_sql_list(dates)

    return f"""
    WITH
    -- 当前批次只读取渠道级事实层。
    social_channel_fact AS (
        SELECT
            dt,
            channel,
            daily_view_increment,
            daily_like_increment,
            new_video_count,
            new_video_contributed_view_count,
            published_7d_cohort_video_count,
            published_7d_cohort_view_count,
            creator_count_to_prev_day,
            latest_3_sample_creator_count,
            latest_3_sample_video_count,
            latest_3_sample_like_count,
            latest_3_sample_view_count
        FROM `{source_table_id()}`
        WHERE dt IN ({dates_str})
    )
    SELECT
        dt,
        channel,
        daily_view_increment,
        daily_like_increment,
        new_video_count,
        new_video_contributed_view_count,
        CAST(CASE
            WHEN COALESCE(new_video_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(new_video_contributed_view_count, 0) AS NUMERIC)
                / CAST(new_video_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_new_video_view_count,
        published_7d_cohort_video_count,
        published_7d_cohort_view_count,
        CAST(CASE
            WHEN COALESCE(published_7d_cohort_video_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(published_7d_cohort_view_count, 0) AS NUMERIC)
                / CAST(published_7d_cohort_video_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS published_7d_cohort_avg_view_count,
        creator_count_to_prev_day,
        CAST(CASE
            WHEN COALESCE(creator_count_to_prev_day, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(daily_view_increment, 0) AS NUMERIC)
                / CAST(creator_count_to_prev_day AS NUMERIC),
                4
            )
        END AS NUMERIC) AS daily_view_increment_per_creator,
        latest_3_sample_creator_count,
        latest_3_sample_video_count,
        latest_3_sample_like_count,
        latest_3_sample_view_count,
        CAST(CASE
            WHEN COALESCE(latest_3_sample_view_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(latest_3_sample_like_count, 0) AS NUMERIC)
                / CAST(latest_3_sample_view_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS latest_3_avg_like_view_rate,
        CURRENT_TIMESTAMP() AS update_time
    FROM social_channel_fact
    """


def run_ads_daily_social_content(dates: list[date]) -> None:
    if not dates:
        logging.info("没有新的 ads_daily_social_content 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        channel,
        daily_view_increment,
        daily_like_increment,
        new_video_count,
        new_video_contributed_view_count,
        avg_new_video_view_count,
        published_7d_cohort_video_count,
        published_7d_cohort_view_count,
        published_7d_cohort_avg_view_count,
        creator_count_to_prev_day,
        daily_view_increment_per_creator,
        latest_3_sample_creator_count,
        latest_3_sample_video_count,
        latest_3_sample_like_count,
        latest_3_sample_view_count,
        latest_3_avg_like_view_rate,
        update_time
    )
    {build_select_sql(dates)}
    """

    logging.info("开始处理: %s", TABLE_NAME)
    client.query(query).result()
    logging.info("%s 刷新完成, 处理日期: %s", TABLE_NAME, dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    args = parse_args()
    if args.full_refresh:
        dates = get_social_content_full_dates_to_process()
    else:
        dates = get_social_content_dates_to_process()

    logging.info("待处理日期: %s", dates if dates else "[]")
    if not dates:
        logging.info("没有新的 ads_daily_social_content 日期需要处理")
        raise SystemExit(0)

    run_ads_daily_social_content(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
