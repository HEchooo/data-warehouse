import argparse
from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_daily_social_content_v2"
SOURCE_TABLE_NAME = "dws_social_channel_daily_v2"
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
        description="构建 Social Content v2 社媒表现数据日报（dt × 展示渠道）"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="按 v2 渠道事实表已落地日期全量重刷，而不是默认最近增量日期",
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
    Social Content v2 ADS 只消费 dws_social_channel_daily_v2。

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
        logging.info("没有新的 %s 日期需要处理", TABLE_NAME)
        return

    if not target_table_exists():
        raise RuntimeError(f"目标表不存在，请先执行 DDL: {target_table_id()}")

    min_date = min(dates)
    max_date = max(dates)
    target_table = target_table_id()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    staging_table = f"{target_table}_tmp_{run_id}"
    changed_table = f"{target_table}_changed_{run_id}"
    target_dropped = False
    insert_staging_sql = f"""
    INSERT INTO `{staging_table}`
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
    validate_staging_sql = f"""
    SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT dt) AS dt_count,
        COUNT(DISTINCT CONCAT(CAST(dt AS STRING), '|', channel))
            AS unique_dt_channel_count
    FROM `{staging_table}`
    """
    create_changed_table_sql = f"""
    CREATE TABLE `{changed_table}`
    PARTITION BY dt
    CLUSTER BY channel AS
    SELECT *
    FROM `{target_table}`
    WHERE dt < DATE('{min_date}') OR dt > DATE('{max_date}')

    UNION ALL

    SELECT *
    FROM `{staging_table}`
    """

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
            CREATE TABLE `{staging_table}` AS
            SELECT *
            FROM `{target_table}`
            WHERE 1 = 0
            """
        ).result()
        client.query(insert_staging_sql).result()

        validation_row = list(client.query(validate_staging_sql).result())[0]
        expected_rows = len(dates) * 4
        if validation_row.row_count != expected_rows:
            raise ValueError(
                f"{TABLE_NAME} staging 行数校验失败: "
                f"expected={expected_rows}, actual={validation_row.row_count}"
            )
        if validation_row.dt_count != len(dates):
            raise ValueError(
                f"{TABLE_NAME} staging 日期数校验失败: "
                f"expected={len(dates)}, actual={validation_row.dt_count}"
            )
        if validation_row.row_count != validation_row.unique_dt_channel_count:
            raise ValueError(
                f"{TABLE_NAME} staging 主键校验失败: "
                f"row_count={validation_row.row_count}, "
                f"unique_dt_channel_count={validation_row.unique_dt_channel_count}"
            )

        client.query(create_changed_table_sql).result()
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
        logging.info("没有新的 %s 日期需要处理", TABLE_NAME)
        raise SystemExit(0)

    run_ads_daily_social_content(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
