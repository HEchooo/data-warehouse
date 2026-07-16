import argparse
from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_daily_social_content_attribute_v2"
SOURCE_TABLE_NAME = "dws_social_attribute_daily_v2"
INCREMENTAL_WINDOW_HOURS = 6
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="构建 Social Content 属性维度日报")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="按 DWS 已落地日期全量重刷",
    )
    return parser.parse_args()


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
    rows = list(
        client.query(f"SELECT COUNT(*) AS row_count FROM `{target_table_id()}`").result()
    )
    return int(rows[0].row_count) if rows else 0


def get_incremental_dates() -> list[date]:
    if get_target_row_count() == 0:
        return get_full_dates()
    query = f"""
    SELECT DISTINCT dt
    FROM `{source_table_id()}`
    WHERE update_time >= TIMESTAMP_SUB(
        CURRENT_TIMESTAMP(),
        INTERVAL {INCREMENTAL_WINDOW_HOURS} HOUR
    )
    ORDER BY dt
    """
    return [row.dt for row in client.query(query).result()]


def get_full_dates() -> list[date]:
    query = f"""
    SELECT DISTINCT dt
    FROM `{source_table_id()}`
    ORDER BY dt
    """
    return [row.dt for row in client.query(query).result()]


def dates_to_sql_list(dates: list[date]) -> str:
    return ", ".join(f"DATE '{value}'" for value in dates)


def build_select_sql(dates: list[date]) -> str:
    dates_sql = dates_to_sql_list(dates)
    return f"""
    WITH
    -- ADS 只读取属性 DWS 的报表粒度分子分母。
    attribute_fact AS (
        SELECT *
        FROM `{source_table_id()}`
        WHERE dt IN ({dates_sql})
    )
    SELECT
        dt,
        channel,
        metric_scope,
        video_attribute,
        video_brand,
        blogger_attribute,
        daily_view_increment,
        daily_like_increment,
        new_video_count,
        new_video_contributed_view_count,
        CAST(SAFE_DIVIDE(
            CAST(new_video_contributed_view_count AS NUMERIC),
            NULLIF(CAST(new_video_count AS NUMERIC), 0)
        ) AS NUMERIC) AS avg_new_video_view_count,
        published_7d_cohort_video_count,
        published_7d_cohort_view_count,
        CAST(SAFE_DIVIDE(
            CAST(published_7d_cohort_view_count AS NUMERIC),
            NULLIF(CAST(published_7d_cohort_video_count AS NUMERIC), 0)
        ) AS NUMERIC) AS published_7d_cohort_avg_view_count,
        published_day0_view_count,
        published_day0_video_count,
        published_day1_view_count,
        published_day1_video_count,
        published_day3_view_count,
        published_day3_video_count,
        published_day7_view_count,
        published_day7_video_count,
        published_day14_view_count,
        published_day14_video_count,
        creator_count_to_prev_day,
        CAST(SAFE_DIVIDE(
            CAST(daily_view_increment AS NUMERIC),
            NULLIF(CAST(creator_count_to_prev_day AS NUMERIC), 0)
        ) AS NUMERIC) AS daily_view_increment_per_creator,
        latest_3_sample_creator_count,
        latest_3_sample_video_count,
        latest_3_sample_like_count,
        latest_3_sample_view_count,
        CAST(SAFE_DIVIDE(
            CAST(latest_3_sample_like_count AS NUMERIC),
            NULLIF(CAST(latest_3_sample_view_count AS NUMERIC), 0)
        ) AS NUMERIC) AS latest_3_avg_like_view_rate,
        CURRENT_TIMESTAMP() AS update_time
    FROM attribute_fact
    """


def run_ads_daily_social_content_attribute(dates: list[date]) -> None:
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
                    COUNT(DISTINCT {key_expr}) AS unique_key_count
                FROM `{staging_table}`
                """
            ).result()
        )[0]
        if validation_row.row_count != validation_row.unique_key_count:
            raise ValueError("ads_daily_social_content_attribute_v2 校验失败：业务键重复")

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
    run_ads_daily_social_content_attribute(dates)
    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
