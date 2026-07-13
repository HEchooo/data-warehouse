import argparse
from datetime import date, datetime, timedelta, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from zoneinfo import ZoneInfo


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "ads_daily_social_video_monitor_rankings_v2"
SOURCE_TABLE_NAME = "dws_social_video_state_daily_v2"
DIM_TABLE_NAME = "dim_video"
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
    parser = argparse.ArgumentParser(
        description="构建 Social Content 视频监控日榜（D 日展示 D-1 两类视频池）"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="按视频状态表的完整成熟日期历史重刷，而不是只处理最近变更日期",
    )
    return parser.parse_args()


def target_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


def source_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{SOURCE_TABLE_NAME}"


def dim_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{DIM_TABLE_NAME}"


def target_table_exists() -> bool:
    try:
        client.get_table(target_table_id())
        return True
    except NotFound:
        return False


def get_target_row_count() -> int:
    if not target_table_exists():
        return 0

    rows = list(client.query(f"SELECT COUNT(*) AS row_count FROM `{target_table_id()}`").result())
    return int(rows[0].row_count) if rows else 0


def get_full_dates_to_process() -> list[date]:
    query = f"""
    SELECT
        DATE_ADD(MIN(dt), INTERVAL 1 DAY) AS min_report_dt,
        DATE_ADD(MAX(dt), INTERVAL 1 DAY) AS max_report_dt
    FROM `{source_table_id()}`
    WHERE dt < CURRENT_DATE('{TORONTO_TZ}')
    """
    rows = list(client.query(query).result())
    if not rows or rows[0].min_report_dt is None or rows[0].max_report_dt is None:
        return []

    min_report_dt = rows[0].min_report_dt
    max_report_dt = rows[0].max_report_dt
    return [
        min_report_dt + timedelta(days=offset)
        for offset in range((max_report_dt - min_report_dt).days + 1)
    ]


def get_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    if not target_table_exists() or get_target_row_count() == 0:
        return get_full_dates_to_process()

    query = f"""
    SELECT DISTINCT DATE_ADD(dt, INTERVAL 1 DAY) AS report_dt
    FROM `{source_table_id()}`
    WHERE dt < CURRENT_DATE('{TORONTO_TZ}')
        AND update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ORDER BY report_dt
    """
    return [row.report_dt for row in client.query(query).result()]


def dates_to_array_sql(dates: list[date]) -> str:
    return ", ".join(f"DATE '{report_dt}'" for report_dt in dates)


def build_select_sql(dates: list[date]) -> str:
    report_dates_sql = dates_to_array_sql(dates)
    min_source_dt = min(dates) - timedelta(days=2)
    max_source_dt = max(dates) - timedelta(days=1)

    return f"""
    WITH
    report_dates AS (
        SELECT
            dt,
            DATE_SUB(dt, INTERVAL 1 DAY) AS target_dt
        FROM UNNEST([{report_dates_sql}]) AS dt
    ),
    social_video_state AS (
        SELECT
            dt,
            channel,
            creator_id,
            video_id,
            video_key,
            day_end_views
        FROM `{source_table_id()}`
        WHERE dt BETWEEN DATE '{min_source_dt}' AND DATE '{max_source_dt}'
    ),
    video_daily AS (
        SELECT
            d.dt,
            d.target_dt,
            current_state.channel,
            current_state.creator_id,
            current_state.video_id,
            current_state.video_key,
            current_state.day_end_views
                - COALESCE(previous_state.day_end_views, 0) AS view_increment,
            current_state.day_end_views AS total_view_count
        FROM report_dates d
        INNER JOIN social_video_state current_state
            ON current_state.dt = d.target_dt
        LEFT JOIN social_video_state previous_state
            ON previous_state.dt = DATE_SUB(d.target_dt, INTERVAL 1 DAY)
            AND previous_state.video_key = current_state.video_key
    ),
    video_enriched AS (
        SELECT
            v.dt,
            v.target_dt,
            CASE
                WHEN dim.published_dt = v.target_dt THEN 't1_published_video_top'
                WHEN dim.published_dt < v.target_dt THEN 'historical_video_top'
            END AS ranking_type,
            v.channel,
            v.creator_id,
            v.video_id,
            v.video_key,
            dim.video_title,
            dim.video_link,
            dim.account_name,
            dim.account_handle,
            dim.account_url,
            v.view_increment,
            v.total_view_count,
            dim.published_ts_utc,
            dim.published_dt
        FROM video_daily v
        INNER JOIN `{dim_table_id()}` dim
            ON v.video_key = dim.video_key
        WHERE dim.is_social_content_video
            AND dim.published_dt <= v.target_dt
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY dt, ranking_type
                ORDER BY view_increment DESC, total_view_count DESC, video_key
            ) AS rank_no
        FROM video_enriched
        WHERE ranking_type IS NOT NULL
    )
    SELECT
        dt,
        target_dt,
        ranking_type,
        rank_no,
        channel,
        creator_id,
        video_id,
        video_key,
        video_title,
        video_link,
        account_name,
        account_handle,
        account_url,
        view_increment,
        total_view_count,
        published_ts_utc,
        published_dt,
        CURRENT_TIMESTAMP() AS update_time
    FROM ranked
    WHERE rank_no <= 10
    """


def run_ads_daily_social_video_monitor_rankings(dates: list[date]) -> None:
    if not dates:
        logging.info("%s 无待处理日期，跳过", TABLE_NAME)
        return

    target_table = target_table_id()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    staging_table = f"{target_table}_tmp_{run_id}"
    changed_table = f"{target_table}_changed_{run_id}"
    target_dropped = False

    try:
        client.query(
            f"""
            CREATE TABLE `{staging_table}`
            PARTITION BY dt
            CLUSTER BY ranking_type, channel, video_key AS
            {build_select_sql(dates)}
            """
        ).result()

        validation_row = list(
            client.query(
                f"""
                WITH ranking_groups AS (
                    SELECT dt, ranking_type, COUNT(*) AS row_count
                    FROM `{staging_table}`
                    GROUP BY dt, ranking_type
                )
                SELECT
                    (SELECT COUNT(*) FROM `{staging_table}`) AS row_count,
                    (SELECT COUNT(DISTINCT dt) FROM `{staging_table}`) AS dt_count,
                    (SELECT COUNT(DISTINCT CONCAT(
                        CAST(dt AS STRING), '|', ranking_type, '|', CAST(rank_no AS STRING)
                    )) FROM `{staging_table}`) AS unique_rank_count,
                    (SELECT COUNTIF(rank_no NOT BETWEEN 1 AND 10)
                     FROM `{staging_table}`) AS invalid_rank_rows,
                    (SELECT COUNTIF(target_dt != DATE_SUB(dt, INTERVAL 1 DAY))
                     FROM `{staging_table}`) AS invalid_target_dt_rows,
                    COUNTIF(row_count > 10) AS invalid_ranking_group_count
                FROM ranking_groups
                """
            ).result()
        )[0]
        if validation_row.row_count != validation_row.unique_rank_count:
            raise RuntimeError("视频监控日榜校验失败：日期、榜单类型和排名不唯一")
        if validation_row.invalid_rank_rows != 0:
            raise RuntimeError("视频监控日榜校验失败：排名超出 1-10")
        if validation_row.invalid_target_dt_rows != 0:
            raise RuntimeError("视频监控日榜校验失败：target_dt 不是 dt 的前一日")
        if validation_row.dt_count != len(dates):
            raise RuntimeError(
                "视频监控日榜校验失败：存在未产出数据的报表日期，"
                f"expected={len(dates)}, actual={validation_row.dt_count}"
            )
        if validation_row.invalid_ranking_group_count != 0:
            raise RuntimeError("视频监控日榜校验失败：存在超过 10 条的视频池")

        if target_table_exists():
            report_dates_sql = dates_to_array_sql(dates)
            client.query(
                f"""
                CREATE TABLE `{changed_table}`
                PARTITION BY dt
                CLUSTER BY ranking_type, channel, video_key AS
                SELECT *
                FROM `{target_table}`
                WHERE dt NOT IN UNNEST([{report_dates_sql}])

                UNION ALL

                SELECT * FROM `{staging_table}`
                """
            ).result()
            client.query(f"DROP TABLE `{target_table}`").result()
            target_dropped = True
            client.query(f"ALTER TABLE `{changed_table}` RENAME TO {TABLE_NAME}").result()
        else:
            client.query(
                f"""
                CREATE TABLE `{target_table}`
                PARTITION BY dt
                CLUSTER BY ranking_type, channel, video_key AS
                SELECT * FROM `{staging_table}`
                """
            ).result()

        client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
    except Exception:
        if not target_dropped:
            client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
            client.query(f"DROP TABLE IF EXISTS `{changed_table}`").result()
        raise

    logging.info(
        "%s 刷新完成，处理日期: %s ~ %s，共 %s 天",
        TABLE_NAME,
        min(dates),
        max(dates),
        len(dates),
    )


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)
    start_time = datetime.now(ZoneInfo(TORONTO_TZ))
    args = parse_args()
    dates = get_full_dates_to_process() if args.full_refresh else get_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")
    run_ads_daily_social_video_monitor_rankings(dates)
    elapsed = (datetime.now(ZoneInfo(TORONTO_TZ)) - start_time).total_seconds()
    logging.info("%s 完成，耗时: %.1f 秒", TABLE_NAME, elapsed)
