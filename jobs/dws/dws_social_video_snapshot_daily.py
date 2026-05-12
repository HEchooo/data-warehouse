import argparse
import logging
from datetime import date, datetime, timedelta, timezone

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from zoneinfo import ZoneInfo


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
DIM_TABLE_NAME = "dim_video"
VIDEO_SOURCE_DATASET_ID = "videos"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_social_video_snapshot_daily"
SOCIAL_VIDEO_REBUILD_DAYS = 30

client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="构建 Social Content 社媒视频日快照表（dt × video_key）"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="按全历史发布时间范围重刷全量分区，而不是默认最近 30 天",
    )
    return parser.parse_args()


def dates_to_sql_list(dates: list[date]) -> str:
    return ", ".join([f"'{d}'" for d in dates])

def target_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


def dim_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{DIM_TABLE_NAME}"


def target_table_exists() -> bool:
    try:
        client.get_table(target_table_id())
        return True
    except NotFound:
        return False


def get_social_video_dates_to_process(
    rebuild_days: int = SOCIAL_VIDEO_REBUILD_DAYS,
) -> list[date]:
    end_date = datetime.now(ZoneInfo(TORONTO_TZ)).date()
    start_date = end_date - timedelta(days=rebuild_days - 1)
    return [start_date + timedelta(days=offset) for offset in range(rebuild_days)]


def get_social_video_full_dates_to_process() -> list[date]:
    query = f"""
    SELECT
        MIN(published_dt) AS start_dt,
        CURRENT_DATE('{TORONTO_TZ}') AS end_dt
    FROM `{dim_table_id()}`
    WHERE published_dt IS NOT NULL
    """
    rows = list(client.query(query).result())
    if not rows or rows[0].start_dt is None or rows[0].end_dt is None:
        return []

    start_date = rows[0].start_dt
    end_date = rows[0].end_dt
    total_days = (end_date - start_date).days + 1
    return [start_date + timedelta(days=offset) for offset in range(total_days)]


def build_social_video_snapshot_select_sql(dates: list[date]) -> str:
    dates_str = dates_to_sql_list(dates)
    min_date = min(dates)
    max_date = max(dates)

    return f"""
    WITH
    params AS (
        SELECT
            DATE('{min_date}') AS min_dt,
            DATE('{max_date}') AS max_dt
    ),
    video_dim AS (
        SELECT
            channel,
            creator_id,
            video_id,
            video_key,
            published_ts_utc,
            published_dt
        FROM `{dim_table_id()}`
        WHERE published_dt <= (SELECT max_dt FROM params)
    ),
    raw_snapshot_log AS (
        SELECT
            video_key,
            DATE(collected_ts_utc, '{TORONTO_TZ}') AS dt,
            collected_ts_utc,
            cum_views,
            cum_likes
        FROM (
            SELECT
                CONCAT('youtube:', CAST(video_id AS STRING)) AS video_key,
                TIMESTAMP(collected_at, 'UTC') AS collected_ts_utc,
                SAFE_CAST(views AS INT64) AS cum_views,
                SAFE_CAST(likes AS INT64) AS cum_likes
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.yt_video_analytics_log`

            UNION ALL

            SELECT
                CONCAT('tiktok:', CAST(video_id AS STRING)) AS video_key,
                TIMESTAMP(collected_at, 'UTC') AS collected_ts_utc,
                SAFE_CAST(view_count AS INT64) AS cum_views,
                SAFE_CAST(like_count AS INT64) AS cum_likes
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.video_stats_log`

            UNION ALL

            SELECT
                CONCAT('ins:', CAST(media_id AS STRING)) AS video_key,
                TIMESTAMP(collected_at, 'UTC') AS collected_ts_utc,
                SAFE_CAST(view_count AS INT64) AS cum_views,
                SAFE_CAST(like_count AS INT64) AS cum_likes
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.ig_media_stats_log`
        )
        WHERE collected_ts_utc IS NOT NULL
            AND (cum_views IS NOT NULL OR cum_likes IS NOT NULL)
            AND DATE(collected_ts_utc, '{TORONTO_TZ}')
                BETWEEN (SELECT min_dt FROM params) AND (SELECT max_dt FROM params)
    ),
    daily_latest_snapshot AS (
        SELECT
            video_key,
            dt,
            cum_views AS day_end_views,
            cum_likes AS day_end_likes
        FROM (
            SELECT
                video_key,
                dt,
                cum_views,
                cum_likes,
                ROW_NUMBER() OVER (
                    PARTITION BY video_key, dt
                    ORDER BY collected_ts_utc DESC
                ) AS rn
            FROM raw_snapshot_log
        )
        WHERE rn = 1
    )
    SELECT
        s.dt,
        v.channel,
        v.creator_id,
        v.video_id,
        v.video_key,
        v.published_ts_utc,
        v.published_dt,
        s.day_end_views,
        s.day_end_likes,
        CURRENT_TIMESTAMP() AS update_time
    FROM daily_latest_snapshot s
    INNER JOIN video_dim v
        ON s.video_key = v.video_key
    WHERE s.dt IN ({dates_str})
    """


def run_dws_social_video_snapshot_daily(dates: list[date]) -> None:
    if not dates:
        logging.info("dws_social_video_snapshot_daily 无待处理日期，跳过")
        return

    min_date = min(dates)
    max_date = max(dates)
    target_table = target_table_id()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    staging_table = f"{target_table}_tmp_{run_id}"
    changed_table = f"{target_table}_changed_{run_id}"
    target_dropped = False
    select_sql = build_social_video_snapshot_select_sql(dates)
    insert_into_staging_sql = f"""
    INSERT INTO `{staging_table}`
    (
        dt,
        channel,
        creator_id,
        video_id,
        video_key,
        published_ts_utc,
        published_dt,
        day_end_views,
        day_end_likes,
        update_time
    )
    {select_sql}
    """
    validate_staging_sql = f"""
    SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT dt) AS dt_count,
        COUNT(DISTINCT CONCAT(CAST(dt AS STRING), '|', video_key)) AS unique_dt_video_count
    FROM `{staging_table}`
    """
    create_changed_table_sql = f"""
    CREATE TABLE `{changed_table}`
    PARTITION BY dt
    CLUSTER BY channel, creator_id, video_key AS
    SELECT *
    FROM `{target_table}`
    WHERE dt < DATE('{min_date}') OR dt > DATE('{max_date}')

    UNION ALL

    SELECT *
    FROM `{staging_table}`
    """

    logging.info(
        "开始处理 dws_social_video_snapshot_daily, 目标表: %s, 临时表: %s, 替换表: %s",
        target_table,
        staging_table,
        changed_table,
    )

    try:
        if target_table_exists():
            client.query(
                f"""
                CREATE TABLE `{staging_table}` AS
                SELECT *
                FROM `{target_table}`
                WHERE 1 = 0
                """
            ).result()
        else:
            client.query(
                f"""
                CREATE TABLE `{staging_table}`
                PARTITION BY dt
                CLUSTER BY channel, creator_id, video_key AS
                SELECT
                    CAST(NULL AS DATE) AS dt,
                    CAST(NULL AS STRING) AS channel,
                    CAST(NULL AS STRING) AS creator_id,
                    CAST(NULL AS STRING) AS video_id,
                    CAST(NULL AS STRING) AS video_key,
                    CAST(NULL AS TIMESTAMP) AS published_ts_utc,
                    CAST(NULL AS DATE) AS published_dt,
                    CAST(NULL AS INT64) AS day_end_views,
                    CAST(NULL AS INT64) AS day_end_likes,
                    CAST(NULL AS TIMESTAMP) AS update_time
                LIMIT 0
                """
            ).result()

        client.query(insert_into_staging_sql).result()

        validation_row = list(client.query(validate_staging_sql).result())[0]
        if validation_row.row_count == 0:
            raise ValueError(
                "dws_social_video_snapshot_daily staging 行数校验失败: actual=0"
            )
        if validation_row.row_count != validation_row.unique_dt_video_count:
            raise ValueError(
                "dws_social_video_snapshot_daily staging 主键校验失败: "
                f"row_count={validation_row.row_count}, "
                f"unique_dt_video_count={validation_row.unique_dt_video_count}"
            )
        logging.info(
            "dws_social_video_snapshot_daily staging 校验通过: rows=%s, actual_dt_count=%s, requested_dt_count=%s",
            validation_row.row_count,
            validation_row.dt_count,
            len(dates),
        )

        if target_table_exists():
            client.query(create_changed_table_sql).result()
            client.query(f"DROP TABLE `{target_table}`").result()
            target_dropped = True
            client.query(
                f"ALTER TABLE `{changed_table}` RENAME TO {TABLE_NAME}"
            ).result()
        else:
            client.query(
                f"CREATE TABLE `{target_table}` "
                "PARTITION BY dt "
                "CLUSTER BY channel, creator_id, video_key AS "
                f"SELECT * FROM `{staging_table}`"
            ).result()

        client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
    except Exception:
        if not target_dropped:
            client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
            client.query(f"DROP TABLE IF EXISTS `{changed_table}`").result()
        raise

    logging.info(
        "dws_social_video_snapshot_daily 刷新完成, 处理日期: %s ~ %s, 共 %s 天",
        min_date,
        max_date,
        len(dates),
    )


if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)

    args = parse_args()
    if args.full_refresh:
        dates = get_social_video_full_dates_to_process()
    else:
        dates = get_social_video_dates_to_process()
    if not dates:
        logging.info("没有 Social Content 视频快照数据需要处理")
        raise SystemExit(0)

    logging.info(
        "Social Content 视频快照表待处理日期范围: %s ~ %s，共 %s 天",
        min(dates),
        max(dates),
        len(dates),
    )
    run_dws_social_video_snapshot_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dws_social_video_snapshot_daily 执行完成, 耗时: %.1f 秒", elapsed)
