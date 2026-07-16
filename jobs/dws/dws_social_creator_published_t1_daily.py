import argparse
from datetime import date, datetime, timedelta, timezone
import logging

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from zoneinfo import ZoneInfo


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_social_creator_published_t1_daily"
SNAPSHOT_TABLE_NAME = "dws_social_video_snapshot_daily_v2"
DIM_TABLE_NAME = "dim_video"
INCREMENTAL_REBUILD_DAYS = 30

client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="构建 Social Content 账号发布日期 T+1 播放事实"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="按真实视频快照表的完整成熟日期历史重刷，而不是最近 30 个成熟发布日期",
    )
    return parser.parse_args()


def table_id(table_name: str) -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{table_name}"


def target_table_exists() -> bool:
    try:
        client.get_table(table_id(TABLE_NAME))
        return True
    except NotFound:
        return False


def get_target_row_count() -> int:
    if not target_table_exists():
        return 0
    rows = list(
        client.query(f"SELECT COUNT(*) AS row_count FROM `{table_id(TABLE_NAME)}`").result()
    )
    return int(rows[0].row_count) if rows else 0


def get_full_dates_to_process() -> list[date]:
    query = f"""
    SELECT
        MIN(published_dt) AS min_published_dt,
        MAX(published_dt) AS max_published_dt
    FROM `{table_id(SNAPSHOT_TABLE_NAME)}`
    WHERE dt = DATE_ADD(published_dt, INTERVAL 1 DAY)
        AND dt < CURRENT_DATE('{TORONTO_TZ}')
    """
    rows = list(client.query(query).result())
    if not rows or rows[0].min_published_dt is None:
        return []
    start_dt = rows[0].min_published_dt
    end_dt = rows[0].max_published_dt
    return [
        start_dt + timedelta(days=offset)
        for offset in range((end_dt - start_dt).days + 1)
    ]


def get_dates_to_process() -> list[date]:
    if not target_table_exists() or get_target_row_count() == 0:
        return get_full_dates_to_process()

    max_mature_dt = datetime.now(ZoneInfo(TORONTO_TZ)).date() - timedelta(days=2)
    start_dt = max_mature_dt - timedelta(days=INCREMENTAL_REBUILD_DAYS - 1)
    return [
        start_dt + timedelta(days=offset)
        for offset in range(INCREMENTAL_REBUILD_DAYS)
    ]


def dates_to_array_sql(dates: list[date]) -> str:
    return ", ".join(f"DATE '{published_dt}'" for published_dt in dates)


def build_select_sql(dates: list[date]) -> str:
    dates_sql = dates_to_array_sql(dates)
    min_t1_dt = min(dates) + timedelta(days=1)
    max_t1_dt = max(dates) + timedelta(days=1)
    return f"""
    WITH
    -- 只读取 T+1 当天存在真实采集的快照；正式号属性不参与该主题准入。
    t1_video_observation AS (
        SELECT
            snapshot.published_dt,
            snapshot.dt AS t1_dt,
            snapshot.channel,
            snapshot.creator_id,
            snapshot.video_key,
            snapshot.published_ts_utc,
            snapshot.day_end_views,
            dim.account_name,
            dim.account_handle,
            dim.account_url,
            TRUE AS has_t1_snapshot
        FROM `{table_id(SNAPSHOT_TABLE_NAME)}` snapshot
        INNER JOIN `{table_id(DIM_TABLE_NAME)}` dim
            ON snapshot.video_key = dim.video_key
        WHERE snapshot.dt BETWEEN DATE '{min_t1_dt}' AND DATE '{max_t1_dt}'
            AND snapshot.published_dt IN UNNEST([{dates_sql}])
            AND snapshot.dt = DATE_ADD(snapshot.published_dt, INTERVAL 1 DAY)
            AND dim.is_social_content_video
            AND dim.is_active_account
    )
    SELECT
        published_dt,
        t1_dt,
        channel,
        creator_id,
        ARRAY_AGG(
            STRUCT(account_name, account_handle, account_url)
            ORDER BY published_ts_utc DESC, video_key DESC
            LIMIT 1
        )[OFFSET(0)].account_name AS account_name,
        ARRAY_AGG(
            STRUCT(account_name, account_handle, account_url)
            ORDER BY published_ts_utc DESC, video_key DESC
            LIMIT 1
        )[OFFSET(0)].account_handle AS account_handle,
        ARRAY_AGG(
            STRUCT(account_name, account_handle, account_url)
            ORDER BY published_ts_utc DESC, video_key DESC
            LIMIT 1
        )[OFFSET(0)].account_url AS account_url,
        COUNT(DISTINCT video_key) AS published_video_count,
        COUNT(DISTINCT video_key) AS observed_t1_video_count,
        COALESCE(SUM(day_end_views), 0) AS observed_t1_view_count,
        CAST(SAFE_DIVIDE(
            SUM(day_end_views),
            COUNT(DISTINCT video_key)
        ) AS NUMERIC) AS t1_avg_view_count,
        TRUE AS is_t1_snapshot_complete,
        CURRENT_TIMESTAMP() AS update_time,
        ARRAY_AGG(
            STRUCT(
                video_key,
                published_ts_utc,
                day_end_views AS t1_view_count,
                has_t1_snapshot
            )
            ORDER BY published_ts_utc, video_key
        ) AS video_samples
    FROM t1_video_observation
    GROUP BY published_dt, t1_dt, channel, creator_id
    """


def run(dates: list[date], full_refresh: bool = False) -> None:
    if not dates:
        logging.info("%s 无待处理日期，跳过", TABLE_NAME)
        return

    target_table = table_id(TABLE_NAME)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    staging_table = f"{target_table}_tmp_{run_id}"
    changed_table = f"{target_table}_changed_{run_id}"
    target_dropped = False
    dates_sql = dates_to_array_sql(dates)

    try:
        client.query(
            f"""
            CREATE TABLE `{staging_table}`
            PARTITION BY published_dt
            CLUSTER BY channel, creator_id AS
            {build_select_sql(dates)}
            """
        ).result()

        validation = list(
            client.query(
                f"""
                SELECT
                    COUNT(*) AS row_count,
                    COUNT(DISTINCT CONCAT(
                        CAST(published_dt AS STRING), '|', channel, '|', creator_id
                    )) AS unique_key_count,
                    COUNTIF(t1_dt != DATE_ADD(published_dt, INTERVAL 1 DAY))
                        AS invalid_t1_date_count,
                    COUNTIF(observed_t1_video_count > published_video_count)
                        AS invalid_observation_count,
                    COUNTIF(observed_t1_video_count != published_video_count)
                        AS incomplete_observation_count,
                    COUNTIF(
                        is_t1_snapshot_complete
                            != (observed_t1_video_count = published_video_count)
                    ) AS invalid_complete_flag_count,
                    COUNTIF(
                        (is_t1_snapshot_complete AND t1_avg_view_count IS NULL)
                        OR (NOT is_t1_snapshot_complete AND t1_avg_view_count IS NOT NULL)
                    ) AS invalid_avg_count,
                    COUNTIF(ARRAY_LENGTH(video_samples) != published_video_count)
                        AS invalid_video_sample_count,
                    COUNTIF(EXISTS(
                        SELECT 1
                        FROM UNNEST(video_samples) sample
                        WHERE sample.has_t1_snapshot
                            != (sample.t1_view_count IS NOT NULL)
                    )) AS invalid_video_observation_count
                FROM `{staging_table}`
                """
            ).result()
        )[0]
        if validation.row_count != validation.unique_key_count:
            raise RuntimeError("账号 T+1 日事实校验失败：发布日期、渠道和账号不唯一")
        if validation.invalid_t1_date_count != 0:
            raise RuntimeError("账号 T+1 日事实校验失败：T+1 日期错误")
        if validation.invalid_observation_count != 0:
            raise RuntimeError("账号 T+1 日事实校验失败：真实快照数超过发布视频数")
        if validation.incomplete_observation_count != 0:
            raise RuntimeError("账号 T+1 日事实校验失败：存在非真实 T+1 视频")
        if validation.invalid_complete_flag_count != 0:
            raise RuntimeError("账号 T+1 日事实校验失败：完整性标记错误")
        if validation.invalid_avg_count != 0:
            raise RuntimeError("账号 T+1 日事实校验失败：篇均播放与完整性不一致")
        if validation.invalid_video_sample_count != 0:
            raise RuntimeError("账号 T+1 日事实校验失败：视频明细数与发布数不一致")
        if validation.invalid_video_observation_count != 0:
            raise RuntimeError("账号 T+1 日事实校验失败：视频明细观测标记错误")

        if target_table_exists():
            if full_refresh:
                preserve_sql = "WHERE FALSE"
            else:
                preserve_sql = f"WHERE published_dt NOT IN UNNEST([{dates_sql}])"
            client.query(
                f"""
                CREATE TABLE `{changed_table}`
                PARTITION BY published_dt
                CLUSTER BY channel, creator_id AS
                SELECT * FROM `{target_table}`
                {preserve_sql}

                UNION ALL BY NAME

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
                PARTITION BY published_dt
                CLUSTER BY channel, creator_id AS
                SELECT * FROM `{staging_table}`
                """
            ).result()

        client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
    except Exception:
        if target_dropped:
            try:
                client.get_table(target_table)
            except NotFound:
                client.query(
                    f"ALTER TABLE `{changed_table}` RENAME TO {TABLE_NAME}"
                ).result()
        else:
            client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
            client.query(f"DROP TABLE IF EXISTS `{changed_table}`").result()
        raise

    logging.info(
        "%s 刷新完成，发布日期: %s ~ %s，共 %s 天",
        TABLE_NAME,
        min(dates),
        max(dates),
        len(dates),
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    started_at = datetime.now(ZoneInfo(TORONTO_TZ))
    args = parse_args()
    dates = get_full_dates_to_process() if args.full_refresh else get_dates_to_process()
    run(dates, full_refresh=args.full_refresh)
    elapsed = (datetime.now(ZoneInfo(TORONTO_TZ)) - started_at).total_seconds()
    logging.info("%s 完成，耗时: %.1f 秒", TABLE_NAME, elapsed)
