import argparse
from datetime import date, datetime, timedelta, timezone
import logging

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from zoneinfo import ZoneInfo


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "ads_daily_social_affiliate_video"
VIDEO_STATE_TABLE_NAME = "dws_social_video_state_daily_v2"
VIDEO_SNAPSHOT_TABLE_NAME = "dws_social_video_snapshot_daily_v2"
CREATOR_SNAPSHOT_TABLE_NAME = "dws_social_creator_snapshot_daily"
DIM_VIDEO_TABLE_NAME = "dim_video"
# 橱窗号范围：服务端账号表的 usage_type 枚举，只有 TikTok 有这类账号。
AFFILIATE_USAGE_TYPE = "tiktok-affiliate"
AFFILIATE_CHANNEL = "tiktok"
INCREMENTAL_WINDOW_HOURS = 6

client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="构建橱窗号视频维度日报（dt × video_key）"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="从橱窗号首个真实播放快照日重刷全部统计日",
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
        client.query(
            f"SELECT COUNT(*) AS row_count FROM `{table_id(TABLE_NAME)}`"
        ).result()
    )
    return int(rows[0].row_count) if rows else 0


def affiliate_creator_sql() -> str:
    """橱窗号名单固定取账号日快照的最新分区，账号数很少且名单本身没有历史。"""
    return f"""
    SELECT
        creator_id,
        account_handle,
        account_name,
        account_url
    FROM `{table_id(CREATOR_SNAPSHOT_TABLE_NAME)}`
    WHERE channel = '{AFFILIATE_CHANNEL}'
        AND usage_type = '{AFFILIATE_USAGE_TYPE}'
        AND dt = (
            SELECT MAX(dt)
            FROM `{table_id(CREATOR_SNAPSHOT_TABLE_NAME)}`
            WHERE channel = '{AFFILIATE_CHANNEL}'
        )
    """


def get_report_start_date() -> date | None:
    """橱窗号播放采集晚于 Social Content v2 历史起点，早于首个真实快照日的分区没有意义。"""
    query = f"""
    WITH affiliate_creator AS (
        {affiliate_creator_sql()}
    )
    SELECT MIN(s.dt) AS start_dt
    FROM `{table_id(VIDEO_SNAPSHOT_TABLE_NAME)}` s
    INNER JOIN affiliate_creator a
        ON s.creator_id = a.creator_id
    WHERE s.channel = '{AFFILIATE_CHANNEL}'
    """
    rows = list(client.query(query).result())
    return rows[0].start_dt if rows else None


def get_full_dates_to_process() -> list[date]:
    start_dt = get_report_start_date()
    if start_dt is None:
        return []

    end_dt = datetime.now(ZoneInfo(TORONTO_TZ)).date() - timedelta(days=1)
    if end_dt < start_dt:
        return []
    return [
        start_dt + timedelta(days=offset) for offset in range((end_dt - start_dt).days + 1)
    ]


def get_dates_to_process() -> list[date]:
    if not target_table_exists() or get_target_row_count() == 0:
        return get_full_dates_to_process()

    start_dt = get_report_start_date()
    if start_dt is None:
        return []

    query = f"""
    WITH
    -- 播放状态表某统计日刷新后，该日及其次日（作为对比基准日）都要重算。
    changed_video_dates AS (
        SELECT DISTINCT dt
        FROM `{table_id(VIDEO_STATE_TABLE_NAME)}`
        WHERE channel = '{AFFILIATE_CHANNEL}'
            AND update_time >= TIMESTAMP_SUB(
                CURRENT_TIMESTAMP(), INTERVAL {INCREMENTAL_WINDOW_HOURS} HOUR
            )
    ),
    -- 目标表水位之后已就绪的统计日需要补齐。
    missing_dates AS (
        SELECT DISTINCT dt
        FROM `{table_id(VIDEO_STATE_TABLE_NAME)}`
        WHERE channel = '{AFFILIATE_CHANNEL}'
            AND dt > (SELECT MAX(dt) FROM `{table_id(TABLE_NAME)}`)
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt FROM changed_video_dates
        UNION ALL
        SELECT DATE_ADD(dt, INTERVAL 1 DAY) AS dt FROM changed_video_dates
        UNION ALL
        SELECT dt FROM missing_dates
    )
    WHERE dt < CURRENT_DATE('{TORONTO_TZ}')
        AND dt >= DATE '{start_dt}'
    ORDER BY dt
    """
    return [row.dt for row in client.query(query).result()]


def dates_to_array_sql(dates: list[date]) -> str:
    return ", ".join(f"DATE '{stat_dt}'" for stat_dt in dates)


def build_select_sql(dates: list[date]) -> str:
    dates_sql = dates_to_array_sql(dates)
    # 增量需要同时读取统计日与其前一日的日末状态分区。
    state_dates = sorted(set(dates) | {stat_dt - timedelta(days=1) for stat_dt in dates})
    state_dates_sql = dates_to_array_sql(state_dates)

    return f"""
    WITH
    -- 橱窗号名单。
    affiliate_creator AS (
        {affiliate_creator_sql()}
    ),
    -- 橱窗号视频展示属性，标题与链接统一来自视频维表。
    affiliate_video AS (
        SELECT
            v.creator_id,
            v.video_id,
            v.video_key,
            v.video_title,
            v.video_link,
            v.published_ts_utc,
            v.published_dt
        FROM `{table_id(DIM_VIDEO_TABLE_NAME)}` v
        INNER JOIN affiliate_creator a
            ON v.creator_id = a.creator_id
        WHERE v.channel = '{AFFILIATE_CHANNEL}'
    ),
    -- 橱窗号视频在统计日与前一日的日末累计状态。
    video_state AS (
        SELECT
            s.dt,
            s.video_key,
            s.day_end_views,
            s.day_end_likes,
            s.day_end_comments,
            s.day_end_shares
        FROM `{table_id(VIDEO_STATE_TABLE_NAME)}` s
        INNER JOIN affiliate_creator a
            ON s.creator_id = a.creator_id
        WHERE s.channel = '{AFFILIATE_CHANNEL}'
            AND s.dt IN UNNEST([{state_dates_sql}])
    )
    -- 单视频当日增量：前一日无状态说明视频当天首发，累计值即当日增量。
    SELECT
        cur.dt,
        '{AFFILIATE_CHANNEL}' AS channel,
        v.creator_id,
        a.account_name,
        a.account_handle,
        a.account_url,
        v.video_id,
        v.video_key,
        v.video_title,
        v.video_link,
        v.published_ts_utc,
        v.published_dt,
        cur.day_end_views - COALESCE(prev.day_end_views, 0) AS view_increment,
        cur.day_end_views AS total_view_count,
        cur.day_end_likes - COALESCE(prev.day_end_likes, 0) AS like_increment,
        cur.day_end_likes AS total_like_count,
        cur.day_end_comments - COALESCE(prev.day_end_comments, 0) AS comment_increment,
        cur.day_end_comments AS total_comment_count,
        cur.day_end_shares - COALESCE(prev.day_end_shares, 0) AS share_increment,
        cur.day_end_shares AS total_share_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM video_state cur
    INNER JOIN affiliate_video v
        ON cur.video_key = v.video_key
    INNER JOIN affiliate_creator a
        ON v.creator_id = a.creator_id
    LEFT JOIN video_state prev
        ON cur.video_key = prev.video_key
        AND prev.dt = DATE_SUB(cur.dt, INTERVAL 1 DAY)
    WHERE cur.dt IN UNNEST([{dates_sql}])
    """


def validate(staging_table: str) -> None:
    result = list(
        client.query(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT CONCAT(CAST(dt AS STRING), '|', video_key))
                    AS unique_video_count,
                COUNTIF(published_dt > dt) AS invalid_published_count,
                COUNTIF(total_view_count < 0
                    OR total_like_count < 0
                    OR total_comment_count < 0) AS invalid_total_count,
                COUNTIF(channel != '{AFFILIATE_CHANNEL}') AS invalid_channel_count,
                COUNTIF(video_key IS NULL OR video_key = '') AS invalid_video_key_count
            FROM `{staging_table}`
            """
        ).result()
    )[0]
    if result.row_count == 0:
        raise RuntimeError("橱窗号视频日报校验失败：staging 无数据")
    if result.row_count != result.unique_video_count:
        raise RuntimeError("橱窗号视频日报校验失败：统计日和视频不唯一")
    if result.invalid_published_count != 0:
        raise RuntimeError("橱窗号视频日报校验失败：存在发布日期晚于统计日的视频")
    if result.invalid_total_count != 0:
        raise RuntimeError("橱窗号视频日报校验失败：存在负数日末累计值")
    if result.invalid_channel_count != 0:
        raise RuntimeError("橱窗号视频日报校验失败：存在非 TikTok 渠道行")
    if result.invalid_video_key_count != 0:
        raise RuntimeError("橱窗号视频日报校验失败：存在空 video_key")


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
            PARTITION BY dt
            CLUSTER BY channel, creator_id, video_key AS
            {build_select_sql(dates)}
            """
        ).result()

        validate(staging_table)

        if target_table_exists():
            if full_refresh:
                client.query(
                    f"""
                    CREATE TABLE `{changed_table}`
                    PARTITION BY dt
                    CLUSTER BY channel, creator_id, video_key AS
                    SELECT * FROM `{staging_table}`
                    """
                ).result()
            else:
                client.query(
                    f"""
                    CREATE TABLE `{changed_table}`
                    PARTITION BY dt
                    CLUSTER BY channel, creator_id, video_key AS
                    SELECT * FROM `{target_table}`
                    WHERE dt NOT IN UNNEST([{dates_sql}])

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
                PARTITION BY dt
                CLUSTER BY channel, creator_id, video_key AS
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
        "%s 刷新完成，统计日期: %s ~ %s，共 %s 天",
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
