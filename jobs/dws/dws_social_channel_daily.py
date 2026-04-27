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
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_social_channel_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
SOCIAL_CHANNEL_REBUILD_DAYS = 30
DISPLAY_CHANNELS = ["全部渠道", "youtube", "tiktok", "ins"]

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
        description="构建 Social Content 渠道级日事实（dt × channel）"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="按 dws_social_video_daily 已落地历史日期全量重刷，而不是默认最近 30 天",
    )
    return parser.parse_args()


def dates_to_sql_list(dates: list[date]) -> str:
    return ", ".join([f"'{d}'" for d in dates])


def dates_to_array_sql(dates: list[date]) -> str:
    return ", ".join([f"DATE '{d}'" for d in dates])


def strings_to_array_sql(values: list[str]) -> str:
    return ", ".join([f"'{value}'" for value in values])


def target_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


def target_table_exists() -> bool:
    try:
        client.get_table(target_table_id())
        return True
    except NotFound:
        return False


def get_social_channel_dates_to_process(
    rebuild_days: int = SOCIAL_CHANNEL_REBUILD_DAYS,
) -> list[date]:
    """
    Social 渠道级事实依赖视频快照和自动发布映射，默认固定重刷最近 30 天。
    这样可以直接覆盖视频补采和自动发布映射修正的常见场景。
    """
    end_date = datetime.now(ZoneInfo(TORONTO_TZ)).date()
    start_date = end_date - timedelta(days=rebuild_days - 1)
    return [start_date + timedelta(days=offset) for offset in range(rebuild_days)]


def get_social_channel_full_dates_to_process() -> list[date]:
    """
    全量重刷时，按 dws_social_video_daily 已落地日期全量展开。
    """
    query = f"""
    SELECT DISTINCT dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dws_social_video_daily`
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def build_select_sql(dates: list[date]) -> str:
    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    channels_array_sql = strings_to_array_sql(DISPLAY_CHANNELS)
    max_date = max(dates)

    return f"""
    WITH
    -- 本次需要产出的目标日期列表。
    date_list AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 固定产出 4 个展示渠道。
    channel_list AS (
        SELECT channel
        FROM UNNEST([{channels_array_sql}]) AS channel
    ),
    -- 当前窗口涉及的社媒视频逐日事实。
    social_video_daily AS (
        SELECT
            dt,
            channel,
            creator_id,
            video_key,
            published_dt,
            published_ts_utc,
            day_end_views,
            day_end_likes,
            daily_view_increment,
            daily_like_increment,
            has_snapshot_to_dt,
            is_new_video,
            new_video_contributed_view_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_social_video_daily`
        WHERE dt IN ({dates_str})
    ),
    -- 自动发布映射统一从 DIM 读取，不再在社媒主题重复拼平台上传表。
    auto_publish_video_task_dim AS (
        SELECT
            video_key,
            unified_video_key
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_auto_publish_video_task`
    ),
    -- 为待刷窗口内的新增视频准备“全部渠道”统一视频键。
    all_channel_new_video_base AS (
        SELECT
            s.dt,
            s.video_key,
            s.new_video_contributed_view_count,
            COALESCE(m.unified_video_key, s.video_key) AS all_channel_video_key
        FROM social_video_daily s
        LEFT JOIN auto_publish_video_task_dim m
            ON s.video_key = m.video_key
        WHERE s.is_new_video
    ),
    -- 取待刷窗口涉及的统一视频键，后续只回看这些键的历史新增日，控制扫描范围。
    relevant_all_channel_video_keys AS (
        SELECT DISTINCT all_channel_video_key
        FROM all_channel_new_video_base
    ),
    -- 回看统一视频键的历史新增日，确保自动发布跨平台视频只在最早首次正播放日期记一次新增。
    all_channel_video_first_positive_dt AS (
        SELECT
            COALESCE(m.unified_video_key, v.video_key) AS all_channel_video_key,
            MIN(v.dt) AS first_positive_view_dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_social_video_daily` v
        LEFT JOIN auto_publish_video_task_dim m
            ON v.video_key = m.video_key
        INNER JOIN relevant_all_channel_video_keys r
            ON COALESCE(m.unified_video_key, v.video_key) = r.all_channel_video_key
        WHERE v.is_new_video
            AND v.dt <= DATE '{max_date}'
        GROUP BY all_channel_video_key
    ),
    -- 先汇总真实渠道的每日新增 views / likes。
    base_daily_channel_metrics AS (
        SELECT
            dt,
            channel,
            COALESCE(SUM(daily_view_increment), 0) AS daily_view_increment,
            COALESCE(SUM(daily_like_increment), 0) AS daily_like_increment
        FROM social_video_daily
        GROUP BY dt, channel
    ),
    -- 在真实渠道结果基础上补出“全部渠道”聚合行。
    daily_channel_metrics AS (
        SELECT
            dt,
            channel,
            daily_view_increment,
            daily_like_increment
        FROM base_daily_channel_metrics

        UNION ALL

        SELECT
            dt,
            '全部渠道' AS channel,
            COALESCE(SUM(daily_view_increment), 0) AS daily_view_increment,
            COALESCE(SUM(daily_like_increment), 0) AS daily_like_increment
        FROM social_video_daily
        GROUP BY dt
    ),
    -- 先汇总真实渠道的每日新增视频数与新增视频贡献播放数。
    base_new_video_channel_metrics AS (
        SELECT
            dt,
            channel,
            COUNT(DISTINCT IF(is_new_video, video_key, NULL)) AS new_video_count,
            COALESCE(SUM(new_video_contributed_view_count), 0)
                AS new_video_contributed_view_count
        FROM social_video_daily
        GROUP BY dt, channel
    ),
    -- 在真实渠道结果基础上补出“全部渠道”的新增视频聚合行。
    new_video_channel_metrics AS (
        SELECT
            dt,
            channel,
            new_video_count,
            new_video_contributed_view_count
        FROM base_new_video_channel_metrics

        UNION ALL

        SELECT
            b.dt,
            '全部渠道' AS channel,
            COUNT(
                DISTINCT IF(
                    f.first_positive_view_dt = b.dt,
                    b.all_channel_video_key,
                    NULL
                )
            ) AS new_video_count,
            COALESCE(SUM(b.new_video_contributed_view_count), 0)
                AS new_video_contributed_view_count
        FROM all_channel_new_video_base b
        LEFT JOIN all_channel_video_first_positive_dt f
            ON b.all_channel_video_key = f.all_channel_video_key
        GROUP BY b.dt
    ),
    -- 统计各真实渠道截至前一天已发布过视频的历史去重博主数。
    base_creator_base AS (
        SELECT
            dt,
            channel,
            COUNT(DISTINCT creator_id) AS creator_count_to_prev_day
        FROM social_video_daily
        WHERE creator_id IS NOT NULL
            AND published_dt < dt
        GROUP BY dt, channel
    ),
    -- 在真实渠道基础上补出“全部渠道”博主分母，按 channel + creator_id 去重。
    creator_base AS (
        SELECT
            dt,
            channel,
            creator_count_to_prev_day
        FROM base_creator_base

        UNION ALL

        SELECT
            dt,
            '全部渠道' AS channel,
            COUNT(DISTINCT CONCAT(channel, ':', creator_id)) AS creator_count_to_prev_day
        FROM social_video_daily
        WHERE creator_id IS NOT NULL
            AND published_dt < dt
        GROUP BY dt
    ),
    -- 对每个日期、渠道、博主挑出截至当天已有数据的最新 3 条视频样本。
    latest_3_pool AS (
        SELECT
            dt,
            channel,
            creator_id,
            video_key,
            day_end_likes,
            day_end_views
        FROM (
            SELECT
                dt,
                channel,
                creator_id,
                video_key,
                day_end_likes,
                day_end_views,
                ROW_NUMBER() OVER (
                    PARTITION BY dt, channel, creator_id
                    ORDER BY published_ts_utc DESC, video_key DESC
                ) AS rn
            FROM social_video_daily
            WHERE creator_id IS NOT NULL
                AND has_snapshot_to_dt
                AND day_end_views > 0
        )
        WHERE rn <= 3
    ),
    -- 先汇总真实渠道“最新 3 条”样本池的 likes / views / 样本量。
    base_latest_3_channel_metrics AS (
        SELECT
            dt,
            channel,
            COUNT(DISTINCT creator_id) AS latest_3_sample_creator_count,
            COUNT(*) AS latest_3_sample_video_count,
            COALESCE(SUM(day_end_likes), 0) AS latest_3_sample_like_count,
            COALESCE(SUM(day_end_views), 0) AS latest_3_sample_view_count
        FROM latest_3_pool
        GROUP BY dt, channel
    ),
    -- 在真实渠道样本池基础上补出“全部渠道”样本池统计。
    latest_3_channel_metrics AS (
        SELECT
            dt,
            channel,
            latest_3_sample_creator_count,
            latest_3_sample_video_count,
            latest_3_sample_like_count,
            latest_3_sample_view_count
        FROM base_latest_3_channel_metrics

        UNION ALL

        SELECT
            dt,
            '全部渠道' AS channel,
            COUNT(DISTINCT CONCAT(channel, ':', creator_id)) AS latest_3_sample_creator_count,
            COUNT(*) AS latest_3_sample_video_count,
            COALESCE(SUM(day_end_likes), 0) AS latest_3_sample_like_count,
            COALESCE(SUM(day_end_views), 0) AS latest_3_sample_view_count
        FROM latest_3_pool
        GROUP BY dt
    )
    SELECT
        d.dt,
        c.channel,
        COALESCE(m.daily_view_increment, 0) AS daily_view_increment,
        COALESCE(m.daily_like_increment, 0) AS daily_like_increment,
        COALESCE(n.new_video_count, 0) AS new_video_count,
        COALESCE(n.new_video_contributed_view_count, 0)
            AS new_video_contributed_view_count,
        COALESCE(b.creator_count_to_prev_day, 0) AS creator_count_to_prev_day,
        COALESCE(l.latest_3_sample_creator_count, 0)
            AS latest_3_sample_creator_count,
        COALESCE(l.latest_3_sample_video_count, 0)
            AS latest_3_sample_video_count,
        COALESCE(l.latest_3_sample_like_count, 0)
            AS latest_3_sample_like_count,
        COALESCE(l.latest_3_sample_view_count, 0)
            AS latest_3_sample_view_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM date_list d
    CROSS JOIN channel_list c
    LEFT JOIN daily_channel_metrics m
        ON d.dt = m.dt
        AND c.channel = m.channel
    LEFT JOIN new_video_channel_metrics n
        ON d.dt = n.dt
        AND c.channel = n.channel
    LEFT JOIN creator_base b
        ON d.dt = b.dt
        AND c.channel = b.channel
    LEFT JOIN latest_3_channel_metrics l
        ON d.dt = l.dt
        AND c.channel = l.channel
    """


def run_dws_social_channel_daily(dates: list[date]) -> None:
    if not dates:
        logging.info("没有新的 dws_social_channel_daily 日期需要处理")
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
        creator_count_to_prev_day,
        latest_3_sample_creator_count,
        latest_3_sample_video_count,
        latest_3_sample_like_count,
        latest_3_sample_view_count,
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
        dates = get_social_channel_full_dates_to_process()
    else:
        dates = get_social_channel_dates_to_process()

    logging.info("待处理日期: %s", dates if dates else "[]")
    if not dates:
        logging.info("没有新的 dws_social_channel_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_social_channel_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
