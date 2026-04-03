import argparse
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from google.cloud import bigquery

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from jobs.dws.dws_social_video_daily import run_dws_social_video_daily

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
VIDEO_SOURCE_DATASET_ID = "videos"
TORONTO_TZ = "America/Toronto"
SOCIAL_CONTENT_REBUILD_DAYS = 30
DISPLAY_CHANNELS = ["全部渠道", "youtube", "tiktok", "ins"]
YOUTUBE_TEST_CHANNEL_IDS = [
    "UCynAg7WHdiy8SuQpESO__yw",
    "UCgBJozWjiE0cqwv7UNalyog",
]
client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="构建 Social Content 社媒表现数据日报（dt × 展示渠道）"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="按全历史发布时间范围重刷全量分区，而不是默认最近 30 天",
    )
    return parser.parse_args()


def dates_to_sql_list(dates):
    return ", ".join([f"'{d}'" for d in dates])


def dates_to_array_sql(dates):
    return ", ".join([f"DATE '{d}'" for d in dates])


def strings_to_sql_list(values):
    return ", ".join([f"'{value}'" for value in values])


def strings_to_array_sql(values):
    return ", ".join([f"'{value}'" for value in values])


def get_social_content_dates_to_process(rebuild_days=SOCIAL_CONTENT_REBUILD_DAYS):
    """
    Social Content 只依赖视频源表，独立重刷最近 N 天分区。
    这样不会受事件 ADS 主链路调度与失败状态影响。
    """
    end_date = datetime.now(ZoneInfo(TORONTO_TZ)).date()
    start_date = end_date - timedelta(days=rebuild_days - 1)
    return [start_date + timedelta(days=offset) for offset in range(rebuild_days)]


def get_social_content_full_dates_to_process():
    """
    全量回刷时，从三端视频最早发布时间开始生成完整日期序列。
    """
    query = f"""
    WITH published_dates AS (
        SELECT MIN(DATE(TIMESTAMP(published_at, 'UTC'), '{TORONTO_TZ}')) AS min_dt
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.yt_videos`
        WHERE channel_id NOT IN ({strings_to_sql_list(YOUTUBE_TEST_CHANNEL_IDS)})

        UNION ALL

        SELECT MIN(DATE(TIMESTAMP_SECONDS(SAFE_CAST(create_time AS INT64)), '{TORONTO_TZ}')) AS min_dt
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.videos`

        UNION ALL

        SELECT MIN(DATE(TIMESTAMP(published_at, 'UTC'), '{TORONTO_TZ}')) AS min_dt
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.ig_media`
    )
    SELECT
        MIN(min_dt) AS start_dt,
        CURRENT_DATE('{TORONTO_TZ}') AS end_dt
    FROM published_dates
    WHERE min_dt IS NOT NULL
    """
    rows = list(client.query(query).result())
    if not rows or rows[0].start_dt is None or rows[0].end_dt is None:
        return []

    start_date = rows[0].start_dt
    end_date = rows[0].end_dt
    total_days = (end_date - start_date).days + 1
    return [start_date + timedelta(days=offset) for offset in range(total_days)]


def build_ads_daily_social_content_select_sql(dates: list[date]) -> str:
    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    channels_array_sql = strings_to_array_sql(DISPLAY_CHANNELS)

    return f"""
    WITH
    -- 本次需要产出的目标日期列表
    date_list AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 最终需要补齐的 4 个展示渠道
    channel_list AS (
        SELECT channel
        FROM UNNEST([{channels_array_sql}]) AS channel
    ),
    -- 只读取本次窗口涉及到的社媒视频日表分区
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
            has_snapshot_to_dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_social_video_daily`
        WHERE dt IN ({dates_str})
    ),
    -- 先汇总真实渠道的每日新增 views / likes
    base_daily_channel_metrics AS (
        SELECT
            dt,
            channel,
            COALESCE(SUM(daily_view_increment), 0) AS daily_view_increment,
            COALESCE(SUM(daily_like_increment), 0) AS daily_like_increment
        FROM social_video_daily
        WHERE dt IN ({dates_str})
        GROUP BY dt, channel
    ),
    -- 在真实渠道结果基础上补出“全部渠道”聚合行
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
        WHERE dt IN ({dates_str})
        GROUP BY dt
    ),
    -- 统计各真实渠道截至前一天已发布过视频的历史去重博主数
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
    -- 在真实渠道基础上补出“全部渠道”博主分母，按 channel + creator_id 去重
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
    -- 对每个日期、渠道、博主挑出截至当天已有数据的最新 3 条视频样本
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
            WHERE dt IN ({dates_str})
                AND creator_id IS NOT NULL
                AND has_snapshot_to_dt
                AND day_end_views > 0
        )
        WHERE rn <= 3
    ),
    -- 先汇总真实渠道“最新 3 条”样本池的 likes / views / 样本量
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
    -- 在真实渠道样本池基础上补出“全部渠道”样本池统计
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
        COALESCE(b.creator_count_to_prev_day, 0) AS creator_count_to_prev_day,
        CAST(CASE
            WHEN COALESCE(b.creator_count_to_prev_day, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(m.daily_view_increment, 0) AS NUMERIC)
                / CAST(b.creator_count_to_prev_day AS NUMERIC),
                4
            )
        END AS NUMERIC) AS daily_view_increment_per_creator,
        COALESCE(l.latest_3_sample_creator_count, 0) AS latest_3_sample_creator_count,
        COALESCE(l.latest_3_sample_video_count, 0) AS latest_3_sample_video_count,
        COALESCE(l.latest_3_sample_like_count, 0) AS latest_3_sample_like_count,
        COALESCE(l.latest_3_sample_view_count, 0) AS latest_3_sample_view_count,
        CAST(CASE
            WHEN COALESCE(l.latest_3_sample_view_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(l.latest_3_sample_like_count, 0) AS NUMERIC)
                / CAST(l.latest_3_sample_view_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS latest_3_avg_like_view_rate,
        CURRENT_TIMESTAMP() AS update_time
    FROM date_list d
    CROSS JOIN channel_list c
    LEFT JOIN daily_channel_metrics m
        ON d.dt = m.dt
        AND c.channel = m.channel
    LEFT JOIN creator_base b
        ON d.dt = b.dt
        AND c.channel = b.channel
    LEFT JOIN latest_3_channel_metrics l
        ON d.dt = l.dt
        AND c.channel = l.channel
    """


def run_ads_daily_social_content(dates: list[date]):
    """
    Social Content 主题日报（dt × 展示渠道）:
    - 先刷新 dws_social_video_daily，统一承接视频补日期、补累计、算日增
    - 再从 dws_social_video_daily 聚合渠道指标、历史博主分母和最新 3 条样本池
    - `全部渠道` 直接写入主表，博主去重 key 使用 `channel + creator_id`
    """
    if not dates:
        logging.info("ads_daily_social_content 无待处理日期，跳过")
        return

    min_date = min(dates)
    max_date = max(dates)
    select_sql = build_ads_daily_social_content_select_sql(dates)
    target_table = f"{PROJECT_ID}.{DATASET_ID}.ads_daily_social_content"
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    staging_table = f"{target_table}_tmp_{run_id}"
    changed_table = f"{target_table}_changed_{run_id}"
    expected_row_count = len(dates) * len(DISPLAY_CHANNELS)
    target_dropped = False

    logging.info("开始先刷新上游表 dws_social_video_daily")
    run_dws_social_video_daily(dates)

    insert_into_staging_sql = f"""
    INSERT INTO `{staging_table}`
    (
        dt,
        channel,
        daily_view_increment,
        daily_like_increment,
        creator_count_to_prev_day,
        daily_view_increment_per_creator,
        latest_3_sample_creator_count,
        latest_3_sample_video_count,
        latest_3_sample_like_count,
        latest_3_sample_view_count,
        latest_3_avg_like_view_rate,
        update_time
    )
    {select_sql}
    """
    validate_staging_sql = f"""
    SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT dt) AS dt_count,
        COUNT(DISTINCT channel) AS channel_count
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
        "开始处理 ads_daily_social_content, 目标表: %s, 临时表: %s, 替换表: %s",
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
        client.query(insert_into_staging_sql).result()

        validation_row = list(client.query(validate_staging_sql).result())[0]
        if validation_row.row_count != expected_row_count:
            raise ValueError(
                "ads_daily_social_content staging 行数校验失败: "
                f"expected={expected_row_count}, actual={validation_row.row_count}"
            )
        if validation_row.dt_count != len(dates):
            raise ValueError(
                "ads_daily_social_content staging 日期数校验失败: "
                f"expected={len(dates)}, actual={validation_row.dt_count}"
            )
        if validation_row.channel_count != len(DISPLAY_CHANNELS):
            raise ValueError(
                "ads_daily_social_content staging 渠道数校验失败: "
                f"expected={len(DISPLAY_CHANNELS)}, actual={validation_row.channel_count}"
            )

        client.query(create_changed_table_sql).result()
        client.query(f"DROP TABLE `{target_table}`").result()
        target_dropped = True
        client.query(
            f"ALTER TABLE `{changed_table}` RENAME TO ads_daily_social_content"
        ).result()
        client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
    except Exception:
        if not target_dropped:
            client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
            client.query(f"DROP TABLE IF EXISTS `{changed_table}`").result()
        raise

    logging.info(
        "ads_daily_social_content 刷新完成, 处理日期: %s ~ %s, 共 %s 天",
        min_date,
        max_date,
        len(dates),
    )


if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)

    args = parse_args()
    if args.full_refresh:
        dates = get_social_content_full_dates_to_process()
    else:
        dates = get_social_content_dates_to_process()
    if not dates:
        logging.info("没有 Social Content 数据需要处理")
        raise SystemExit(0)

    logging.info(
        "Social Content 待处理日期范围: %s ~ %s，共 %s 天",
        min(dates),
        max(dates),
        len(dates),
    )
    run_ads_daily_social_content(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("ads_daily_social_content 执行完成, 耗时: %.1f 秒", elapsed)
