from datetime import datetime, timedelta, timezone
import logging
from zoneinfo import ZoneInfo

from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
VIDEO_SOURCE_DATASET_ID = "videos"
TORONTO_TZ = "America/Toronto"
VIDEO_REBUILD_DAYS = 30
YOUTUBE_TEST_CHANNEL_IDS = [
    "UCynAg7WHdiy8SuQpESO__yw",
    "UCgBJozWjiE0cqwv7UNalyog",
]
client = bigquery.Client(project=PROJECT_ID)


def dates_to_sql_list(dates):
    return ", ".join([f"'{d}'" for d in dates])


def strings_to_sql_list(values):
    return ", ".join([f"'{value}'" for value in values])


def get_video_dates_to_process(rebuild_days=VIDEO_REBUILD_DAYS):
    """
    视频播放快照存在补采和同日多次刷新，固定重刷最近 N 天分区。
    使用多伦多时间（America/Toronto）。
    """
    end_date = datetime.now(ZoneInfo(TORONTO_TZ)).date()
    start_date = end_date - timedelta(days=rebuild_days - 1)
    return [start_date + timedelta(days=offset) for offset in range(rebuild_days)]


def run_dws_video_daily(dates):
    """
    纯 SQL 计算 dws_video_daily，DELETE + INSERT 保证幂等。
    口径：
    - 发布时间、采集时间统一按 America/Toronto 切天
    - 各渠道 view_count 为累计值，日增量按日末累计值与前一日做差
    - 次日无采集记录时，沿用前一日日末累计值，次日增量记为 0
    """
    if not dates:
        logging.info("dws_video_daily 无待处理日期，跳过")
        return

    dates_str = dates_to_sql_list(dates)
    min_date = min(dates)
    max_date = max(dates)
    youtube_test_channel_ids_sql = strings_to_sql_list(YOUTUBE_TEST_CHANNEL_IDS)

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.dws_video_daily`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.dws_video_daily`
    (
        dt,
        channel,
        video_id,
        video_key,
        published_dt,
        raw_day_end_views,
        day_end_views,
        daily_view_increment,
        has_snapshot,
        is_active_video,
        is_new_video,
        new_video_contributed_view_count,
        update_time
    )
    WITH
    -- 定义本次重刷窗口及其种子日期。
    params AS (
        SELECT
            DATE('{min_date}') AS min_dt,
            DATE('{max_date}') AS max_dt,
            DATE_SUB(DATE('{min_date}'), INTERVAL 1 DAY) AS seed_dt
    ),
    -- 统一三端视频维表，并按多伦多时区取发布日期。
    video_dim AS (
        SELECT
            channel,
            video_id,
            video_key,
            published_dt
        FROM (
            SELECT
                'youtube' AS channel,
                CAST(video_id AS STRING) AS video_id,
                CONCAT('youtube:', CAST(video_id AS STRING)) AS video_key,
                DATE(TIMESTAMP(published_at, 'UTC'), '{TORONTO_TZ}') AS published_dt
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.yt_videos`
            WHERE channel_id NOT IN ({youtube_test_channel_ids_sql})

            UNION ALL

            SELECT
                'tiktok' AS channel,
                CAST(id AS STRING) AS video_id,
                CONCAT('tiktok:', CAST(id AS STRING)) AS video_key,
                DATE(TIMESTAMP_SECONDS(SAFE_CAST(create_time AS INT64)), '{TORONTO_TZ}') AS published_dt
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.videos`

            UNION ALL

            SELECT
                'ins' AS channel,
                CAST(media_id AS STRING) AS video_id,
                CONCAT('ins:', CAST(media_id AS STRING)) AS video_key,
                DATE(TIMESTAMP(published_at, 'UTC'), '{TORONTO_TZ}') AS published_dt
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.ig_media`
        )
        WHERE published_dt IS NOT NULL
            AND published_dt <= (SELECT max_dt FROM params)
    ),
    -- 统一三端播放快照，并按多伦多时区切天。
    video_snapshot AS (
        SELECT
            channel,
            video_id,
            video_key,
            collected_ts_utc,
            source_id,
            DATE(collected_ts_utc, '{TORONTO_TZ}') AS dt,
            cum_views
        FROM (
            SELECT
                'youtube' AS channel,
                CAST(video_id AS STRING) AS video_id,
                CONCAT('youtube:', CAST(video_id AS STRING)) AS video_key,
                TIMESTAMP(collected_at, 'UTC') AS collected_ts_utc,
                SAFE_CAST(id AS INT64) AS source_id,
                SAFE_CAST(view_count AS INT64) AS cum_views
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.yt_video_stats_log`

            UNION ALL

            SELECT
                'tiktok' AS channel,
                CAST(video_id AS STRING) AS video_id,
                CONCAT('tiktok:', CAST(video_id AS STRING)) AS video_key,
                TIMESTAMP(collected_at, 'UTC') AS collected_ts_utc,
                0 AS source_id,
                SAFE_CAST(view_count AS INT64) AS cum_views
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.video_stats_log`

            UNION ALL

            SELECT
                'ins' AS channel,
                CAST(media_id AS STRING) AS video_id,
                CONCAT('ins:', CAST(media_id AS STRING)) AS video_key,
                TIMESTAMP(collected_at, 'UTC') AS collected_ts_utc,
                0 AS source_id,
                SAFE_CAST(view_count AS INT64) AS cum_views
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.ig_media_stats_log`
        )
        WHERE collected_ts_utc IS NOT NULL
            AND cum_views IS NOT NULL
            AND DATE(collected_ts_utc, '{TORONTO_TZ}') <= (SELECT max_dt FROM params)
    ),
    -- 取每个视频每天最后一次采集到的累计播放值。
    daily_latest AS (
        SELECT
            video_key,
            dt,
            cum_views AS raw_day_end_views
        FROM (
            SELECT
                video_key,
                dt,
                cum_views,
                ROW_NUMBER() OVER (
                    PARTITION BY video_key, dt
                    ORDER BY collected_ts_utc DESC, source_id DESC
                ) AS rn
            FROM video_snapshot
        )
        WHERE rn = 1
    ),
    -- 识别每个视频首次被采到正播放量的日期。
    first_positive_snapshot AS (
        SELECT
            video_key,
            MIN(dt) AS first_positive_view_dt
        FROM daily_latest
        WHERE raw_day_end_views > 0
        GROUP BY video_key
    ),
    -- 为重刷窗口准备前一日种子累计播放值，保证缺采时能向前填充。
    seed_snapshot AS (
        SELECT
            video_key,
            raw_day_end_views AS seed_day_end_views
        FROM (
            SELECT
                video_key,
                raw_day_end_views,
                ROW_NUMBER() OVER (
                    PARTITION BY video_key
                    ORDER BY dt DESC
                ) AS rn
            FROM daily_latest
            WHERE dt < (SELECT min_dt FROM params)
        )
        WHERE rn = 1
    ),
    -- 仅保留本次窗口真正相关的视频，避免无关历史视频进入 spine。
    relevant_videos AS (
        SELECT
            v.channel,
            v.video_id,
            v.video_key,
            v.published_dt
        FROM video_dim v
        WHERE
            v.published_dt >= (SELECT seed_dt FROM params)
            OR EXISTS (
                SELECT 1
                FROM daily_latest s
                WHERE s.video_key = v.video_key
                    AND s.dt >= (SELECT seed_dt FROM params)
            )
    ),
    -- 为相关视频生成按天连续的日期骨架。
    date_spine AS (
        SELECT
            v.channel,
            v.video_id,
            v.video_key,
            v.published_dt,
            dt
        FROM relevant_videos v,
        UNNEST(
            GENERATE_DATE_ARRAY(
                GREATEST(v.published_dt, (SELECT seed_dt FROM params)),
                (SELECT max_dt FROM params)
            )
        ) AS dt
    ),
    -- 把每日快照挂到日期骨架上，并为种子日补齐窗口前累计值。
    spine_with_snapshot AS (
        SELECT
            s.dt,
            s.channel,
            s.video_id,
            s.video_key,
            s.published_dt,
            CASE
                WHEN s.dt = (SELECT seed_dt FROM params)
                    AND l.raw_day_end_views IS NULL
                THEN ss.seed_day_end_views
                ELSE l.raw_day_end_views
            END AS raw_day_end_views,
            (l.raw_day_end_views IS NOT NULL) AS has_snapshot
        FROM date_spine s
        LEFT JOIN daily_latest l
            ON s.video_key = l.video_key
            AND s.dt = l.dt
        LEFT JOIN seed_snapshot ss
            ON s.video_key = ss.video_key
    ),
    -- 对缺采日期沿用前一日累计播放值，得到连续日末累计值。
    filled_daily AS (
        SELECT
            dt,
            channel,
            video_id,
            video_key,
            published_dt,
            raw_day_end_views,
            COALESCE(
                LAST_VALUE(raw_day_end_views IGNORE NULLS) OVER (
                    PARTITION BY video_key
                    ORDER BY dt
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ),
                0
            ) AS day_end_views,
            has_snapshot
        FROM spine_with_snapshot
    ),
    -- 计算每个视频每天的播放增量。
    final AS (
        SELECT
            dt,
            channel,
            video_id,
            video_key,
            published_dt,
            raw_day_end_views,
            day_end_views,
            day_end_views - LAG(day_end_views, 1, 0) OVER (
                PARTITION BY video_key
                ORDER BY dt
            ) AS daily_view_increment,
            has_snapshot
        FROM filled_daily
    )
    SELECT
        f.dt,
        f.channel,
        f.video_id,
        f.video_key,
        f.published_dt,
        f.raw_day_end_views,
        f.day_end_views,
        f.daily_view_increment,
        f.has_snapshot,
        (f.daily_view_increment > 0) AS is_active_video,
        (fps.first_positive_view_dt = f.dt) AS is_new_video,
        CASE
            WHEN fps.first_positive_view_dt = f.dt
            THEN COALESCE(f.raw_day_end_views, 0)
            ELSE 0
        END AS new_video_contributed_view_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM final f
    LEFT JOIN first_positive_snapshot fps
        ON f.video_key = fps.video_key
    WHERE f.dt IN ({dates_str});
    """
    logging.info("开始处理: dws_video_daily")
    job = client.query(query)
    job.result()
    logging.info(f"dws_video_daily 刷新完成, 处理日期: {dates}")


if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)

    dates = get_video_dates_to_process()
    if not dates:
        logging.info("没有视频数据需要处理")
        exit(0)

    logging.info(f"视频待处理日期: {dates}")
    run_dws_video_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"dws_video_daily 执行完成, 耗时: {elapsed:.1f} 秒")
