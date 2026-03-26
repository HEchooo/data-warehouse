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
client = bigquery.Client(project=PROJECT_ID)


def dates_to_sql_list(dates):
    return ", ".join([f"'{d}'" for d in dates])


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
    - 各渠道 views / view_count 为累计值，日增量按日末累计值与前一日做差
    - 次日无采集记录时，沿用前一日日末累计值，次日增量记为 0
    """
    if not dates:
        logging.info("dws_video_daily 无待处理日期，跳过")
        return

    dates_str = dates_to_sql_list(dates)
    min_date = min(dates)
    max_date = max(dates)

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
        update_time
    )
    WITH
    params AS (
        SELECT
            DATE('{min_date}') AS min_dt,
            DATE('{max_date}') AS max_dt,
            DATE_SUB(DATE('{min_date}'), INTERVAL 1 DAY) AS seed_dt
    ),
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
    video_snapshot AS (
        SELECT
            channel,
            video_id,
            video_key,
            collected_ts_utc,
            DATE(collected_ts_utc, '{TORONTO_TZ}') AS dt,
            cum_views
        FROM (
            SELECT
                'youtube' AS channel,
                CAST(video_id AS STRING) AS video_id,
                CONCAT('youtube:', CAST(video_id AS STRING)) AS video_key,
                TIMESTAMP(collected_at, 'UTC') AS collected_ts_utc,
                SAFE_CAST(views AS INT64) AS cum_views
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.yt_video_analytics_log`

            UNION ALL

            SELECT
                'tiktok' AS channel,
                CAST(video_id AS STRING) AS video_id,
                CONCAT('tiktok:', CAST(video_id AS STRING)) AS video_key,
                TIMESTAMP(collected_at, 'UTC') AS collected_ts_utc,
                SAFE_CAST(view_count AS INT64) AS cum_views
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.video_stats_log`

            UNION ALL

            SELECT
                'ins' AS channel,
                CAST(media_id AS STRING) AS video_id,
                CONCAT('ins:', CAST(media_id AS STRING)) AS video_key,
                TIMESTAMP(collected_at, 'UTC') AS collected_ts_utc,
                SAFE_CAST(view_count AS INT64) AS cum_views
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.ig_media_stats_log`
        )
        WHERE collected_ts_utc IS NOT NULL
            AND cum_views IS NOT NULL
            AND DATE(collected_ts_utc, '{TORONTO_TZ}') <= (SELECT max_dt FROM params)
    ),
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
                    ORDER BY collected_ts_utc DESC
                ) AS rn
            FROM video_snapshot
        )
        WHERE rn = 1
    ),
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
        dt,
        channel,
        video_id,
        video_key,
        published_dt,
        raw_day_end_views,
        day_end_views,
        daily_view_increment,
        has_snapshot,
        (daily_view_increment > 0) AS is_active_video,
        (published_dt = dt AND daily_view_increment > 0) AS is_new_video,
        CURRENT_TIMESTAMP() AS update_time
    FROM final
    WHERE dt IN ({dates_str});
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
