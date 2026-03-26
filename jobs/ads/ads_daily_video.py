from datetime import datetime, timedelta, timezone
import logging
from zoneinfo import ZoneInfo

from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
VIDEO_REBUILD_DAYS = 30
client = bigquery.Client(project=PROJECT_ID)


def dates_to_sql_list(dates):
    return ", ".join([f"'{d}'" for d in dates])


def get_video_dates_to_process(rebuild_days=VIDEO_REBUILD_DAYS):
    """
    与 dws_video_daily 保持一致，默认重刷最近 N 天视频日报分区。
    使用多伦多时间（America/Toronto）。
    """
    end_date = datetime.now(ZoneInfo(TORONTO_TZ)).date()
    start_date = end_date - timedelta(days=rebuild_days - 1)
    return [start_date + timedelta(days=offset) for offset in range(rebuild_days)]


def run_ads_daily_video(dates):
    """
    视频日报：
    - new_video_count: 当天新发布且有播放增量的视频数
    - active_video_count: 当天有播放增量的视频数
    - new_video_view_count: 当天新发布视频在当天产生的播放增量
    - avg_video_view_count: new_video_view_count / active_video_count
    使用多伦多时间（America/Toronto）。
    """
    if not dates:
        logging.info("ads_daily_video 无待处理日期，跳过")
        return

    dates_str = dates_to_sql_list(dates)

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_video`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ads_daily_video`
    (
        dt,
        new_video_count,
        active_video_count,
        new_video_view_count,
        avg_video_view_count,
        update_time
    )
    WITH
    metric_dates AS (
        SELECT DATE(dt_text) AS dt
        FROM UNNEST([{dates_str}]) AS dt_text
    ),
    video_metrics AS (
        SELECT
            dt,
            COUNTIF(is_new_video) AS new_video_count,
            COUNTIF(is_active_video) AS active_video_count,
            CAST(
                COALESCE(
                    SUM(IF(published_dt = dt, daily_view_increment, 0)),
                    0
                ) AS INT64
            ) AS new_video_view_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_video_daily`
        WHERE dt IN ({dates_str})
        GROUP BY dt
    )
    SELECT
        d.dt,
        COALESCE(v.new_video_count, 0) AS new_video_count,
        COALESCE(v.active_video_count, 0) AS active_video_count,
        COALESCE(v.new_video_view_count, 0) AS new_video_view_count,
        CAST(
            CASE
                WHEN COALESCE(v.active_video_count, 0) = 0 THEN 0
                ELSE ROUND(
                    SAFE_DIVIDE(
                        CAST(v.new_video_view_count AS NUMERIC),
                        v.active_video_count
                    ),
                    4
                )
            END AS NUMERIC
        ) AS avg_video_view_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM metric_dates d
    LEFT JOIN video_metrics v
        ON d.dt = v.dt;
    """
    logging.info("开始处理: ads_daily_video")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_video 刷新完成, 处理日期: {dates}")


if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)

    dates = get_video_dates_to_process()
    if not dates:
        logging.info("没有视频数据需要处理")
        exit(0)

    logging.info(f"视频 ADS 待处理日期: {dates}")
    run_ads_daily_video(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info(f"ads_daily_video 执行完成, 耗时: {elapsed:.1f} 秒")
