from datetime import datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
VIDEO_SOURCE_DATASET_ID = "videos"
TABLE_NAME = "dim_auto_publish_video_task"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dim" / TABLE_NAME

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


def target_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


def build_select_sql() -> str:
    return f"""
    WITH
    -- 收集三端自动发布成功的视频与任务映射。
    completed_task_map AS (
        SELECT DISTINCT
            CONCAT('youtube:', CAST(v.video_id AS STRING)) AS video_key,
            'youtube' AS channel,
            CAST(v.video_id AS STRING) AS video_id,
            u.task_id
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.yt_uploads` u
        INNER JOIN `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.yt_videos` v
            ON u.channel_id = v.channel_id
            AND u.youtube_video_id = CAST(v.video_id AS STRING)
        WHERE u.status = 'COMPLETED'
            AND u.task_id IS NOT NULL
            AND u.task_id != ''
            AND u.channel_id IS NOT NULL
            AND u.channel_id != ''
            AND u.youtube_video_id IS NOT NULL
            AND u.youtube_video_id != ''

        UNION DISTINCT

        SELECT DISTINCT
            CONCAT('tiktok:', CAST(v.id AS STRING)) AS video_key,
            'tiktok' AS channel,
            CAST(v.id AS STRING) AS video_id,
            u.task_id
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.video_uploads` u
        INNER JOIN `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.videos` v
            ON CAST(u.user_id AS STRING) = CAST(v.user_id AS STRING)
            AND u.tiktok_video_id = CAST(v.id AS STRING)
        WHERE u.status = 'COMPLETED'
            AND u.task_id IS NOT NULL
            AND u.task_id != ''
            AND u.user_id IS NOT NULL
            AND CAST(u.user_id AS STRING) != ''
            AND u.tiktok_video_id IS NOT NULL
            AND u.tiktok_video_id != ''

        UNION DISTINCT

        SELECT DISTINCT
            CONCAT('ins:', CAST(v.media_id AS STRING)) AS video_key,
            'ins' AS channel,
            CAST(v.media_id AS STRING) AS video_id,
            u.task_id
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.ig_uploads` u
        INNER JOIN `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.ig_media` v
            ON CAST(u.channel_id AS STRING) = CAST(v.account_id AS STRING)
            AND u.instagram_media_id = CAST(v.media_id AS STRING)
        WHERE u.status = 'COMPLETED'
            AND u.task_id IS NOT NULL
            AND u.task_id != ''
            AND u.channel_id IS NOT NULL
            AND CAST(u.channel_id AS STRING) != ''
            AND u.instagram_media_id IS NOT NULL
            AND u.instagram_media_id != ''
    ),
    -- 统一到每个视频键唯一一行；异常情况下保留原始 video_key，不做误并。
    unified_task_map AS (
        SELECT
            video_key,
            ANY_VALUE(channel) AS channel,
            ANY_VALUE(video_id) AS video_id,
            IF(COUNT(DISTINCT task_id) = 1, MAX(task_id), NULL) AS auto_publish_task_id,
            COUNT(DISTINCT task_id) AS completed_task_id_count
        FROM completed_task_map
        GROUP BY video_key
    )
    SELECT
        video_key,
        channel,
        video_id,
        auto_publish_task_id,
        COALESCE(CONCAT('auto_task:', auto_publish_task_id), video_key)
            AS unified_video_key,
        completed_task_id_count,
        completed_task_id_count > 1 AS is_mapping_ambiguous,
        CURRENT_TIMESTAMP() AS update_time
    FROM unified_task_map
    """


def run_dim_auto_publish_video_task() -> None:
    staging_table_id = f"{target_table_id()}_tmp_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    select_sql = build_select_sql()

    logging.info("开始处理: %s", TABLE_NAME)

    try:
        client.query(
            f"""
            CREATE TABLE `{staging_table_id}`
            CLUSTER BY channel, auto_publish_task_id, video_key AS
            {select_sql}
            """
        ).result()

        validation_query = f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT video_key) AS distinct_video_keys,
            COUNTIF(video_key IS NULL OR video_key = '') AS invalid_video_key_rows
        FROM `{staging_table_id}`
        """
        validation_row = list(client.query(validation_query).result())[0]
        if validation_row.total_rows != validation_row.distinct_video_keys:
            raise RuntimeError("dim_auto_publish_video_task 校验失败：video_key 未做到一键一行")
        if validation_row.invalid_video_key_rows != 0:
            raise RuntimeError("dim_auto_publish_video_task 校验失败：存在空 video_key")

        client.query(
            f"""
            CREATE OR REPLACE TABLE `{target_table_id()}`
            CLUSTER BY channel, auto_publish_task_id, video_key AS
            SELECT *
            FROM `{staging_table_id}`
            """
        ).result()
        client.delete_table(staging_table_id, not_found_ok=True)
    except Exception:
        client.delete_table(staging_table_id, not_found_ok=True)
        raise

    logging.info("%s 刷新完成", TABLE_NAME)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    run_dim_auto_publish_video_task()
    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
