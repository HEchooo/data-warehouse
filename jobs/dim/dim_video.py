from datetime import datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
VIDEO_SOURCE_DATASET_ID = "videos"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dim_video"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dim" / TABLE_NAME
YOUTUBE_AI_TEAM_TEST_CHANNEL_IDS = [
    "UCynAg7WHdiy8SuQpESO__yw",
    "UCgBJozWjiE0cqwv7UNalyog",
]
YOUTUBE_AI_TEAM_INTERNAL_CHANNEL_IDS = [
    "UC4rj_IlO3V-d5ebVhgagh1w",
    "UC70Y2-yeuFOoTx6yNkqioKA",
    "UCiX5DzRDNIb9fH_MQvoIN7w",
    "UCLs3oc5eyptqVTEnXaIwHvQ",
    "UCPAQ09Nusb_OZzwfWGAE_Aw",
    "UCV5FBW_l8cayS8L_zAIvlkQ",
    "UCVsLt_8hbY5nG5da8sslD_w",
    "UCzIQMkLCHa63JekjxaA133w",
]
YOUTUBE_AI_TEAM_EXCLUDED_CHANNEL_IDS = (
    YOUTUBE_AI_TEAM_TEST_CHANNEL_IDS + YOUTUBE_AI_TEAM_INTERNAL_CHANNEL_IDS
)

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


def strings_to_sql_list(values: list[str]) -> str:
    return ", ".join([f"'{value}'" for value in values])


def build_select_sql() -> str:
    excluded_channel_ids_sql = strings_to_sql_list(YOUTUBE_AI_TEAM_EXCLUDED_CHANNEL_IDS)
    return f"""
    WITH
    -- 正式号视频标记统一来自服务端 tag 表，供下游复用。
    formal_video_tag AS (
        SELECT
            platform AS channel,
            CAST(platform_channel_id AS STRING) AS creator_id,
            CAST(platform_video_id AS STRING) AS video_id,
            CONCAT(platform, ':', CAST(platform_video_id AS STRING)) AS video_key,
            MIN(TIMESTAMP(created_at, 'UTC')) AS formal_tag_created_ts_utc
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.platform_video_tags`
        WHERE tag_type = 'env'
            AND tag_value = 'prod'
            AND platform IN ('youtube', 'tiktok', 'ins')
            AND NULLIF(CAST(platform_video_id AS STRING), '') IS NOT NULL
        GROUP BY channel, creator_id, video_id, video_key
    ),
    -- 汇总三端视频主数据；YouTube AI 团队渠道仍按 Social Content 规则排除，但正式号视频例外保留。
    raw_video_union AS (
        SELECT
            'youtube' AS channel,
            CAST(channel_id AS STRING) AS creator_id,
            CAST(video_id AS STRING) AS video_id,
            CONCAT('youtube:', CAST(video_id AS STRING)) AS video_key,
            TIMESTAMP(published_at, 'UTC') AS published_ts_utc,
            1 AS source_priority
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.yt_videos`
        WHERE channel_id NOT IN ({excluded_channel_ids_sql})
            OR CONCAT('youtube:', CAST(video_id AS STRING)) IN (
                SELECT video_key FROM formal_video_tag
            )

        UNION ALL

        SELECT
            'tiktok' AS channel,
            CAST(user_id AS STRING) AS creator_id,
            CAST(id AS STRING) AS video_id,
            CONCAT('tiktok:', CAST(id AS STRING)) AS video_key,
            TIMESTAMP_SECONDS(SAFE_CAST(create_time AS INT64)) AS published_ts_utc,
            1 AS source_priority
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.videos`

        UNION ALL

        SELECT
            'ins' AS channel,
            CAST(account_id AS STRING) AS creator_id,
            CAST(media_id AS STRING) AS video_id,
            CONCAT('ins:', CAST(media_id AS STRING)) AS video_key,
            TIMESTAMP(published_at, 'UTC') AS published_ts_utc,
            1 AS source_priority
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.ig_media`

        UNION ALL

        SELECT
            channel,
            creator_id,
            video_id,
            video_key,
            formal_tag_created_ts_utc AS published_ts_utc,
            2 AS source_priority
        FROM formal_video_tag
    ),
    -- video_key 去重，优先使用平台主视频表时间；tag-only 视频用 tag 创建时间兜底。
    deduped_video AS (
        SELECT
            channel,
            creator_id,
            video_id,
            video_key,
            published_ts_utc
        FROM raw_video_union
        WHERE published_ts_utc IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY video_key
            ORDER BY source_priority, published_ts_utc DESC
        ) = 1
    )
    SELECT
        deduped_video.channel,
        deduped_video.creator_id,
        deduped_video.video_id,
        deduped_video.video_key,
        deduped_video.published_ts_utc,
        DATE(deduped_video.published_ts_utc, '{TORONTO_TZ}') AS published_dt,
        formal_video_tag.formal_tag_created_ts_utc IS NOT NULL AS is_formal_video,
        formal_video_tag.formal_tag_created_ts_utc,
        CURRENT_TIMESTAMP() AS update_time
    FROM deduped_video
    LEFT JOIN formal_video_tag
        USING (video_key)
    """


def run_dim_video() -> None:
    staging_table_id = (
        f"{target_table_id()}_tmp_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    )
    select_sql = build_select_sql()

    logging.info("开始处理: %s", TABLE_NAME)

    try:
        client.query(
            f"""
            CREATE TABLE `{staging_table_id}`
            CLUSTER BY channel, creator_id, video_key AS
            {select_sql}
            """
        ).result()

        validation_query = f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT video_key) AS distinct_video_keys,
            COUNTIF(video_key IS NULL OR video_key = '') AS invalid_video_key_rows,
            COUNTIF(is_formal_video AND formal_tag_created_ts_utc IS NULL)
                AS invalid_formal_video_rows
        FROM `{staging_table_id}`
        """
        validation_row = list(client.query(validation_query).result())[0]
        if validation_row.total_rows != validation_row.distinct_video_keys:
            raise RuntimeError("dim_video 校验失败：video_key 未做到一键一行")
        if validation_row.invalid_video_key_rows != 0:
            raise RuntimeError("dim_video 校验失败：存在空 video_key")
        if validation_row.invalid_formal_video_rows != 0:
            raise RuntimeError("dim_video 校验失败：正式号视频缺少 tag 创建时间")

        client.query(
            f"""
            CREATE OR REPLACE TABLE `{target_table_id()}`
            CLUSTER BY channel, creator_id, video_key AS
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
    run_dim_video()
    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
