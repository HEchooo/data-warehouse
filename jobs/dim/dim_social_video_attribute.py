from datetime import datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
VIDEO_SOURCE_DATASET_ID = "videos"
TABLE_NAME = "dim_social_video_attribute"
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


def target_table_exists() -> bool:
    try:
        client.get_table(target_table_id())
        return True
    except NotFound:
        return False


def build_select_sql() -> str:
    return f"""
    WITH
    -- 只保留 ext_info 顶层存在 video_task 的上传任务，并一次性解析属性 JSON。
    task_attributes AS (
        SELECT
            task_id,
            JSON_VALUE(ext_info, '$.production_type') AS production_type,
            NULLIF(
                JSON_VALUE(ext_info, '$.video_task.video_brand_group.name'),
                ''
            ) AS video_brand,
            JSON_VALUE(ext_info, '$.video_task.blogger_type') AS blogger_type,
            created_at AS task_created_at,
            updated_at AS task_updated_at
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.video_upload_tasks`
        WHERE JSON_QUERY(ext_info, '$.video_task') IS NOT NULL
    ),
    -- 同一平台视频若存在重试映射，只保留最近更新的一条任务关系。
    latest_channel_task AS (
        SELECT * EXCEPT(rn)
        FROM (
            SELECT
                CASE LOWER(c.platform)
                    WHEN 'instagram' THEN 'ins'
                    ELSE LOWER(c.platform)
                END AS channel,
                CAST(c.platform_video_id AS STRING) AS video_id,
                c.task_id,
                a.production_type,
                a.video_brand,
                a.blogger_type,
                a.task_created_at,
                a.task_updated_at,
                c.updated_at AS mapping_updated_at,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        CASE LOWER(c.platform)
                            WHEN 'instagram' THEN 'ins'
                            ELSE LOWER(c.platform)
                        END,
                        CAST(c.platform_video_id AS STRING)
                    ORDER BY c.updated_at DESC, c.id DESC
                ) AS rn
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.video_upload_task_channels` c
            INNER JOIN task_attributes a USING (task_id)
            WHERE c.platform_video_id IS NOT NULL
                AND c.platform_video_id != ''
        )
        WHERE rn = 1
    )
    SELECT
        d.video_key,
        d.channel,
        CAST(d.video_id AS STRING) AS video_id,
        m.task_id,
        CONCAT('auto_task:', m.task_id) AS unified_video_key,
        m.production_type,
        CASE m.production_type
            WHEN 'normal' THEN '常规'
            WHEN 'brand' THEN 'CPM'
            WHEN 'brand_cps' THEN 'CPS'
            ELSE m.production_type
        END AS video_attribute,
        COALESCE(m.video_brand, '未设置') AS video_brand,
        m.blogger_type,
        CASE m.blogger_type
            WHEN 'normal' THEN '常规'
            WHEN 'cpm' THEN 'CPM'
            WHEN 'cps' THEN 'CPS'
            ELSE m.blogger_type
        END AS blogger_attribute,
        m.task_created_at,
        m.task_updated_at,
        m.mapping_updated_at,
        CURRENT_TIMESTAMP() AS update_time
    FROM `{PROJECT_ID}.{DATASET_ID}.dim_video` d
    INNER JOIN latest_channel_task m
        ON d.channel = m.channel
        AND CAST(d.video_id AS STRING) = m.video_id
    WHERE d.is_social_content_video
    """


def run_dim_social_video_attribute() -> None:
    if not target_table_exists():
        raise RuntimeError(f"目标表不存在，请先执行 DDL: {target_table_id()}")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    target_table = target_table_id()
    staging_table = f"{target_table}_tmp_{run_id}"
    changed_table = f"{target_table}_changed_{run_id}"
    target_dropped = False

    logging.info(
        "开始处理 %s, staging=%s, changed=%s",
        TABLE_NAME,
        staging_table,
        changed_table,
    )
    try:
        client.query(
            f"""
            CREATE TABLE `{staging_table}`
            CLUSTER BY channel, video_attribute, blogger_attribute, video_key AS
            {build_select_sql()}
            """
        ).result()

        validation_row = list(
            client.query(
                f"""
                SELECT
                    COUNT(*) AS row_count,
                    COUNT(DISTINCT video_key) AS distinct_video_key_count,
                    COUNTIF(video_key IS NULL OR video_key = '') AS invalid_video_key_count,
                    COUNTIF(task_id IS NULL OR task_id = '') AS invalid_task_id_count
                FROM `{staging_table}`
                """
            ).result()
        )[0]
        if validation_row.row_count != validation_row.distinct_video_key_count:
            raise ValueError("dim_social_video_attribute 校验失败：video_key 不是一键一行")
        if validation_row.invalid_video_key_count != 0:
            raise ValueError("dim_social_video_attribute 校验失败：存在空 video_key")
        if validation_row.invalid_task_id_count != 0:
            raise ValueError("dim_social_video_attribute 校验失败：存在空 task_id")

        client.query(
            f"""
            CREATE TABLE `{changed_table}`
            CLUSTER BY channel, video_attribute, blogger_attribute, video_key AS
            SELECT *
            FROM `{staging_table}`
            """
        ).result()
        client.query(f"DROP TABLE `{target_table}`").result()
        target_dropped = True
        client.query(
            f"ALTER TABLE `{changed_table}` RENAME TO {TABLE_NAME}"
        ).result()
        target_dropped = False
        client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
    except Exception:
        if target_dropped:
            try:
                client.query(
                    f"ALTER TABLE `{changed_table}` RENAME TO {TABLE_NAME}"
                ).result()
            except Exception:
                logging.exception("目标表恢复失败，保留替换表: %s", changed_table)
        else:
            client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
            client.query(f"DROP TABLE IF EXISTS `{changed_table}`").result()
        raise

    logging.info("%s 刷新完成", TABLE_NAME)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)
    start_time = datetime.now(timezone.utc)
    run_dim_social_video_attribute()
    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
