import argparse
from datetime import datetime, timezone
import logging

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from zoneinfo import ZoneInfo


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
VIDEO_SOURCE_DATASET_ID = "videos"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_social_creator_snapshot_daily"

client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "构建社媒账号日快照表（dt × channel × creator_id）；"
            "服务端账号表只有当前状态，历史不可回溯，只重写当天分区"
        )
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


def build_select_sql() -> str:
    return f"""
    -- 服务端 TikTok 账号表只保留当前状态，粉丝数等指标必须逐日落快照才能算新增。
    SELECT
        CURRENT_DATE('{TORONTO_TZ}') AS dt,
        'tiktok' AS channel,
        CAST(id AS STRING) AS creator_id,
        username AS account_handle,
        display_name AS account_name,
        profile_deep_link AS account_url,
        usage_type,
        is_active = 1 AS is_active,
        SAFE_CAST(follower_count AS INT64) AS day_end_follower_count,
        SAFE_CAST(following_count AS INT64) AS day_end_following_count,
        SAFE_CAST(likes_count AS INT64) AS day_end_like_count,
        SAFE_CAST(video_count AS INT64) AS day_end_video_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.users`
    WHERE NULLIF(CAST(id AS STRING), '') IS NOT NULL
    """


def validate(staging_table: str) -> None:
    result = list(
        client.query(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT CONCAT(CAST(dt AS STRING), '|', channel, '|', creator_id))
                    AS unique_account_count,
                COUNT(DISTINCT dt) AS dt_count,
                COUNTIF(usage_type = 'tiktok-affiliate') AS affiliate_count
            FROM `{staging_table}`
            """
        ).result()
    )[0]
    if result.row_count == 0:
        raise RuntimeError("社媒账号日快照校验失败：staging 无数据")
    if result.row_count != result.unique_account_count:
        raise RuntimeError("社媒账号日快照校验失败：统计日、渠道和账号不唯一")
    if result.dt_count != 1:
        raise RuntimeError("社媒账号日快照校验失败：单次运行只应写入一个统计日")
    if result.affiliate_count == 0:
        raise RuntimeError("社媒账号日快照校验失败：橱窗号账号数为 0")


def run() -> None:
    target_table = table_id(TABLE_NAME)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    staging_table = f"{target_table}_tmp_{run_id}"
    changed_table = f"{target_table}_changed_{run_id}"
    target_dropped = False

    try:
        client.query(
            f"""
            CREATE TABLE `{staging_table}`
            PARTITION BY dt
            CLUSTER BY channel, creator_id AS
            {build_select_sql()}
            """
        ).result()

        validate(staging_table)

        if target_table_exists():
            client.query(
                f"""
                CREATE TABLE `{changed_table}`
                PARTITION BY dt
                CLUSTER BY channel, creator_id AS
                SELECT * FROM `{target_table}`
                WHERE dt != CURRENT_DATE('{TORONTO_TZ}')

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

    logging.info("%s 刷新完成", TABLE_NAME)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    started_at = datetime.now(ZoneInfo(TORONTO_TZ))
    parse_args()
    run()
    elapsed = (datetime.now(ZoneInfo(TORONTO_TZ)) - started_at).total_seconds()
    logging.info("%s 完成，耗时: %.1f 秒", TABLE_NAME, elapsed)
