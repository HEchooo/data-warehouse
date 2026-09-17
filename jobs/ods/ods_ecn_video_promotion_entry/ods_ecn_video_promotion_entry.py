"""ODS ECN 视频首次投流发送时间快照 ETL。

GET /api/v1/open-api/videos/promotion-entry
404 / 409 落空 ads_first_sent_at，不当失败。
"""

import logging
import os
import time
from datetime import datetime, timezone

from google.cloud import bigquery, secretmanager
from google.cloud.exceptions import NotFound
import requests

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ods_ecn_video_promotion_entry"
CREATOR_SNAPSHOT_TABLE_NAME = "dws_social_creator_snapshot_daily"
DIM_VIDEO_TABLE_NAME = "dim_video"
AFFILIATE_USAGE_TYPE = "tiktok-affiliate"
PLATFORM = "tiktok"
API_PATH = "/api/v1/open-api/videos/promotion-entry"
BUSINESS_STATUS = {404, 409}
SCHEMA = [
    bigquery.SchemaField("platform", "STRING"),
    bigquery.SchemaField("video_id", "STRING"),
    bigquery.SchemaField("ads_first_sent_at", "TIMESTAMP"),
    bigquery.SchemaField("http_status", "INTEGER"),
    bigquery.SchemaField("error_code", "STRING"),
    bigquery.SchemaField("fetched_at", "TIMESTAMP"),
    bigquery.SchemaField("update_time", "TIMESTAMP"),
]

client = bigquery.Client(project=PROJECT_ID)


def table_id(table_name: str = TABLE_NAME) -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{table_name}"


def target_table_exists() -> bool:
    try:
        client.get_table(table_id())
        return True
    except NotFound:
        return False


def get_ecn_host() -> str:
    host = os.environ.get("ECN_API_HOST", "").strip().rstrip("/")
    if not host:
        raise RuntimeError("缺少 ECN_API_HOST")
    if not host.startswith("http"):
        host = f"https://{host}"
    return host


def get_ecn_api_key() -> str:
    key = os.environ.get("ACCOUNT_CHANNEL_API_KEY", "").strip()
    if key:
        return key
    secret_id = os.environ.get(
        "ACCOUNT_CHANNEL_API_KEY_SECRET_ID", "prod-ecn-account-channel-api-key"
    )
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    try:
        return (
            secretmanager.SecretManagerServiceClient()
            .access_secret_version(request={"name": name})
            .payload.data.decode("utf-8")
            .strip()
        )
    except Exception as exc:
        raise RuntimeError(
            "缺少 ACCOUNT_CHANNEL_API_KEY，也读不到 Secret Manager 中的对应 secret"
        ) from exc


def list_affiliate_video_ids() -> list[str]:
    rows = client.query(
        f"""
        SELECT DISTINCT v.video_id
        FROM `{table_id(DIM_VIDEO_TABLE_NAME)}` v
        INNER JOIN (
            SELECT creator_id
            FROM `{table_id(CREATOR_SNAPSHOT_TABLE_NAME)}`
            WHERE channel = '{PLATFORM}'
                AND usage_type = '{AFFILIATE_USAGE_TYPE}'
                AND dt = (
                    SELECT MAX(dt)
                    FROM `{table_id(CREATOR_SNAPSHOT_TABLE_NAME)}`
                    WHERE channel = '{PLATFORM}'
                )
        ) a
            ON v.creator_id = a.creator_id
        WHERE v.channel = '{PLATFORM}'
            AND NULLIF(v.video_id, '') IS NOT NULL
        """
    ).result()
    video_ids = [row.video_id for row in rows]
    if not video_ids:
        raise RuntimeError("没有橱窗号视频，无法查询投流时间")
    logging.info("待查询橱窗号视频: %d 个", len(video_ids))
    return video_ids


def error_code(payload) -> str | None:
    if not isinstance(payload, dict):
        return None
    detail = payload.get("detail")
    if isinstance(detail, dict):
        return detail.get("code")
    return None


def fetch_promotion_entry(session: requests.Session, host: str, video_id: str) -> dict:
    response = session.get(
        f"{host}{API_PATH}",
        params={"platform": PLATFORM, "video_id": video_id},
        timeout=60,
    )
    payload = response.json() if response.content else {}
    if response.status_code == 200:
        return {
            "platform": PLATFORM,
            "video_id": video_id,
            "ads_first_sent_at": payload.get("ads_first_sent_at"),
            "http_status": 200,
            "error_code": None,
        }
    if response.status_code in BUSINESS_STATUS:
        return {
            "platform": PLATFORM,
            "video_id": video_id,
            "ads_first_sent_at": None,
            "http_status": response.status_code,
            "error_code": error_code(payload),
        }
    raise RuntimeError(
        f"ECN promotion-entry 失败: video_id={video_id} status={response.status_code}"
    )


def validate(staging_table: str) -> None:
    result = list(
        client.query(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT CONCAT(platform, '|', video_id)) AS unique_key_count,
                COUNTIF(platform IS NULL OR video_id IS NULL) AS null_key_count
            FROM `{staging_table}`
            """
        ).result()
    )[0]
    if result.row_count == 0:
        raise RuntimeError("ECN 视频投流时间校验失败：staging 无数据")
    if result.row_count != result.unique_key_count:
        raise RuntimeError("ECN 视频投流时间校验失败：platform + video_id 不唯一")
    if result.null_key_count:
        raise RuntimeError("ECN 视频投流时间校验失败：主键字段存在空值")


def replace_table(rows: list[dict]) -> None:
    target_table = table_id()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    staging_table = f"{target_table}_tmp_{run_id}"
    target_dropped = False

    if not target_table_exists():
        table = bigquery.Table(target_table, schema=SCHEMA)
        table.clustering_fields = ["platform", "video_id"]
        client.create_table(table)

    try:
        client.query(
            f"CREATE TABLE `{staging_table}` AS SELECT * FROM `{target_table}` WHERE 1 = 0"
        ).result()
        client.load_table_from_json(
            rows,
            staging_table,
            job_id_prefix=f"ecn_promo_{run_id}_",
            job_config=bigquery.LoadJobConfig(
                schema=client.get_table(target_table).schema,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ),
        ).result()
        validate(staging_table)
        if target_table_exists():
            client.query(f"DROP TABLE `{target_table}`").result()
            target_dropped = True
        client.query(f"ALTER TABLE `{staging_table}` RENAME TO {TABLE_NAME}").result()
    except Exception:
        if target_dropped:
            logging.error("目标表已删除但换名未完成，staging 表保留待恢复: %s", staging_table)
        else:
            client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
        raise
    logging.info("%s 刷新完成, rows=%d", TABLE_NAME, len(rows))


def run() -> None:
    host = get_ecn_host()
    session = requests.Session()
    session.headers["X-API-Key"] = get_ecn_api_key()
    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for video_id in list_affiliate_video_ids():
        row = fetch_promotion_entry(session, host, video_id)
        row["fetched_at"] = fetched_at
        row["update_time"] = fetched_at
        rows.append(row)
        time.sleep(0.05)
    replace_table(rows)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    started_at = datetime.now(timezone.utc)
    run()
    logging.info(
        "%s 完成，耗时: %.1f 秒",
        TABLE_NAME,
        (datetime.now(timezone.utc) - started_at).total_seconds(),
    )
