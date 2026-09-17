"""ODS ECN 账号联盟归属快照 ETL。

GET /api/v1/open-api/account-affiliation
404 / 409 落空标签，不当失败。Key 从环境变量 ACCOUNT_CHANNEL_API_KEY 读。
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
TABLE_NAME = "ods_ecn_account_affiliation"
CREATOR_SNAPSHOT_TABLE_NAME = "dws_social_creator_snapshot_daily"
AFFILIATE_USAGE_TYPE = "tiktok-affiliate"
PLATFORM = "tiktok"
API_PATH = "/api/v1/open-api/account-affiliation"
BUSINESS_STATUS = {404, 409}
SCHEMA = [
    bigquery.SchemaField("platform", "STRING"),
    bigquery.SchemaField("account_handle", "STRING"),
    bigquery.SchemaField("account_identifier", "STRING"),
    bigquery.SchemaField("affiliation", "STRING"),
    bigquery.SchemaField("affiliation_label", "STRING"),
    bigquery.SchemaField("ai_kol_id", "STRING"),
    bigquery.SchemaField("binding_source", "STRING"),
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


def list_affiliate_handles() -> list[str]:
    rows = client.query(
        f"""
        SELECT DISTINCT account_handle
        FROM `{table_id(CREATOR_SNAPSHOT_TABLE_NAME)}`
        WHERE channel = '{PLATFORM}'
            AND usage_type = '{AFFILIATE_USAGE_TYPE}'
            AND dt = (
                SELECT MAX(dt)
                FROM `{table_id(CREATOR_SNAPSHOT_TABLE_NAME)}`
                WHERE channel = '{PLATFORM}'
            )
            AND NULLIF(account_handle, '') IS NOT NULL
        """
    ).result()
    handles = [row.account_handle for row in rows]
    if not handles:
        raise RuntimeError("没有橱窗号账号，无法查询联盟归属")
    logging.info("待查询橱窗号: %d 个", len(handles))
    return handles


def error_code(payload) -> str | None:
    if not isinstance(payload, dict):
        return None
    detail = payload.get("detail")
    if isinstance(detail, dict):
        return detail.get("code")
    return None


def fetch_affiliation(session: requests.Session, host: str, handle: str) -> dict:
    response = session.get(
        f"{host}{API_PATH}",
        params={"platform": PLATFORM, "account_identifier": handle},
        timeout=60,
    )
    payload = response.json() if response.content else {}
    if response.status_code == 200:
        return {
            "platform": PLATFORM,
            "account_handle": handle,
            "account_identifier": payload.get("account_identifier"),
            "affiliation": payload.get("affiliation"),
            "affiliation_label": payload.get("affiliation_label"),
            "ai_kol_id": payload.get("ai_kol_id"),
            "binding_source": payload.get("binding_source"),
            "http_status": 200,
            "error_code": None,
        }
    if response.status_code in BUSINESS_STATUS:
        return {
            "platform": PLATFORM,
            "account_handle": handle,
            "account_identifier": handle,
            "affiliation": None,
            "affiliation_label": None,
            "ai_kol_id": None,
            "binding_source": None,
            "http_status": response.status_code,
            "error_code": error_code(payload),
        }
    raise RuntimeError(
        f"ECN account-affiliation 失败: handle={handle} status={response.status_code}"
    )


def validate(staging_table: str) -> None:
    result = list(
        client.query(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT CONCAT(platform, '|', account_handle)) AS unique_key_count,
                COUNTIF(platform IS NULL OR account_handle IS NULL) AS null_key_count
            FROM `{staging_table}`
            """
        ).result()
    )[0]
    if result.row_count == 0:
        raise RuntimeError("ECN 账号联盟归属校验失败：staging 无数据")
    if result.row_count != result.unique_key_count:
        raise RuntimeError("ECN 账号联盟归属校验失败：platform + account_handle 不唯一")
    if result.null_key_count:
        raise RuntimeError("ECN 账号联盟归属校验失败：主键字段存在空值")


def replace_table(rows: list[dict]) -> None:
    target_table = table_id()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    staging_table = f"{target_table}_tmp_{run_id}"
    target_dropped = False

    if not target_table_exists():
        table = bigquery.Table(target_table, schema=SCHEMA)
        table.clustering_fields = ["platform", "account_handle"]
        client.create_table(table)

    try:
        client.query(
            f"CREATE TABLE `{staging_table}` AS SELECT * FROM `{target_table}` WHERE 1 = 0"
        ).result()
        client.load_table_from_json(
            rows,
            staging_table,
            job_id_prefix=f"ecn_aff_{run_id}_",
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
    for handle in list_affiliate_handles():
        row = fetch_affiliation(session, host, handle)
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
