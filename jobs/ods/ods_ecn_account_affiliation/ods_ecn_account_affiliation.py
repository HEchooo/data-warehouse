"""ODS ECN 账号联盟归属 ETL。

GET /api/v1/open-api/account-affiliation
每天拉全部橱窗号：200 写入地区、ai_kol_name + 橱窗状态，404 / 409 不落新行、不改已有行。
维表地区仍只首次绑定；本表保留最近一次成功响应。凭据读 env/ecn.json，不入库。
"""

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery
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
RETRY_STATUS = {502, 503, 504}
BATCH_SIZE = 50
MAX_WORKERS = 32
HANDLE_KEY = "LOWER(LTRIM({alias}.account_handle, '@'))"
SCHEMA = [
    bigquery.SchemaField("platform", "STRING"),
    bigquery.SchemaField("account_handle", "STRING"),
    bigquery.SchemaField("account_identifier", "STRING"),
    bigquery.SchemaField("affiliation", "STRING"),
    bigquery.SchemaField("affiliation_label", "STRING"),
    bigquery.SchemaField("ai_kol_id", "STRING"),
    bigquery.SchemaField("ai_kol_name", "STRING"),
    bigquery.SchemaField("binding_source", "STRING"),
    bigquery.SchemaField("http_status", "INTEGER"),
    bigquery.SchemaField("error_code", "STRING"),
    bigquery.SchemaField("fetched_at", "TIMESTAMP"),
    bigquery.SchemaField("update_time", "TIMESTAMP"),
    bigquery.SchemaField("tiktok_showcase_status", "STRING"),
    bigquery.SchemaField("tiktok_showcase_status_label", "STRING"),
    bigquery.SchemaField("tiktok_showcase_status_reason", "STRING"),
    bigquery.SchemaField("tiktok_showcase_status_updated_at", "TIMESTAMP"),
]
STATUS_COLUMNS = (
    ("tiktok_showcase_status", "STRING"),
    ("tiktok_showcase_status_label", "STRING"),
    ("tiktok_showcase_status_reason", "STRING"),
    ("tiktok_showcase_status_updated_at", "TIMESTAMP"),
)

client = bigquery.Client(project=PROJECT_ID)


def table_id(table_name: str = TABLE_NAME) -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{table_name}"


def target_table_exists(name: str = TABLE_NAME) -> bool:
    try:
        client.get_table(table_id(name))
        return True
    except NotFound:
        return False


def ensure_table() -> None:
    if target_table_exists():
        ensure_columns()
        return
    table = bigquery.Table(table_id(), schema=SCHEMA)
    table.clustering_fields = ["platform", "account_handle"]
    client.create_table(table)
    logging.info("已创建空表 %s", TABLE_NAME)


def ensure_columns() -> None:
    for name, field_type in STATUS_COLUMNS + (("ai_kol_name", "STRING"),):
        client.query(
            f"ALTER TABLE `{table_id()}` ADD COLUMN IF NOT EXISTS {name} {field_type}"
        ).result()


def handle_key(alias: str) -> str:
    return HANDLE_KEY.format(alias=alias)


def load_ecn_config() -> tuple[str, str]:
    path = Path(__file__).resolve().parents[3] / "env" / "ecn.json"
    data = {}
    if path.is_file():
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise RuntimeError("env/ecn.json 必须是 JSON 对象")
        data = loaded
    host = (
        os.environ.get("ECN_API_HOST") or str(data.get("ECN_API_HOST") or "")
    ).strip().rstrip("/")
    key = (
        os.environ.get("ACCOUNT_CHANNEL_API_KEY")
        or str(data.get("ACCOUNT_CHANNEL_API_KEY") or "")
    ).strip()
    if not host or not key:
        raise RuntimeError("缺少 ECN_API_HOST 或 ACCOUNT_CHANNEL_API_KEY，请写入 env/ecn.json")
    if not host.startswith("http"):
        host = f"https://{host}"
    return host, key


def list_affiliate_handles() -> list[str]:
    rows = client.query(
        f"""
        SELECT DISTINCT s.account_handle
        FROM `{table_id(CREATOR_SNAPSHOT_TABLE_NAME)}` s
        WHERE s.channel = '{PLATFORM}'
            AND s.usage_type = '{AFFILIATE_USAGE_TYPE}'
            AND s.dt = (
                SELECT MAX(dt)
                FROM `{table_id(CREATOR_SNAPSHOT_TABLE_NAME)}`
                WHERE channel = '{PLATFORM}'
            )
            AND NULLIF(s.account_handle, '') IS NOT NULL
        """
    ).result()
    handles = [row.account_handle for row in rows]
    logging.info("待查询橱窗号: %d 个", len(handles))
    return handles


def parse_json(response: requests.Response) -> dict:
    try:
        payload = response.json() if response.content else {}
    except ValueError:
        return {}
    return payload if isinstance(payload, dict) else {}


def get_with_retry(session: requests.Session, url: str, params: dict) -> requests.Response:
    response = None
    for attempt in range(5):
        response = session.get(url, params=params, timeout=60)
        if response.status_code not in RETRY_STATUS:
            return response
        delay = 2 * (2 ** attempt)
        logging.warning("ECN 返回 %s，%ss 后重试 %s/5", response.status_code, delay, attempt + 1)
        time.sleep(delay)
    return response


def fetch_affiliation(session: requests.Session, host: str, handle: str) -> dict | None:
    response = get_with_retry(
        session,
        f"{host}{API_PATH}",
        {"platform": PLATFORM, "account_identifier": handle},
    )
    payload = parse_json(response)
    if response.status_code == 200:
        label = payload.get("affiliation_label")
        if not label:
            return None
        fetched_at = datetime.now(timezone.utc).isoformat()
        return {
            "platform": PLATFORM,
            "account_handle": handle,
            "account_identifier": payload.get("account_identifier"),
            "affiliation": payload.get("affiliation"),
            "affiliation_label": label,
            "ai_kol_id": payload.get("ai_kol_id"),
            "ai_kol_name": payload.get("ai_kol_name"),
            "binding_source": payload.get("binding_source"),
            "http_status": 200,
            "error_code": None,
            "fetched_at": fetched_at,
            "update_time": fetched_at,
            "tiktok_showcase_status": payload.get("tiktok_showcase_status"),
            "tiktok_showcase_status_label": payload.get("tiktok_showcase_status_label"),
            "tiktok_showcase_status_reason": payload.get("tiktok_showcase_status_reason"),
            "tiktok_showcase_status_updated_at": payload.get(
                "tiktok_showcase_status_updated_at"
            ),
        }
    if response.status_code in BUSINESS_STATUS:
        return None
    raise RuntimeError(
        f"ECN account-affiliation 失败: handle={handle} status={response.status_code}"
    )


def fetch_one(host: str, api_key: str, handle: str) -> dict | None:
    with requests.Session() as session:
        session.headers["X-API-Key"] = api_key
        return fetch_affiliation(session, host, handle)


def validate(table_name: str, label: str, require_labels: bool = False) -> None:
    result = list(
        client.query(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT CONCAT(platform, '|', {handle_key("t")})) AS unique_key_count,
                COUNTIF(platform IS NULL OR account_handle IS NULL) AS null_key_count,
                COUNTIF(http_status = 200 AND (affiliation_label IS NULL OR affiliation_label = '')) AS missing_label_count,
                COUNTIF(http_status = 200 AND NULLIF(TRIM(ai_kol_name), '') IS NULL) AS missing_ai_kol_name_count
            FROM `{table_name}` t
            """
        ).result()
    )[0]
    if result.row_count != result.unique_key_count:
        raise RuntimeError(f"ECN 账号联盟归属校验失败：{label} platform + account_handle 不唯一")
    if result.null_key_count:
        raise RuntimeError(f"ECN 账号联盟归属校验失败：{label} 主键字段存在空值")
    if require_labels and result.missing_label_count:
        raise RuntimeError(f"ECN 账号联盟归属校验失败：{label} 200 响应缺少 affiliation_label")
    if require_labels and result.missing_ai_kol_name_count:
        logging.warning(
            "ECN account-affiliation 当前响应缺少 ai_kol_name：%d 条；本字段允许为空，不阻断账号归属更新",
            result.missing_ai_kol_name_count,
        )


def upsert_rows(rows: list[dict]) -> None:
    if not rows:
        return
    target_table = table_id()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    staging_table = f"{target_table}_tmp_{run_id}"
    changed_table = f"{target_table}_changed_{run_id}"
    target_dropped = False
    ensure_table()

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
        validate(staging_table, "staging", require_labels=True)
        client.query(
            f"""
            CREATE TABLE `{changed_table}`
            CLUSTER BY platform, account_handle AS
            SELECT t.*
            FROM `{target_table}` t
            LEFT JOIN `{staging_table}` s
                ON t.platform = s.platform
                AND {handle_key("t")} = {handle_key("s")}
            WHERE s.account_handle IS NULL
            UNION ALL
            SELECT * FROM `{staging_table}`
            """
        ).result()
        validate(changed_table, "changed")
        client.query(f"DROP TABLE `{target_table}`").result()
        target_dropped = True
        client.query(f"ALTER TABLE `{changed_table}` RENAME TO {TABLE_NAME}").result()
    except Exception:
        if target_dropped:
            logging.error("目标表已删除但换名未完成，changed 表保留待恢复: %s", changed_table)
        else:
            client.query(f"DROP TABLE IF EXISTS `{changed_table}`").result()
        raise
    finally:
        client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
    logging.info("%s 写入 %d 条绑定", TABLE_NAME, len(rows))


def run() -> None:
    host, api_key = load_ecn_config()
    ensure_table()
    handles = list_affiliate_handles()
    if not handles:
        logging.info("%s 没有橱窗号", TABLE_NAME)
        return

    failed: list[str] = []
    matched = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for start in range(0, len(handles), BATCH_SIZE):
            futures = {
                pool.submit(fetch_one, host, api_key, handle): handle
                for handle in handles[start : start + BATCH_SIZE]
            }
            batch: list[dict] = []
            for future in as_completed(futures):
                handle = futures[future]
                try:
                    row = future.result()
                except Exception as exc:
                    logging.warning("查询失败，本轮跳过 handle=%s err=%s", handle, exc)
                    failed.append(handle)
                    continue
                if row:
                    batch.append(row)
                    matched += 1
            upsert_rows(batch)
            logging.info("已查询 %d/%d，命中地区 %d", min(start + BATCH_SIZE, len(handles)), len(handles), matched)

    if failed:
        raise RuntimeError(f"ECN account-affiliation 有 {len(failed)} 条失败，已写入成功行，下次只补缺")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    started_at = datetime.now(timezone.utc)
    run()
    logging.info(
        "%s 完成，耗时: %.1f 秒",
        TABLE_NAME,
        (datetime.now(timezone.utc) - started_at).total_seconds(),
    )
