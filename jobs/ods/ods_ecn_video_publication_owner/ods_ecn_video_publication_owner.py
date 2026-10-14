"""ODS ECN 外部视频归属 ETL。

GET /api/v1/open-api/videos/publication-owner
只补还没有 task_id 的视频；已有成功映射不再请求。404 / 409 不当失败，下次再查。
凭据读 env/ecn.json，不入库。
"""

import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import requests

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ods_ecn_video_publication_owner"
CREATOR_SNAPSHOT_TABLE_NAME = "dws_social_creator_snapshot_daily"
DIM_VIDEO_TABLE_NAME = "dim_video"
AFFILIATE_USAGE_TYPE = "tiktok-affiliate"
PLATFORM = "tiktok"
API_PATH = "/api/v1/open-api/videos/publication-owner"
BUSINESS_STATUS = {404, 409}
RETRY_STATUS = {502, 503, 504}
BATCH_SIZE = 50
CONCURRENCY = 4
CIRCUIT_FAILURE_LIMIT = 3
GATEWAY_COOLDOWN_SECONDS = 45
MAX_GATEWAY_RECOVERIES = 1
_thread_local = threading.local()
SCHEMA = [
    bigquery.SchemaField("platform", "STRING"),
    bigquery.SchemaField("video_id", "STRING"),
    bigquery.SchemaField("task_id", "STRING"),
    bigquery.SchemaField("platform_account_name", "STRING"),
    bigquery.SchemaField("http_status", "INTEGER"),
    bigquery.SchemaField("error_code", "STRING"),
    bigquery.SchemaField("fetched_at", "TIMESTAMP"),
    bigquery.SchemaField("update_time", "TIMESTAMP"),
]

client = bigquery.Client(project=PROJECT_ID)


class GatewayCircuit:
    def __init__(self, limit: int = CIRCUIT_FAILURE_LIMIT) -> None:
        self.limit = limit
        self._n = 0
        self._lock = threading.Lock()
        self.open = False
        self.open_status = None

    def note(self, status: int | None) -> None:
        with self._lock:
            if status is None or status in RETRY_STATUS:
                self._n += 1
                if self._n >= self.limit:
                    self.open = True
                    self.open_status = status if status is not None else "连接失败"
            else:
                self._n = 0

    def raise_if_open(self) -> None:
        if self.open:
            raise RuntimeError(
                f"ECN 网关连续 {self.limit} 次 {self.open_status}，整批停止"
            )


def thread_session(api_key: str) -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        _thread_local.session = session
    session.headers["X-API-Key"] = api_key
    return session


def init_thread_session(api_key: str, sessions: ExitStack, lock: threading.Lock) -> None:
    # ExitStack 由主线程在工作线程全部退出后关闭，注册回调需要串行化。
    session = thread_session(api_key)
    with lock:
        sessions.callback(session.close)


def table_id(table_name: str = TABLE_NAME) -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{table_name}"


def target_table_exists() -> bool:
    try:
        client.get_table(table_id())
        return True
    except NotFound:
        return False


def ensure_table() -> None:
    if target_table_exists():
        return
    table = bigquery.Table(table_id(), schema=SCHEMA)
    table.clustering_fields = ["platform", "video_id"]
    client.create_table(table)
    logging.info("已创建空表 %s", TABLE_NAME)


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


def list_missing_video_ids() -> list[str]:
    join_sql = ""
    if target_table_exists():
        join_sql = f"""
        LEFT JOIN `{table_id()}` o
            ON o.platform = '{PLATFORM}'
            AND o.video_id = v.video_id
            AND NULLIF(o.task_id, '') IS NOT NULL
        """
        where_missing = "AND o.video_id IS NULL"
    else:
        where_missing = ""
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
        {join_sql}
        WHERE v.channel = '{PLATFORM}'
            AND NULLIF(v.video_id, '') IS NOT NULL
            {where_missing}
        """
    ).result()
    video_ids = [row.video_id for row in rows]
    logging.info("待查询无 task_id 的橱窗号视频: %d 个", len(video_ids))
    return video_ids


def parse_json(response: requests.Response) -> dict:
    try:
        payload = response.json() if response.content else {}
    except ValueError:
        return {}
    return payload if isinstance(payload, dict) else {}


def get_with_retry(
    session: requests.Session,
    url: str,
    params: dict,
    circuit: GatewayCircuit,
) -> requests.Response:
    for attempt in range(2):
        circuit.raise_if_open()
        try:
            response = session.get(url, params=params, timeout=(5, 60))
        except (requests.ConnectionError, requests.Timeout):
            circuit.note(None)
            circuit.raise_if_open()
            if attempt:
                raise
            logging.warning("ECN 连接失败，2s 后重试 1 次")
        else:
            circuit.note(response.status_code)
            if response.status_code not in RETRY_STATUS:
                return response
            try:
                circuit.raise_if_open()
            except RuntimeError:
                response.close()
                raise
            if attempt:
                return response
            logging.warning("ECN 返回 %s，2s 后重试 1 次", response.status_code)
            response.close()
        time.sleep(2)
    raise AssertionError("ECN 重试循环未返回结果")


def fetch_publication_owner(
    host: str,
    api_key: str,
    video_id: str,
    circuit: GatewayCircuit,
) -> dict | None:
    response = get_with_retry(
        thread_session(api_key),
        f"{host}{API_PATH}",
        {"platform": PLATFORM, "video_id": video_id},
        circuit,
    )
    payload = parse_json(response)
    if response.status_code == 200:
        task_id = payload.get("task_id")
        if not task_id:
            raise RuntimeError(f"ECN publication-owner 200 但没有 task_id: video_id={video_id}")
        fetched_at = datetime.now(timezone.utc).isoformat()
        return {
            "platform": PLATFORM,
            "video_id": video_id,
            "task_id": task_id,
            "platform_account_name": payload.get("platform_account_name"),
            "http_status": 200,
            "error_code": None,
            "fetched_at": fetched_at,
            "update_time": fetched_at,
        }
    if response.status_code in BUSINESS_STATUS:
        return None
    raise RuntimeError(
        f"ECN publication-owner 失败: video_id={video_id} status={response.status_code}"
    )


def validate(table_name: str, label: str) -> None:
    result = list(
        client.query(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT CONCAT(platform, '|', video_id)) AS unique_key_count,
                COUNTIF(platform IS NULL OR video_id IS NULL) AS null_key_count,
                COUNTIF(task_id IS NULL OR task_id = '') AS missing_task_count
            FROM `{table_name}`
            """
        ).result()
    )[0]
    if result.row_count != result.unique_key_count:
        raise RuntimeError(f"ECN 外部视频归属校验失败：{label} platform + video_id 不唯一")
    if result.null_key_count:
        raise RuntimeError(f"ECN 外部视频归属校验失败：{label} 主键字段存在空值")
    if result.missing_task_count:
        raise RuntimeError(f"ECN 外部视频归属校验失败：{label} 缺少 task_id")


def upsert_rows(rows: list[dict]) -> None:
    if not rows:
        return
    target_table = table_id()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
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
            job_id_prefix=f"ecn_owner_{run_id}_",
            job_config=bigquery.LoadJobConfig(
                schema=client.get_table(target_table).schema,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ),
        ).result()
        validate(staging_table, "staging")
        client.query(
            f"""
            CREATE TABLE `{changed_table}`
            CLUSTER BY platform, video_id AS
            SELECT t.*
            FROM `{target_table}` t
            LEFT JOIN `{staging_table}` s
                ON t.platform = s.platform
                AND t.video_id = s.video_id
            WHERE s.video_id IS NULL
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
    logging.info("%s 写入 %d 条 task_id", TABLE_NAME, len(rows))


def run() -> None:
    host, api_key = load_ecn_config()
    ensure_table()
    video_ids = list_missing_video_ids()
    if not video_ids:
        logging.info("%s 无需补充", TABLE_NAME)
        return

    circuit = GatewayCircuit()
    failed: list[str] = []
    matched = 0
    recoveries = 0
    with ExitStack() as sessions, ThreadPoolExecutor(
        max_workers=CONCURRENCY,
        initializer=init_thread_session,
        initargs=(api_key, sessions, threading.Lock()),
    ) as executor:
        for start in range(0, len(video_ids), BATCH_SIZE):
            if circuit.open:
                break
            chunk = video_ids[start : start + BATCH_SIZE]
            pending = chunk
            while pending:
                futures = {
                    executor.submit(
                        fetch_publication_owner, host, api_key, video_id, circuit
                    ): video_id
                    for video_id in pending
                }
                batch: list[dict] = []
                attempt_failed: list[str] = []
                # 收齐所有在途结果后才能换熔断器；成功或业务性空结果不重复请求。
                for future in as_completed(futures):
                    video_id = futures[future]
                    try:
                        row = future.result()
                    except Exception as exc:
                        logging.warning("查询失败 video_id=%s err=%s", video_id, exc)
                        attempt_failed.append(video_id)
                        continue
                    if row:
                        batch.append(row)
                        matched += 1
                upsert_rows(batch)
                if circuit.open and recoveries < MAX_GATEWAY_RECOVERIES:
                    recoveries += 1
                    logging.warning(
                        "ECN 网关熔断，等待 %ss 后重试本批失败项 %d 个（恢复 %d/%d）",
                        GATEWAY_COOLDOWN_SECONDS, len(attempt_failed),
                        recoveries, MAX_GATEWAY_RECOVERIES,
                    )
                    time.sleep(GATEWAY_COOLDOWN_SECONDS)
                    circuit = GatewayCircuit()
                    pending = attempt_failed
                else:
                    failed.extend(attempt_failed)
                    break
            logging.info(
                "已查询 %d/%d，命中 task_id %d，并发 %d",
                min(start + len(chunk), len(video_ids)),
                len(video_ids),
                matched,
                CONCURRENCY,
            )

    if circuit.open:
        raise RuntimeError(
            f"ECN publication-owner 网关熔断，已写入成功行 {matched}，失败 {len(failed)}，下次只补缺"
        )
    if failed:
        raise RuntimeError(f"ECN publication-owner 有 {len(failed)} 条失败，已写入成功行，下次只补缺")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    started_at = datetime.now(timezone.utc)
    run()
    logging.info(
        "%s 完成，耗时: %.1f 秒",
        TABLE_NAME,
        (datetime.now(timezone.utc) - started_at).total_seconds(),
    )
