import csv
from datetime import datetime, timedelta, timezone
import io
import json
import logging
from pathlib import Path
import time
from zoneinfo import ZoneInfo

from google.cloud import bigquery
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
APPSFLYER_REBUILD_DAYS = 30
PROJECT_ROOT = Path(__file__).resolve().parents[3]
APPSFLYER_CONFIG_PATH = PROJECT_ROOT / "env" / "appsflyer.json"
APPSFLYER_URL_TEMPLATE = (
    "https://hq1.appsflyer.com/api/agg-data/export/app/{app_id}/daily_report/v5"
)

client = bigquery.Client(project=PROJECT_ID)


def load_appsflyer_config():
    if not APPSFLYER_CONFIG_PATH.exists():
        raise RuntimeError(f"配置文件不存在: {APPSFLYER_CONFIG_PATH}")

    data = json.loads(APPSFLYER_CONFIG_PATH.read_text(encoding="utf-8"))

    required_keys = ("token", "android-app-id", "ios-app-id")
    for key in required_keys:
        value = data.get(key)
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"appsflyer 配置缺少有效字段: {key}")

    return {
        "token": data["token"].strip(),
        "apps": [
            {"platform": "iOS", "app_id": data["ios-app-id"].strip()},
            {"platform": "Android", "app_id": data["android-app-id"].strip()},
        ],
    }


def get_date_window(rebuild_days=APPSFLYER_REBUILD_DAYS):
    end_date = datetime.now(ZoneInfo(TORONTO_TZ)).date()
    start_date = end_date - timedelta(days=rebuild_days - 1)
    return start_date, end_date


def get_case_insensitive_value(row, *candidates):
    normalized = {key.strip().lower(): key for key in row.keys()}
    for candidate in candidates:
        matched_key = normalized.get(candidate.strip().lower())
        if matched_key:
            value = row.get(matched_key)
            return value.strip() if isinstance(value, str) else value
    return None


def get_contains_value(row, *candidates):
    normalized = {key.strip().lower(): key for key in row.keys()}
    for candidate in candidates:
        candidate_lower = candidate.strip().lower()
        for normalized_key, original_key in normalized.items():
            if candidate_lower in normalized_key:
                value = row.get(original_key)
                return value.strip() if isinstance(value, str) else value
    return None


def parse_installs(value):
    text = str(value or "0").strip().replace(",", "")
    if not text:
        return 0
    return int(float(text))


def fetch_daily_report_rows(token, platform, app_id, from_date, to_date):
    url = APPSFLYER_URL_TEMPLATE.format(app_id=app_id)
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "timezone": TORONTO_TZ,
    }

    logger.info(
        f"开始拉取 AppsFlyer 日报: platform={platform}, app_id={app_id}, "
        f"from={params['from']}, to={params['to']}"
    )
    response = requests.get(url, headers=headers, params=params, timeout=120)
    response.raise_for_status()

    csv_text = response.text.lstrip("\ufeff").strip()
    if not csv_text:
        logger.warning(f"AppsFlyer 返回空内容: platform={platform}, app_id={app_id}")
        return []

    reader = csv.DictReader(io.StringIO(csv_text))
    if not reader.fieldnames:
        logger.warning(f"AppsFlyer 未返回表头: platform={platform}, app_id={app_id}")
        return []

    logger.info(
        f"AppsFlyer CSV 字段: platform={platform}, app_id={app_id}, "
        f"fieldnames={reader.fieldnames}"
    )

    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for row in reader:
        date_value = get_case_insensitive_value(row, "date")
        installs_value = get_case_insensitive_value(row, "installs")

        if not date_value or installs_value is None:
            raise RuntimeError(
                f"AppsFlyer 返回缺少关键列，当前列名: {list(row.keys())}"
            )

        rows.append(
            {
                "dt": date_value,
                "platform": platform,
                "app_id": app_id,
                "media_source": get_contains_value(
                    row,
                    "media source",
                    "media_source",
                    "media source (pid)",
                ),
                "campaign": get_contains_value(
                    row,
                    "campaign",
                    "campaign name",
                    "campaign (c)",
                ),
                "installs": parse_installs(installs_value),
                "raw_row_json": json.dumps(row, ensure_ascii=False, sort_keys=True),
                "report_timezone": TORONTO_TZ,
                "fetched_at": fetched_at,
                "update_time": fetched_at,
            }
        )

    logger.info(
        f"AppsFlyer 拉取完成: platform={platform}, app_id={app_id}, rows={len(rows)}"
    )
    return rows


def create_temp_table(target_table_id, temp_table_id):
    query = f"""
    CREATE TABLE `{temp_table_id}` AS
    SELECT *
    FROM `{target_table_id}`
    WHERE 1 = 0
    """
    client.query(query).result()
    logger.info(f"已创建临时表: {temp_table_id}")


def drop_table_if_exists(table_id):
    client.query(f"DROP TABLE IF EXISTS `{table_id}`").result()
    logger.info(f"已删除表: {table_id}")


def build_changed_table(
    target_table_id, staging_table_id, changed_table_id, min_date, max_date
):
    query = f"""
    CREATE TABLE `{changed_table_id}`
    PARTITION BY dt
    CLUSTER BY platform, app_id, media_source AS
    SELECT *
    FROM `{target_table_id}`
    WHERE dt < DATE('{min_date}') OR dt > DATE('{max_date}')

    UNION ALL

    SELECT *
    FROM `{staging_table_id}`
    """
    client.query(query).result()
    logger.info(f"已构建变更表: {changed_table_id}")


def drop_and_rename_table(source_table_id, target_table_id):
    target_table_name = target_table_id.split(".")[-1]
    drop_table_if_exists(target_table_id)
    client.query(
        f"ALTER TABLE `{source_table_id}` RENAME TO {target_table_name}"
    ).result()
    logger.info(f"已将 {source_table_id} 重命名为 {target_table_id}")


def upsert_rows(rows, min_date, max_date):
    if not rows:
        logger.warning("本次 AppsFlyer 无可写入记录，跳过表替换")
        return

    table_id = f"{PROJECT_ID}.{DATASET_ID}.ods_appsflyer_download"
    run_id = f"{min_date.strftime('%Y%m%d')}_{int(time.time())}"
    staging_table_id = f"{table_id}_tmp_{run_id}"
    changed_table_id = f"{table_id}_changed_{run_id}"
    swapped = False

    try:
        create_temp_table(table_id, staging_table_id)

        errors = client.insert_rows_json(staging_table_id, rows)
        if errors:
            raise RuntimeError(f"写入 AppsFlyer 临时表失败: {errors}")

        build_changed_table(
            table_id,
            staging_table_id,
            changed_table_id,
            min_date.isoformat(),
            max_date.isoformat(),
        )
        drop_and_rename_table(changed_table_id, table_id)
        swapped = True
        logger.info(
            f"AppsFlyer ODS 替换完成: {min_date.isoformat()} ~ {max_date.isoformat()}"
        )
    finally:
        drop_table_if_exists(staging_table_id)
        if not swapped:
            logger.warning(f"保留变更表用于排查: {changed_table_id}")


def main():
    config = load_appsflyer_config()
    start_date, end_date = get_date_window()

    all_rows = []
    for app in config["apps"]:
        all_rows.extend(
            fetch_daily_report_rows(
                token=config["token"],
                platform=app["platform"],
                app_id=app["app_id"],
                from_date=start_date,
                to_date=end_date,
            )
        )

    upsert_rows(all_rows, start_date, end_date)


if __name__ == "__main__":
    main()
