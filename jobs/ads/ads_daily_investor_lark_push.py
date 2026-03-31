from __future__ import annotations

import argparse
from datetime import date, datetime
import json
import logging
import os
from pathlib import Path
from decimal import Decimal

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
DASHBOARD_URL = "https://bi.alvinclub.ca/superset/dashboard/101/"
METRIC_DOC_URL = "https://echooo-labs.sg.larksuite.com/wiki/S79pwd96dirBuLkA7DClb4ZzgHe"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
ETL_CONFIG_PATH = PROJECT_ROOT / "env" / "etl_config.json"
LARK_WEBHOOK_CONFIG_PATH = PROJECT_ROOT / "env" / "lark_webhook.json"

client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="推送投资人报表（日表）- 明细表的 Lark 机器人消息"
    )
    parser.add_argument(
        "--date",
        dest="target_date",
        help="指定推送 dt，格式 YYYY-MM-DD；默认取 America/Toronto 昨天",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只打印将要发送的消息，不实际调用 webhook",
    )
    return parser.parse_args()


def must_non_empty_str(value, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"{field} 必须是非空字符串")
    return value.strip()


def load_json(path: Path) -> dict:
    if not path.exists():
        raise RuntimeError(f"配置文件不存在: {path}")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"配置文件 JSON 解析失败: {path} ({exc})") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"配置文件必须是 JSON 对象: {path}")
    return data


def get_runtime_mode() -> str:
    override = os.environ.get("PATH_MODE_OVERRIDE")
    if isinstance(override, str) and override.strip():
        mode = override.strip().lower()
    else:
        config = load_json(ETL_CONFIG_PATH)
        mode = config.get("path_mode")

    if mode not in {"local", "remote"}:
        raise RuntimeError("运行模式必须是 local 或 remote")
    return mode


def load_lark_webhook() -> str:
    mode = get_runtime_mode()
    data = load_json(LARK_WEBHOOK_CONFIG_PATH)
    return must_non_empty_str(data.get(mode), f"lark_webhook.{mode}")


def get_target_date(date_text: str | None) -> date:
    if date_text:
        try:
            return date.fromisoformat(date_text)
        except ValueError as exc:
            raise RuntimeError("--date 必须是 YYYY-MM-DD 格式") from exc

    query = f"SELECT DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY) AS dt"
    result = list(client.query(query).result())
    if not result:
        raise RuntimeError("无法计算默认推送日期")
    return result[0].dt


def fetch_investor_row(target_date: date) -> dict:
    query = f"""
    SELECT
        dt,
        new_video_count,
        active_video_count,
        new_video_view_count,
        avg_video_view_count,
        new_download_count,
        play_download_conversion_rate,
        avg_video_download_conversion_count,
        new_device_count,
        active_device_count,
        active_registered_user_count,
        avg_duration_sec,
        avg_content_exposure_count,
        avg_content_click_count,
        content_ctr,
        next_day_retention_rate,
        day_7_retention_rate,
        update_time
    FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_investor`
    WHERE dt = @target_date
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("target_date", "DATE", target_date)
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        raise RuntimeError(f"ads_daily_investor 未找到 dt={target_date} 的数据")
    return dict(rows[0].items())


def format_int(value) -> str:
    if value is None:
        return "--"
    return f"{int(value):,}"


def format_decimal(value, scale: int = 2) -> str:
    if value is None:
        return "--"
    if isinstance(value, Decimal):
        value = float(value)
    return f"{float(value):,.{scale}f}"


def format_percent(value, scale: int = 2) -> str:
    if value is None:
        return "--"
    if isinstance(value, Decimal):
        value = float(value)
    return f"{float(value) * 100:,.{scale}f}%"


def format_duration_seconds(value) -> str:
    if value is None:
        return "--"
    if isinstance(value, Decimal):
        value = float(value)
    return f"{float(value):,.2f} 秒"


def build_card_payload(row: dict) -> dict:
    dt_text = row["dt"].isoformat()

    metrics_overview = "\n".join(
        [
            f"**新增视频**：{format_int(row['new_video_count'])}",
            f"**活跃视频数**：{format_int(row['active_video_count'])}",
            f"**新增视频播放数**：{format_int(row['new_video_view_count'])}",
            f"**视频平均播放数**：{format_decimal(row['avg_video_view_count'])}",
        ]
    )
    metrics_conversion = "\n".join(
        [
            f"**新增下载**：{format_int(row['new_download_count'])}",
            f"**播放下载转化率**：{format_percent(row['play_download_conversion_rate'])}",
            (
                "**视频平均下载转化数**："
                f"{format_decimal(row['avg_video_download_conversion_count'], 4)}"
            ),
        ]
    )
    metrics_user = "\n".join(
        [
            f"**新增设备数**：{format_int(row['new_device_count'])}",
            f"**活跃设备数**：{format_int(row['active_device_count'])}",
            (
                "**活跃注册用户数**："
                f"{format_int(row['active_registered_user_count'])}"
            ),
            f"**人均停留时长**：{format_duration_seconds(row['avg_duration_sec'])}",
        ]
    )
    metrics_content = "\n".join(
        [
            (
                "**人均内容展示量**："
                f"{format_decimal(row['avg_content_exposure_count'], 4)}"
            ),
            (
                "**人均内容点击量**："
                f"{format_decimal(row['avg_content_click_count'], 4)}"
            ),
            f"**内容点击率**：{format_percent(row['content_ctr'])}",
        ]
    )

    update_time = row.get("update_time")
    if isinstance(update_time, datetime):
        update_time_text = update_time.strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        update_time_text = "--"

    return {
        "msg_type": "interactive",
        "card": {
            "config": {
                "wide_screen_mode": True,
                "enable_forward": True,
            },
            "header": {
                "template": "blue",
                "title": {
                    "tag": "plain_text",
                    "content": f"核心业务报表 - 明细表 | {dt_text}",
                },
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            f"**数据日期**：`{dt_text}`\n"
                            f"**日期口径**：America/Toronto 昨日\n"
                            f"**更新时间**：{update_time_text}"
                        ),
                    },
                },
                {"tag": "hr"},
                {"tag": "div", "text": {"tag": "lark_md", "content": metrics_overview}},
                {"tag": "hr"},
                {
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": metrics_conversion},
                },
                {"tag": "hr"},
                {"tag": "div", "text": {"tag": "lark_md", "content": metrics_user}},
                {"tag": "hr"},
                {"tag": "div", "text": {"tag": "lark_md", "content": metrics_content}},
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {
                                "tag": "plain_text",
                                "content": "打开 Dashboard",
                            },
                            "type": "primary",
                            "url": DASHBOARD_URL,
                        },
                        {
                            "tag": "button",
                            "text": {
                                "tag": "plain_text",
                                "content": "查看数据口径",
                            },
                            "type": "primary",
                            "url": METRIC_DOC_URL,
                        }
                    ],
                },
            ],
        },
    }


def send_lark_message(webhook_url: str, payload: dict) -> None:
    response = requests.post(webhook_url, json=payload, timeout=30)
    response.raise_for_status()

    try:
        body = response.json()
    except ValueError as exc:
        raise RuntimeError("Lark webhook 返回了非 JSON 响应") from exc

    code = body.get("code")
    if code not in (None, 0):
        raise RuntimeError(f"Lark webhook 发送失败: {body}")


def main() -> int:
    args = parse_args()
    target_date = get_target_date(args.target_date)
    logger.info(f"准备推送投资人报表明细，dt={target_date}")

    row = fetch_investor_row(target_date)
    payload = build_card_payload(row)

    if args.dry_run:
        logger.info("dry-run 模式，不实际发送 webhook")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    webhook_url = load_lark_webhook()
    send_lark_message(webhook_url, payload)
    logger.info(f"Lark 推送完成，dt={target_date}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
