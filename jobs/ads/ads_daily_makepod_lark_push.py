from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from decimal import Decimal
import json
import logging
import os
from pathlib import Path

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
MAKEPOD_TZ = "Asia/Shanghai"
DASHBOARD_URL = "https://bi.alvinclub.ca/superset/dashboard/makepod-platform-dashboard/"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
ETL_CONFIG_PATH = PROJECT_ROOT / "env" / "etl_config.json"
LARK_WEBHOOK_CONFIG_PATH = PROJECT_ROOT / "env" / "lark_webhook.json"

METRIC_DEFS = [
    ("DAU", "dau_uv_total"),
    ("新用户 DAU", "dau_new_uv"),
    ("老用户 DAU", "dau_old_uv"),
    ("支付成功人数", "pay_user_count"),
    ("支付成功订单数", "pay_order_count"),
    ("下单人数", "order_user_count"),
    ("支付成功 GMV（USD）", "pay_gmv_usd"),
    ("支付成功 SKU 件数", "pay_sku_quantity"),
    ("毛利（USD）", "gross_profit_usd"),
    ("毛利率", "gross_profit_rate"),
]
USD_FIELDS = {"pay_gmv_usd", "gross_profit_usd"}
PERCENT_FIELDS = {"gross_profit_rate"}

client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="推送【Makepod】平台看板数据日报的 Lark 机器人消息"
    )
    parser.add_argument(
        "--date",
        dest="target_date",
        help="指定推送 dt，格式 YYYY-MM-DD；默认取 Asia/Shanghai 昨天",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只打印将要发送的消息，不实际调用 webhook",
    )
    parser.add_argument(
        "--webhook-url",
        help="覆盖配置中的 Lark webhook，仅用于临时测试；不要写入仓库配置",
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
    webhook_key = "remote_makepod" if mode == "remote" else mode
    return must_non_empty_str(data.get(webhook_key), f"lark_webhook.{webhook_key}")


def get_target_date(date_text: str | None) -> date:
    if date_text:
        try:
            return date.fromisoformat(date_text)
        except ValueError as exc:
            raise RuntimeError("--date 必须是 YYYY-MM-DD 格式") from exc

    query = f"SELECT DATE_SUB(CURRENT_DATE('{MAKEPOD_TZ}'), INTERVAL 1 DAY) AS dt"
    result = list(client.query(query).result())
    if not result:
        raise RuntimeError("无法计算默认推送日期")
    return result[0].dt


def fetch_makepod_rows(target_date: date) -> dict[date, dict]:
    query = f"""
    SELECT
        dt,
        dau_uv_total,
        dau_new_uv,
        dau_old_uv,
        pay_user_count,
        pay_order_count,
        order_user_count,
        pay_gmv_usd,
        pay_sku_quantity,
        gross_profit_usd,
        gross_profit_rate,
        update_time
    FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_makepod`
    WHERE channel = 'Overall'
      AND dt IN (@target_date, DATE_SUB(@target_date, INTERVAL 1 DAY))
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("target_date", "DATE", target_date)
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    return {row.dt: dict(row.items()) for row in rows}


def format_int(value) -> str:
    if value is None:
        return "--"
    return f"{int(value):,}"


def format_usd(value) -> str:
    if value is None:
        return "--"
    if isinstance(value, Decimal):
        value = float(value)
    return f"${float(value):,.2f}"


def format_percent(value, scale: int = 2) -> str:
    if value is None:
        return "--"
    if isinstance(value, Decimal):
        value = float(value)
    return f"{float(value) * 100:,.{scale}f}%"


def format_metric_value(field: str, value) -> str:
    if field in USD_FIELDS:
        return format_usd(value)
    if field in PERCENT_FIELDS:
        return format_percent(value)
    return format_int(value)


def calc_change_rate(today_value, yesterday_value):
    if today_value is None or yesterday_value is None:
        return None
    yesterday_float = float(yesterday_value)
    if yesterday_float == 0:
        return None
    return (float(today_value) - yesterday_float) / yesterday_float


def format_change(rate) -> str:
    if rate is None:
        return "-"
    sign = "+" if rate > 0 else ""
    text = f"{sign}{rate * 100:.2f}%"
    if rate > 0:
        return f"<font color='green'>{text}</font>"
    if rate < 0:
        return f"<font color='red'>{text}</font>"
    return text


def build_metric_rows(today_row: dict, yesterday_row: dict | None) -> list[dict]:
    rows = []
    for label, field in METRIC_DEFS:
        today_value = today_row.get(field)
        yesterday_value = yesterday_row.get(field) if yesterday_row else None
        rate = calc_change_rate(today_value, yesterday_value)
        rows.append(
            {
                "metric": label,
                "value": format_metric_value(field, today_value),
                "change": format_change(rate),
            }
        )
    return rows


def build_metrics_table(rows: list[dict]) -> dict:
    return {
        "tag": "table",
        "element_id": "makepod_daily_metrics",
        "margin": "8px 0px 0px 0px",
        "page_size": len(rows),
        "row_height": "low",
        "header_style": {
            "text_align": "center",
            "text_size": "normal",
            "background_style": "grey",
            "text_color": "default",
            "bold": True,
            "lines": 1,
        },
        "columns": [
            {
                "name": "metric",
                "display_name": "指标",
                "data_type": "text",
                "horizontal_align": "center",
                "vertical_align": "top",
                "width": "40%",
            },
            {
                "name": "value",
                "display_name": "当日",
                "data_type": "text",
                "horizontal_align": "center",
                "vertical_align": "top",
                "width": "30%",
            },
            {
                "name": "change",
                "display_name": "较前日",
                "data_type": "lark_md",
                "horizontal_align": "center",
                "vertical_align": "top",
                "width": "30%",
            },
        ],
        "rows": rows,
    }


def build_dashboard_button() -> dict:
    return {
        "tag": "button",
        "text": {
            "tag": "plain_text",
            "content": "打开【Makepod】平台看板",
        },
        "type": "primary",
        "width": "default",
        "size": "medium",
        "behaviors": [
            {
                "type": "open_url",
                "default_url": DASHBOARD_URL,
            }
        ],
        "margin": "16px 0px 0px 0px",
    }


def build_card_payload(
    target_date: date, today_row: dict, yesterday_row: dict | None
) -> dict:
    dt_text = target_date.isoformat()
    rows = build_metric_rows(today_row, yesterday_row)

    update_time = today_row.get("update_time")
    if isinstance(update_time, datetime):
        update_time_text = update_time.strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        update_time_text = "--"

    return {
        "msg_type": "interactive",
        "card": {
            "schema": "2.0",
            "header": {
                "template": "blue",
                "title": {
                    "tag": "plain_text",
                    "content": f"Makepod 数据日报｜{dt_text}",
                },
            },
            "body": {
                "direction": "vertical",
                "padding": "12px 12px 12px 12px",
                "elements": [
                    {
                        "tag": "markdown",
                        "content": (
                            f"**数据日期**：{dt_text}\n"
                            f"**日期口径**：Asia/Shanghai 昨日，channel=Overall\n"
                            f"**更新时间**：{update_time_text}"
                        ),
                        "text_align": "left",
                        "margin": "0px 0px 0px 0px",
                    },
                    build_metrics_table(rows),
                    build_dashboard_button(),
                ],
            },
        },
    }


def build_alert_payload(target_date: date) -> dict:
    dt_text = target_date.isoformat()
    return {
        "msg_type": "interactive",
        "card": {
            "schema": "2.0",
            "header": {
                "template": "red",
                "title": {
                    "tag": "plain_text",
                    "content": f"Makepod 数据日报｜{dt_text}",
                },
            },
            "body": {
                "direction": "vertical",
                "padding": "12px 12px 12px 12px",
                "elements": [
                    {
                        "tag": "markdown",
                        "content": (
                            f"⚠️ `ads_daily_makepod` 未找到 dt=`{dt_text}`、"
                            "channel=`Overall` 的数据，请检查 ETL 是否已产出。"
                        ),
                        "text_align": "left",
                        "margin": "0px 0px 0px 0px",
                    },
                    build_dashboard_button(),
                ],
            },
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
    logger.info(f"准备推送 Makepod 数据日报，dt={target_date}")

    rows_by_dt = fetch_makepod_rows(target_date)
    today_row = rows_by_dt.get(target_date)
    yesterday_row = rows_by_dt.get(target_date - timedelta(days=1))

    if today_row is None:
        logger.warning(f"ads_daily_makepod 未找到 dt={target_date}、channel=Overall 的数据")
        payload = build_alert_payload(target_date)
    else:
        payload = build_card_payload(target_date, today_row, yesterday_row)

    if args.dry_run:
        logger.info("dry-run 模式，不实际发送 webhook")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    webhook_url = args.webhook_url or load_lark_webhook()
    send_lark_message(webhook_url, payload)
    logger.info(f"Lark 推送完成，dt={target_date}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
