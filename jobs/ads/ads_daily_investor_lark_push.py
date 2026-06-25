from __future__ import annotations

import argparse
from datetime import date, datetime
import json
import logging
import os
from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP

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
METRIC_DOC_URL = "https://zjp80698olrf.jp.larksuite.com/wiki/Co2EwgCFriMX2vk4hiFjzYiUpmg"
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
    WITH
    -- 汇总目标日期和前一日的发布任务统计；源表按 platform 拆行，推送层只展示总量。
    publish_stats AS (
        SELECT
            stat_date,
            SUM(COALESCE(active_account_count, 0)) AS active_account_count,
            SUM(COALESCE(api_task_total, 0)) AS api_task_total,
            SUM(COALESCE(rpa_task_total, 0)) AS rpa_task_total,
            SUM(COALESCE(api_task_success_count, 0)) AS api_task_success_count,
            SUM(COALESCE(rpa_task_success_count, 0)) AS rpa_task_success_count
        FROM `{PROJECT_ID}.videos.video_publish_daily_stats`
        WHERE stat_date IN (
            @target_date,
            DATE_SUB(@target_date, INTERVAL 1 DAY)
        )
        GROUP BY stat_date
    ),
    -- 读取目标日期的投资人日报。
    target_investor AS (
        SELECT
            *
        FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_investor`
        WHERE dt = @target_date
        LIMIT 1
    ),
    -- 读取前一日新增视频数，供 Lark 卡片做当日 / 昨日对比。
    previous_investor AS (
        SELECT
            dt,
            new_video_count
        FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_investor`
        WHERE dt = DATE_SUB(@target_date, INTERVAL 1 DAY)
        LIMIT 1
    )
    SELECT
        t.dt,
        t.new_video_count,
        prev.new_video_count AS previous_new_video_count,
        t.active_video_count,
        t.new_video_view_count,
        t.avg_new_video_view_count,
        t.new_video_7d_avg_view_count,
        t.video_production_cost_usd,
        t.cost_per_thousand_views_usd,
        t.new_download_count,
        t.play_download_conversion_rate,
        t.new_device_count,
        t.active_device_count,
        t.active_registered_user_count,
        t.avg_duration_sec,
        t.avg_content_exposure_count,
        t.avg_content_click_count,
        t.content_ctr,
        t.avg_product_detail_exposure_count,
        t.next_day_retention_rate,
        t.day_7_retention_rate,
        t.update_time,
        today_stats.active_account_count AS active_account_count,
        yesterday_stats.active_account_count AS previous_active_account_count,
        today_stats.api_task_total AS api_task_total,
        yesterday_stats.api_task_total AS previous_api_task_total,
        today_stats.rpa_task_total AS rpa_task_total,
        yesterday_stats.rpa_task_total AS previous_rpa_task_total,
        CASE
            WHEN today_stats.stat_date IS NULL THEN NULL
            ELSE COALESCE(today_stats.api_task_total, 0)
                + COALESCE(today_stats.rpa_task_total, 0)
        END AS publish_task_total,
        CASE
            WHEN yesterday_stats.stat_date IS NULL THEN NULL
            ELSE COALESCE(yesterday_stats.api_task_total, 0)
                + COALESCE(yesterday_stats.rpa_task_total, 0)
        END AS previous_publish_task_total,
        today_stats.api_task_success_count AS api_task_success_count,
        yesterday_stats.api_task_success_count AS previous_api_task_success_count,
        today_stats.rpa_task_success_count AS rpa_task_success_count,
        yesterday_stats.rpa_task_success_count AS previous_rpa_task_success_count,
        CASE
            WHEN today_stats.stat_date IS NULL THEN NULL
            ELSE COALESCE(today_stats.api_task_success_count, 0)
                + COALESCE(today_stats.rpa_task_success_count, 0)
        END AS publish_task_success_count,
        CASE
            WHEN yesterday_stats.stat_date IS NULL THEN NULL
            ELSE COALESCE(yesterday_stats.api_task_success_count, 0)
                + COALESCE(yesterday_stats.rpa_task_success_count, 0)
        END AS previous_publish_task_success_count
    FROM target_investor t
    LEFT JOIN previous_investor prev
        ON prev.dt = DATE_SUB(t.dt, INTERVAL 1 DAY)
    LEFT JOIN publish_stats today_stats
        ON today_stats.stat_date = t.dt
    LEFT JOIN publish_stats yesterday_stats
        ON yesterday_stats.stat_date = DATE_SUB(t.dt, INTERVAL 1 DAY)
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


def format_rounded_int(value) -> str:
    if value is None:
        return "--"
    rounded_value = Decimal(str(value)).quantize(
        Decimal("1"), rounding=ROUND_HALF_UP
    )
    return f"{int(rounded_value):,}"


def format_percent(value, scale: int = 2) -> str:
    if value is None:
        return "--"
    if isinstance(value, Decimal):
        value = float(value)
    return f"{float(value) * 100:,.{scale}f}%"


def calc_rate(numerator, denominator):
    if numerator is None or denominator is None:
        return None
    if denominator == 0:
        return None
    return float(numerator) / float(denominator)


def format_duration_seconds(value) -> str:
    if value is None:
        return "--"
    if isinstance(value, Decimal):
        value = float(value)
    return f"{float(value):,.2f} 秒"


def build_value_rows(row: dict) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    overview_rows = [
        {"metric": "新增视频", "value": format_int(row["new_video_count"])},
        {"metric": "活跃视频数", "value": format_int(row["active_video_count"])},
        {
            "metric": "新增视频播放数",
            "value": format_int(row["new_video_view_count"]),
        },
        {
            "metric": "新增视频平均播放数",
            "value": format_rounded_int(row["avg_new_video_view_count"]),
        },
        {
            "metric": "新增视频 7 日播放数",
            "value": format_rounded_int(row["new_video_7d_avg_view_count"]),
        },
        {
            "metric": "视频制作成本（美元）",
            "value": format_decimal(row["video_production_cost_usd"]),
        },
        {
            "metric": "每千次播放成本（美元）",
            "value": format_decimal(row["cost_per_thousand_views_usd"], 2),
        },
    ]
    conversion_rows = [
        {"metric": "新增下载", "value": format_int(row["new_download_count"])},
        {
            "metric": "播放下载转化率",
            "value": format_percent(row["play_download_conversion_rate"]),
        },
    ]
    user_rows = [
        {"metric": "新增设备数", "value": format_int(row["new_device_count"])},
        {"metric": "活跃设备数", "value": format_int(row["active_device_count"])},
        {
            "metric": "活跃注册用户数",
            "value": format_int(row["active_registered_user_count"]),
        },
        {
            "metric": "人均停留时长",
            "value": format_duration_seconds(row["avg_duration_sec"]),
        },
    ]
    content_rows = [
        {
            "metric": "人均内容展示量",
            "value": format_decimal(row["avg_content_exposure_count"], 2),
        },
        {
            "metric": "人均内容点击量",
            "value": format_decimal(row["avg_content_click_count"], 2),
        },
        {"metric": "内容点击率", "value": format_percent(row["content_ctr"])},
        {
            "metric": "人均商详页点击量",
            "value": format_decimal(
                row["avg_product_detail_exposure_count"],
                2,
            ),
        },
    ]
    return overview_rows, conversion_rows, user_rows, content_rows


def build_publish_rows(row: dict) -> list[dict]:
    return [
        {
            "metric": "活跃运营账号数",
            "today": format_int(row["active_account_count"]),
            "yesterday": format_int(row["previous_active_account_count"]),
        },
        {
            "metric": "API 发布视频数",
            "today": format_int(row["api_task_total"]),
            "yesterday": format_int(row["previous_api_task_total"]),
        },
        {
            "metric": "RPA 发布视频数",
            "today": format_int(row["rpa_task_total"]),
            "yesterday": format_int(row["previous_rpa_task_total"]),
        },
        {
            "metric": "发布总视频数",
            "today": format_int(row["publish_task_total"]),
            "yesterday": format_int(row["previous_publish_task_total"]),
        },
        {
            "metric": "账号发视频率",
            "today": format_percent(
                calc_rate(
                    row["publish_task_total"],
                    row["active_account_count"],
                )
            ),
            "yesterday": format_percent(
                calc_rate(
                    row["previous_publish_task_total"],
                    row["previous_active_account_count"],
                )
            ),
        },
        {
            "metric": "API 发布成功视频数",
            "today": format_int(row["api_task_success_count"]),
            "yesterday": format_int(row["previous_api_task_success_count"]),
        },
        {
            "metric": "RPA 发布成功视频数",
            "today": format_int(row["rpa_task_success_count"]),
            "yesterday": format_int(row["previous_rpa_task_success_count"]),
        },
        {
            "metric": "发布成功总视频数",
            "today": format_int(row["publish_task_success_count"]),
            "yesterday": format_int(row["previous_publish_task_success_count"]),
        },
        {
            "metric": "视频发布成功率",
            "today": format_percent(
                calc_rate(
                    row["publish_task_success_count"],
                    row["publish_task_total"],
                )
            ),
            "yesterday": format_percent(
                calc_rate(
                    row["previous_publish_task_success_count"],
                    row["previous_publish_task_total"],
                )
            ),
        },
        {
            "metric": "新增视频数",
            "today": format_int(row["new_video_count"]),
            "yesterday": format_int(row["previous_new_video_count"]),
        },
        {
            "metric": "社媒平台发布视频数读数成功率",
            "today": format_percent(
                calc_rate(
                    row["new_video_count"],
                    row["publish_task_total"],
                )
            ),
            "yesterday": format_percent(
                calc_rate(
                    row["previous_new_video_count"],
                    row["previous_publish_task_total"],
                )
            ),
        },
    ]


def build_value_table(rows: list[dict], element_id: str) -> dict:
    return {
        "tag": "table",
        "element_id": element_id,
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
                "display_name": "Metrics",
                "data_type": "text",
                "horizontal_align": "center",
                "vertical_align": "top",
                "width": "50%",
            },
            {
                "name": "value",
                "display_name": "Value",
                "data_type": "text",
                "horizontal_align": "center",
                "vertical_align": "top",
                "width": "50%",
            },
        ],
        "rows": rows,
    }


def build_compare_table(rows: list[dict], element_id: str) -> dict:
    return {
        "tag": "table",
        "element_id": element_id,
        "margin": "8px 0px 0px 0px",
        "page_size": 10,
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
                "display_name": "Metrics",
                "data_type": "text",
                "horizontal_align": "center",
                "vertical_align": "top",
                "width": "45%",
            },
            {
                "name": "today",
                "display_name": "Today",
                "data_type": "text",
                "horizontal_align": "center",
                "vertical_align": "top",
                "width": "27.5%",
            },
            {
                "name": "yesterday",
                "display_name": "Yesterday",
                "data_type": "text",
                "horizontal_align": "center",
                "vertical_align": "top",
                "width": "27.5%",
            },
        ],
        "rows": rows,
    }


def build_card_payload(row: dict) -> dict:
    dt_text = row["dt"].isoformat()
    overview_rows, conversion_rows, user_rows, content_rows = build_value_rows(row)
    publish_rows = build_publish_rows(row)

    update_time = row.get("update_time")
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
                    "content": f"核心业务报表 - 明细表 | {dt_text}",
                },
            },
            "body": {
                "direction": "vertical",
                "padding": "12px 12px 12px 12px",
                "elements": [
                    {
                        "tag": "markdown",
                        "content": (
                            f"**数据日期**：`{dt_text}`\n"
                            f"**日期口径**：America/Toronto 昨日\n"
                            f"**更新时间**：{update_time_text}"
                        ),
                        "text_align": "left",
                        "margin": "0px 0px 0px 0px",
                    },
                    {
                        "tag": "markdown",
                        "content": "**概览**",
                        "text_align": "left",
                        "margin": "16px 0px 0px 0px",
                    },
                    build_value_table(overview_rows, element_id="overview_metrics"),
                    {
                        "tag": "markdown",
                        "content": "**发布**",
                        "text_align": "left",
                        "margin": "16px 0px 0px 0px",
                    },
                    build_compare_table(publish_rows, element_id="publish_metrics"),
                    {
                        "tag": "markdown",
                        "content": "**转化**",
                        "text_align": "left",
                        "margin": "16px 0px 0px 0px",
                    },
                    build_value_table(
                        conversion_rows,
                        element_id="conversion_metrics",
                    ),
                    {
                        "tag": "markdown",
                        "content": "**用户**",
                        "text_align": "left",
                        "margin": "16px 0px 0px 0px",
                    },
                    build_value_table(user_rows, element_id="user_metrics"),
                    {
                        "tag": "markdown",
                        "content": "**内容**",
                        "text_align": "left",
                        "margin": "16px 0px 0px 0px",
                    },
                    build_value_table(content_rows, element_id="content_metrics"),
                    {
                        "tag": "button",
                        "text": {
                            "tag": "plain_text",
                            "content": "打开 Dashboard",
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
                    },
                    {
                        "tag": "button",
                        "text": {
                            "tag": "plain_text",
                            "content": "查看数据口径",
                        },
                        "type": "primary",
                        "width": "default",
                        "size": "medium",
                        "behaviors": [
                            {
                                "type": "open_url",
                                "default_url": METRIC_DOC_URL,
                            }
                        ],
                        "margin": "12px 0px 0px 0px",
                    },
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
    logger.info(f"准备推送投资人报表明细，dt={target_date}")

    row = fetch_investor_row(target_date)
    payload = build_card_payload(row)

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
