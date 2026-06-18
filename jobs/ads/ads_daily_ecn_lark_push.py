from __future__ import annotations

import argparse
from datetime import date, datetime
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
TORONTO_TZ = "America/Toronto"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
ETL_CONFIG_PATH = PROJECT_ROOT / "env" / "etl_config.json"
LARK_WEBHOOK_CONFIG_PATH = PROJECT_ROOT / "env" / "lark_webhook.json"

client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="推送 ECN 数据发布指标的 Lark 机器人消息"
    )
    parser.add_argument(
        "--date",
        dest="target_date",
        help="指定推送日期，格式 YYYY-MM-DD；默认取 America/Toronto 昨天",
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
    webhook_key = "remote_ecn" if mode == "remote" else mode
    return must_non_empty_str(data.get(webhook_key), f"lark_webhook.{webhook_key}")


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


def fetch_publish_metrics(target_date: date) -> dict:
    query = f"""
    WITH
    -- 源表按 platform 拆行，推送层按 stat_date 汇总三端总量。
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
    -- 新增视频数仍来自投资人 ADS 日表，保证与看板口径一致。
    investor_stats AS (
        SELECT
            dt,
            new_video_count,
            update_time
        FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_investor`
        WHERE dt IN (
            @target_date,
            DATE_SUB(@target_date, INTERVAL 1 DAY)
        )
    )
    SELECT
        @target_date AS dt,
        today_publish.active_account_count AS active_account_count,
        yesterday_publish.active_account_count AS previous_active_account_count,
        today_publish.api_task_total AS api_task_total,
        yesterday_publish.api_task_total AS previous_api_task_total,
        today_publish.rpa_task_total AS rpa_task_total,
        yesterday_publish.rpa_task_total AS previous_rpa_task_total,
        CASE
            WHEN today_publish.stat_date IS NULL THEN NULL
            ELSE COALESCE(today_publish.api_task_total, 0)
                + COALESCE(today_publish.rpa_task_total, 0)
        END AS publish_task_total,
        CASE
            WHEN yesterday_publish.stat_date IS NULL THEN NULL
            ELSE COALESCE(yesterday_publish.api_task_total, 0)
                + COALESCE(yesterday_publish.rpa_task_total, 0)
        END AS previous_publish_task_total,
        today_publish.api_task_success_count AS api_task_success_count,
        yesterday_publish.api_task_success_count AS previous_api_task_success_count,
        today_publish.rpa_task_success_count AS rpa_task_success_count,
        yesterday_publish.rpa_task_success_count
            AS previous_rpa_task_success_count,
        CASE
            WHEN today_publish.stat_date IS NULL THEN NULL
            ELSE COALESCE(today_publish.api_task_success_count, 0)
                + COALESCE(today_publish.rpa_task_success_count, 0)
        END AS publish_task_success_count,
        CASE
            WHEN yesterday_publish.stat_date IS NULL THEN NULL
            ELSE COALESCE(yesterday_publish.api_task_success_count, 0)
                + COALESCE(yesterday_publish.rpa_task_success_count, 0)
        END AS previous_publish_task_success_count,
        today_investor.new_video_count AS new_video_count,
        yesterday_investor.new_video_count AS previous_new_video_count,
        today_investor.update_time AS update_time
    FROM (SELECT @target_date AS dt) d
    LEFT JOIN publish_stats today_publish
        ON today_publish.stat_date = d.dt
    LEFT JOIN publish_stats yesterday_publish
        ON yesterday_publish.stat_date = DATE_SUB(d.dt, INTERVAL 1 DAY)
    LEFT JOIN investor_stats today_investor
        ON today_investor.dt = d.dt
    LEFT JOIN investor_stats yesterday_investor
        ON yesterday_investor.dt = DATE_SUB(d.dt, INTERVAL 1 DAY)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("target_date", "DATE", target_date)
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        raise RuntimeError(f"未找到 dt={target_date} 的发布运营数据")
    return dict(rows[0].items())


def format_int(value) -> str:
    if value is None:
        return "--"
    return f"{int(value):,}"


def format_today_yesterday(today_value, yesterday_value) -> str:
    return (
        f"当日 {format_int(today_value)} / "
        f"昨日 {format_int(yesterday_value)}"
    )


def format_percent(value, scale: int = 2) -> str:
    if value is None:
        return "--"
    return f"{float(value) * 100:,.{scale}f}%"


def calc_rate(numerator, denominator):
    if numerator is None or denominator is None:
        return None
    if denominator == 0:
        return None
    return float(numerator) / float(denominator)


def format_rate_today_yesterday(
    today_numerator,
    today_denominator,
    yesterday_numerator,
    yesterday_denominator,
) -> str:
    return (
        f"当日 {format_percent(calc_rate(today_numerator, today_denominator))} / "
        f"昨日 {format_percent(calc_rate(yesterday_numerator, yesterday_denominator))}"
    )


def build_card_payload(row: dict) -> dict:
    dt_text = row["dt"].isoformat()
    update_time = row.get("update_time")
    if isinstance(update_time, datetime):
        update_time_text = update_time.strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        update_time_text = "--"

    metrics_publish = "\n".join(
        [
            (
                "**活跃运营账号数**："
                f"{format_today_yesterday(row['active_account_count'], row['previous_active_account_count'])}"
            ),
            (
                "**API 发布视频数**："
                f"{format_today_yesterday(row['api_task_total'], row['previous_api_task_total'])}"
            ),
            (
                "**RPA 发布视频数**："
                f"{format_today_yesterday(row['rpa_task_total'], row['previous_rpa_task_total'])}"
            ),
            (
                "**发布总视频数**："
                f"{format_today_yesterday(row['publish_task_total'], row['previous_publish_task_total'])}"
            ),
            (
                "**账号发视频率**："
                f"{format_rate_today_yesterday(row['publish_task_total'], row['active_account_count'], row['previous_publish_task_total'], row['previous_active_account_count'])}"
            ),
            (
                "**API 发布成功视频数**："
                f"{format_today_yesterday(row['api_task_success_count'], row['previous_api_task_success_count'])}"
            ),
            (
                "**RPA 发布成功视频数**："
                f"{format_today_yesterday(row['rpa_task_success_count'], row['previous_rpa_task_success_count'])}"
            ),
            (
                "**发布成功总视频数**："
                f"{format_today_yesterday(row['publish_task_success_count'], row['previous_publish_task_success_count'])}"
            ),
            (
                "**视频发布成功率**："
                f"{format_rate_today_yesterday(row['publish_task_success_count'], row['publish_task_total'], row['previous_publish_task_success_count'], row['previous_publish_task_total'])}"
            ),
            (
                "**新增视频数**："
                f"{format_today_yesterday(row['new_video_count'], row['previous_new_video_count'])}"
            ),
            (
                "**社媒平台发布视频数读数成功率**："
                f"{format_rate_today_yesterday(row['new_video_count'], row['publish_task_total'], row['previous_new_video_count'], row['previous_publish_task_total'])}"
            ),
        ]
    )

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
                    "content": f"ECN 数据发布 | {dt_text}",
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
                {"tag": "div", "text": {"tag": "lark_md", "content": metrics_publish}},
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
    logger.info("准备推送 ECN 数据发布指标，dt=%s", target_date)

    row = fetch_publish_metrics(target_date)
    payload = build_card_payload(row)

    if args.dry_run:
        logger.info("dry-run 模式，不实际发送 webhook")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    webhook_url = args.webhook_url or load_lark_webhook()
    send_lark_message(webhook_url, payload)
    logger.info("Lark ECN 数据发布指标推送完成，dt=%s", target_date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
