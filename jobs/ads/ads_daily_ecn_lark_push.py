from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
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


def get_social_content_cohort_dates(target_date: date) -> list[date]:
    return [
        target_date - timedelta(days=1),
        target_date - timedelta(days=2),
        target_date - timedelta(days=3),
    ]


def fetch_social_content_metrics(target_date: date) -> dict:
    cohort_dates = get_social_content_cohort_dates(target_date)
    query = f"""
    WITH
    -- 固定取最近 3 个发布时间 cohort，供 push 展示 T+1 / T+3。
    target_dates AS (
        SELECT dt
        FROM UNNEST(@cohort_dates) AS dt
    ),
    -- 复用 ADS 中已产出的发布时间 cohort 总播放。
    published_view_metrics AS (
        SELECT
            dt,
            published_day1_view_count,
            published_day3_view_count,
            update_time
        FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_social_content_v2`
        WHERE channel = '全部渠道'
            AND dt IN (
                SELECT dt
                FROM target_dates
            )
    ),
    -- 复用 v2 状态层 + 统一视频键，计算全部渠道发布时间 cohort 视频数。
    cohort_video_counts AS (
        SELECT
            s.published_dt AS dt,
            COUNT(
                DISTINCT COALESCE(m.unified_video_key, s.video_key)
            ) AS cohort_video_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_social_video_state_daily_v2` s
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_auto_publish_video_task` m
            ON s.video_key = m.video_key
        WHERE s.published_dt IN (
            SELECT dt
            FROM target_dates
        )
            AND s.dt = s.published_dt
        GROUP BY s.published_dt
    )
    SELECT
        t.dt,
        SAFE_DIVIDE(
            CAST(v.published_day1_view_count AS NUMERIC),
            CAST(c.cohort_video_count AS NUMERIC)
        ) AS published_day1_avg_view_count,
        SAFE_DIVIDE(
            CAST(v.published_day3_view_count AS NUMERIC),
            CAST(c.cohort_video_count AS NUMERIC)
        ) AS published_day3_avg_view_count,
        c.cohort_video_count,
        v.update_time
    FROM target_dates t
    LEFT JOIN published_view_metrics v
        ON t.dt = v.dt
    LEFT JOIN cohort_video_counts c
        ON t.dt = c.dt
    ORDER BY t.dt DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("cohort_dates", "DATE", cohort_dates)
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        raise RuntimeError("未找到 Social Content v2 push 数据")
    return {
        "target_date": target_date,
        "cohort_dates": cohort_dates,
        "rows": [dict(row.items()) for row in rows],
    }


def fetch_thirdapp_metrics(target_date: date) -> dict:
    query = f"""
    SELECT
        dt,
        exposure_uv,
        video_detail_exposure_pv,
        product_click_count,
        update_time
    FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_thirdapp_channel_uv`
    WHERE dt = @target_date
        AND channel = 'overall'
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("target_date", "DATE", target_date)
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        raise RuntimeError(f"未找到 dt={target_date} 的第三方渠道外链跳转数据")
    return dict(rows[0].items())


def format_int(value) -> str:
    if value is None:
        return "--"
    return f"{int(value):,}"


def format_number(value, scale: int = 2) -> str:
    if value is None:
        return "--"
    numeric_value = float(value)
    if numeric_value.is_integer():
        return f"{int(numeric_value):,}"
    return f"{numeric_value:,.{scale}f}".rstrip("0").rstrip(".")


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


def build_table_rows(row: dict) -> list[dict]:
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


def build_metrics_table(rows: list[dict], element_id: str) -> dict:
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


def build_social_content_table(
    social_metrics: dict,
    element_id: str,
) -> dict:
    target_date = social_metrics["target_date"]
    social_row_map = {
        row["dt"]: row
        for row in social_metrics["rows"]
    }
    rows = [
        {
            "horizon": "T+1",
            **{
                f"date_{index}": format_number(
                    social_row_map.get(cohort_date, {}).get(
                        "published_day1_avg_view_count"
                    )
                    if cohort_date <= target_date - timedelta(days=1)
                    else None
                )
                for index, cohort_date in enumerate(
                    social_metrics["cohort_dates"], start=1
                )
            },
        },
        {
            "horizon": "T+3",
            **{
                f"date_{index}": format_number(
                    social_row_map.get(cohort_date, {}).get(
                        "published_day3_avg_view_count"
                    )
                    if cohort_date <= target_date - timedelta(days=3)
                    else None
                )
                for index, cohort_date in enumerate(
                    social_metrics["cohort_dates"], start=1
                )
            },
        },
    ]
    columns = [
        {
            "name": "horizon",
            "display_name": "观察窗",
            "data_type": "text",
            "horizontal_align": "center",
            "vertical_align": "top",
            "width": "16%",
        }
    ]
    for index, cohort_date in enumerate(social_metrics["cohort_dates"], start=1):
        columns.append(
            {
                "name": f"date_{index}",
                "display_name": cohort_date.isoformat(),
                "data_type": "text",
                "horizontal_align": "center",
                "vertical_align": "top",
                "width": "28%",
            }
        )

    return {
        "tag": "table",
        "element_id": element_id,
        "margin": "8px 0px 0px 0px",
        "page_size": 2,
        "row_height": "low",
        "header_style": {
            "text_align": "center",
            "text_size": "normal",
            "background_style": "grey",
            "text_color": "default",
            "bold": True,
            "lines": 1,
        },
        "columns": columns,
        "rows": rows,
    }


def build_thirdapp_table(row: dict, element_id: str) -> dict:
    rows = [
        {
            "metric": "曝光 UV",
            "value": format_int(row["exposure_uv"]),
        },
        {
            "metric": "视频详情页曝光 PV",
            "value": format_int(row["video_detail_exposure_pv"]),
        },
        {
            "metric": "商品点击个数",
            "value": format_int(row["product_click_count"]),
        },
    ]
    return {
        "tag": "table",
        "element_id": element_id,
        "margin": "8px 0px 0px 0px",
        "page_size": 3,
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
                "width": "60%",
            },
            {
                "name": "value",
                "display_name": "Value",
                "data_type": "text",
                "horizontal_align": "center",
                "vertical_align": "top",
                "width": "40%",
            },
        ],
        "rows": rows,
    }


def format_update_time(value) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S UTC")
    return "--"


def get_latest_update_time_text(*rows_or_metrics: dict) -> str:
    update_times = []
    for item in rows_or_metrics:
        if "rows" in item:
            update_times.extend(
                row.get("update_time")
                for row in item["rows"]
                if isinstance(row.get("update_time"), datetime)
            )
        elif isinstance(item.get("update_time"), datetime):
            update_times.append(item["update_time"])

    if not update_times:
        return "--"
    return format_update_time(max(update_times))


def build_card_payload(
    publish_row: dict,
    social_metrics: dict,
    thirdapp_row: dict,
) -> dict:
    dt_text = publish_row["dt"].isoformat()
    metrics_rows = build_table_rows(publish_row)
    publish_update_time_text = format_update_time(publish_row.get("update_time"))
    social_update_time_text = get_latest_update_time_text(social_metrics)
    thirdapp_update_time_text = format_update_time(thirdapp_row.get("update_time"))

    return {
        "msg_type": "interactive",
        "card": {
            "schema": "2.0",
            "header": {
                "template": "blue",
                "title": {
                    "tag": "plain_text",
                    "content": f"ECN 数据发布 | {dt_text}",
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
                            f"**更新时间**：{publish_update_time_text}"
                        ),
                        "text_align": "left",
                        "margin": "0px 0px 0px 0px",
                    },
                    build_metrics_table(
                        metrics_rows,
                        element_id="publish_metrics",
                    ),
                    {
                        "tag": "markdown",
                        "content": (
                            "#### 发布时间 cohort 平均播放\n"
                            f"**展示渠道**：`全部渠道`\n"
                            f"**更新时间**：{social_update_time_text}"
                        ),
                        "text_align": "left",
                        "margin": "16px 0px 0px 0px",
                    },
                    build_social_content_table(
                        social_metrics,
                        element_id="social_content_metrics",
                    ),
                    {
                        "tag": "markdown",
                        "content": (
                            "#### 第三方渠道外链跳转数据\n"
                            f"**展示渠道**：`overall`\n"
                            f"**更新时间**：{thirdapp_update_time_text}"
                        ),
                        "text_align": "left",
                        "margin": "16px 0px 0px 0px",
                    },
                    build_thirdapp_table(
                        thirdapp_row,
                        element_id="thirdapp_metrics",
                    ),
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
    logger.info("准备推送 ECN 数据发布指标，dt=%s", target_date)

    publish_row = fetch_publish_metrics(target_date)
    social_metrics = fetch_social_content_metrics(target_date)
    thirdapp_row = fetch_thirdapp_metrics(target_date)
    payload = build_card_payload(
        publish_row,
        social_metrics,
        thirdapp_row,
    )

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
