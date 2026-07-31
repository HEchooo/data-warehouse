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

# 第四块展示的支付用户分层；周表出口只保留有支付数据的组合，缺失分组在推送层补零。
ORDER_GROUPS = ["1单", "2单", "3单及以上"]

client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="推送【Makepod】平台看板数据周报的 Lark 机器人消息"
    )
    parser.add_argument(
        "--week-start",
        dest="week_start",
        help="指定周一日期，格式 YYYY-MM-DD；默认取 Asia/Shanghai 上一个完整自然周",
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


def get_week_start(week_start_text: str | None) -> date:
    if week_start_text:
        try:
            week_start = date.fromisoformat(week_start_text)
        except ValueError as exc:
            raise RuntimeError("--week-start 必须是 YYYY-MM-DD 格式") from exc
        if week_start.weekday() != 0:
            raise RuntimeError("--week-start 必须是周一日期")
        return week_start

    # 上一个完整自然周的周一：本周一往前推 7 天。
    query = f"""
    SELECT DATE_SUB(
        DATE_TRUNC(CURRENT_DATE('{MAKEPOD_TZ}'), WEEK(MONDAY)),
        INTERVAL 7 DAY
    ) AS week_start
    """
    result = list(client.query(query).result())
    if not result:
        raise RuntimeError("无法计算默认推送周")
    return result[0].week_start


def fetch_channel_aggregates(week_start: date) -> dict[str, dict[str, dict]]:
    """按 channel 聚合上周与上上周；日均 DAU 取 AVG，交易类指标累加。"""
    query = f"""
    SELECT
        IF(dt >= @week_start, 'current', 'previous') AS week_tag,
        channel,
        COUNT(DISTINCT dt) AS day_count,
        AVG(dau_uv_total) AS avg_dau,
        SUM(pay_order_count) AS pay_order_count,
        SUM(pay_gmv_usd) AS pay_gmv_usd,
        SUM(gross_profit_usd) AS gross_profit_usd,
        MAX(update_time) AS update_time
    FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_makepod`
    WHERE dt BETWEEN DATE_SUB(@week_start, INTERVAL 7 DAY)
                 AND DATE_ADD(@week_start, INTERVAL 6 DAY)
    GROUP BY week_tag, channel
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("week_start", "DATE", week_start)
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())

    aggregates: dict[str, dict[str, dict]] = {"current": {}, "previous": {}}
    for row in rows:
        aggregates[row.week_tag][row.channel] = dict(row.items())
    return aggregates


def fetch_daily_trend(week_start: date) -> list[dict]:
    query = f"""
    SELECT
        dt,
        dau_uv_total,
        pay_order_count,
        pay_gmv_usd
    FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_makepod`
    WHERE channel = 'Overall'
      AND dt BETWEEN @week_start AND DATE_ADD(@week_start, INTERVAL 6 DAY)
    ORDER BY dt
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("week_start", "DATE", week_start)
        ]
    )
    return [dict(row.items()) for row in client.query(query, job_config=job_config).result()]


def fetch_order_distribution(week_start: date) -> dict[str, dict]:
    query = f"""
    SELECT
        order_group,
        pay_user_count,
        pay_order_count,
        pay_gmv_usd,
        pay_user_count_share,
        pay_gmv_share,
        pay_gmv_usd_wow,
        update_time
    FROM `{PROJECT_ID}.{DATASET_ID}.ads_weekly_makepod_paid_user_order`
    WHERE channel = 'Overall'
      AND week_start_dt = @week_start
      AND order_group IN UNNEST(@order_groups)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("week_start", "DATE", week_start),
            bigquery.ArrayQueryParameter("order_groups", "STRING", ORDER_GROUPS),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    return {row.order_group: dict(row.items()) for row in rows}


def to_float(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def format_int(value) -> str:
    if value is None:
        return "--"
    return f"{int(value):,}"


def format_rounded(value, scale: int = 1) -> str:
    number = to_float(value)
    if number is None:
        return "--"
    return f"{number:,.{scale}f}"


def format_usd(value) -> str:
    number = to_float(value)
    if number is None:
        return "--"
    return f"${number:,.2f}"


def format_percent(value, scale: int = 2) -> str:
    number = to_float(value)
    if number is None:
        return "--"
    return f"{number * 100:,.{scale}f}%"


def calc_change_rate(current, previous):
    current_float = to_float(current)
    previous_float = to_float(previous)
    if current_float is None or previous_float is None or previous_float == 0:
        return None
    return (current_float - previous_float) / previous_float


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


def calc_gross_profit_rate(row: dict | None):
    if not row:
        return None
    gmv = to_float(row.get("pay_gmv_usd"))
    profit = to_float(row.get("gross_profit_usd"))
    if gmv is None or profit is None or gmv == 0:
        return None
    return profit / gmv


def format_update_time(
    current_overall: dict | None, distribution: dict[str, dict]
) -> str:
    """①②③来自日报表、④来自周表，取两者较晚的更新时间。"""
    candidates = [(current_overall or {}).get("update_time")]
    candidates.extend(row.get("update_time") for row in distribution.values())

    update_times = [value for value in candidates if isinstance(value, datetime)]
    if not update_times:
        return "--"
    return max(update_times).strftime("%Y-%m-%d %H:%M:%S UTC")


def table_header_style() -> dict:
    return {
        "text_align": "center",
        "text_size": "normal",
        "background_style": "grey",
        "text_color": "default",
        "bold": True,
        "lines": 1,
    }


def text_column(name: str, display_name: str, width: str) -> dict:
    return {
        "name": name,
        "display_name": display_name,
        "data_type": "text",
        "horizontal_align": "center",
        "vertical_align": "top",
        "width": width,
    }


def md_column(name: str, display_name: str, width: str) -> dict:
    column = text_column(name, display_name, width)
    column["data_type"] = "lark_md"
    return column


def build_overview_table(current: dict | None, previous: dict | None) -> dict:
    current = current or {}
    previous = previous or {}

    current_rate = calc_gross_profit_rate(current)
    previous_rate = calc_gross_profit_rate(previous)

    metrics = [
        (
            "日均 DAU",
            format_rounded(current.get("avg_dau")),
            calc_change_rate(current.get("avg_dau"), previous.get("avg_dau")),
        ),
        (
            "总支付订单数",
            format_int(current.get("pay_order_count")),
            calc_change_rate(
                current.get("pay_order_count"), previous.get("pay_order_count")
            ),
        ),
        (
            "总 GMV（USD）",
            format_usd(current.get("pay_gmv_usd")),
            calc_change_rate(current.get("pay_gmv_usd"), previous.get("pay_gmv_usd")),
        ),
        (
            "总毛利（USD）",
            format_usd(current.get("gross_profit_usd")),
            calc_change_rate(
                current.get("gross_profit_usd"), previous.get("gross_profit_usd")
            ),
        ),
        (
            "总毛利率",
            format_percent(current_rate),
            calc_change_rate(current_rate, previous_rate),
        ),
    ]

    return {
        "tag": "table",
        "element_id": "makepod_weekly_overview",
        "margin": "8px 0px 0px 0px",
        "page_size": len(metrics),
        "row_height": "low",
        "header_style": table_header_style(),
        "columns": [
            text_column("metric", "指标", "40%"),
            text_column("value", "上周", "30%"),
            md_column("change", "较上上周", "30%"),
        ],
        "rows": [
            {"metric": label, "value": value, "change": format_change(rate)}
            for label, value, rate in metrics
        ],
    }


def build_trend_charts(trend_rows: list[dict]) -> list[dict]:
    """三个指标量级差异大（DAU 百级、订单个位、GMV 百级），分成三张图避免小量级被压平。"""
    specs = [
        ("流量趋势（DAU）", "dau_uv_total"),
        ("交易趋势（支付成功订单数）", "pay_order_count"),
        ("GMV 趋势（USD）", "pay_gmv_usd"),
    ]

    charts = []
    for title, field in specs:
        values = [
            {
                "date": row["dt"].strftime("%m-%d"),
                "value": to_float(row.get(field)) or 0,
            }
            for row in trend_rows
        ]
        charts.append(
            {
                "tag": "chart",
                "aspect_ratio": "16:9",
                "color_theme": "brand",
                "margin": "8px 0px 0px 0px",
                "chart_spec": {
                    "type": "line",
                    "title": {"text": title},
                    "data": {"values": values},
                    "xField": "date",
                    "yField": "value",
                    "label": {"visible": True},
                },
            }
        )
    return charts


def build_peak_summary(trend_rows: list[dict]) -> str:
    """峰谷取最大/最小值所在日期；数值并列时取最早的一天。"""
    specs = [
        ("流量", "dau_uv_total", format_int),
        ("交易", "pay_order_count", lambda v: f"{format_int(v)} 单"),
        ("GMV", "pay_gmv_usd", format_usd),
    ]

    lines = []
    for label, field, formatter in specs:
        points: list[tuple[date, float]] = []
        for row in trend_rows:
            number = to_float(row.get(field))
            if number is not None:
                points.append((row["dt"], number))

        if not points:
            lines.append(f"**{label}**：--")
            continue

        peak_dt, peak_value = max(points, key=lambda point: point[1])
        low_dt, low_value = min(points, key=lambda point: point[1])
        lines.append(
            f"**{label}**：峰值 {peak_dt.strftime('%m-%d')}"
            f"（{formatter(peak_value)}）"
            f"｜低值 {low_dt.strftime('%m-%d')}"
            f"（{formatter(low_value)}）"
        )
    return "\n".join(lines)


def build_channel_table(
    current_by_channel: dict[str, dict], previous_by_channel: dict[str, dict]
) -> dict | None:
    """只展示有量的渠道（日均 DAU 或 GMV 非 0），按 GMV 降序。"""
    rows = []
    for channel, current in current_by_channel.items():
        if channel == "Overall":
            continue
        avg_dau = to_float(current.get("avg_dau")) or 0
        gmv = to_float(current.get("pay_gmv_usd")) or 0
        if avg_dau == 0 and gmv == 0:
            continue

        previous = previous_by_channel.get(channel, {})
        rows.append(
            {
                "sort_gmv": gmv,
                "channel": channel,
                "avg_dau": format_rounded(current.get("avg_dau")),
                "pay_order_count": format_int(current.get("pay_order_count")),
                "pay_gmv_usd": format_usd(current.get("pay_gmv_usd")),
                "gross_profit_usd": format_usd(current.get("gross_profit_usd")),
                "gmv_change": format_change(
                    calc_change_rate(
                        current.get("pay_gmv_usd"), previous.get("pay_gmv_usd")
                    )
                ),
            }
        )

    if not rows:
        return None

    rows.sort(key=lambda row: row["sort_gmv"], reverse=True)
    for row in rows:
        row.pop("sort_gmv")

    return {
        "tag": "table",
        "element_id": "makepod_weekly_channel",
        "margin": "8px 0px 0px 0px",
        "page_size": len(rows),
        "row_height": "low",
        "header_style": table_header_style(),
        "columns": [
            text_column("channel", "渠道", "22%"),
            text_column("avg_dau", "日均DAU", "14%"),
            text_column("pay_order_count", "订单", "12%"),
            text_column("pay_gmv_usd", "GMV", "18%"),
            text_column("gross_profit_usd", "毛利", "18%"),
            md_column("gmv_change", "GMV环比", "16%"),
        ],
        "rows": rows,
    }


def build_order_distribution_table(distribution: dict[str, dict]) -> dict:
    rows = []
    for order_group in ORDER_GROUPS:
        row = distribution.get(order_group)
        if row is None:
            # 周表出口过滤掉了无支付数据的分组，这里补零保证三层始终齐全。
            rows.append(
                {
                    "order_group": order_group,
                    "pay_user_count": "0（0.00%）",
                    "pay_order_count": "0",
                    "pay_gmv_usd": "$0.00（0.00%）",
                    "gmv_change": "-",
                }
            )
            continue

        rows.append(
            {
                "order_group": order_group,
                "pay_user_count": (
                    f"{format_int(row.get('pay_user_count'))}"
                    f"（{format_percent(row.get('pay_user_count_share'))}）"
                ),
                "pay_order_count": format_int(row.get("pay_order_count")),
                "pay_gmv_usd": (
                    f"{format_usd(row.get('pay_gmv_usd'))}"
                    f"（{format_percent(row.get('pay_gmv_share'))}）"
                ),
                "gmv_change": format_change(to_float(row.get("pay_gmv_usd_wow"))),
            }
        )

    return {
        "tag": "table",
        "element_id": "makepod_weekly_order_distribution",
        "margin": "8px 0px 0px 0px",
        "page_size": len(rows),
        "row_height": "low",
        "header_style": table_header_style(),
        "columns": [
            text_column("order_group", "分组", "18%"),
            text_column("pay_user_count", "用户数（占比）", "24%"),
            text_column("pay_order_count", "订单", "12%"),
            text_column("pay_gmv_usd", "GMV（占比）", "28%"),
            md_column("gmv_change", "GMV环比", "18%"),
        ],
        "rows": rows,
    }


def build_section_title(content: str) -> dict:
    return {
        "tag": "markdown",
        "content": f"# {content}",
        "text_align": "left",
        "margin": "16px 0px 0px 0px",
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
    week_start: date,
    aggregates: dict[str, dict[str, dict]],
    trend_rows: list[dict],
    distribution: dict[str, dict],
) -> dict:
    week_end = week_start + timedelta(days=6)
    week_range = f"{week_start.isoformat()} ~ {week_end.isoformat()}"

    current_overall = aggregates["current"].get("Overall")
    previous_overall = aggregates["previous"].get("Overall")

    day_count = int((current_overall or {}).get("day_count") or 0)
    day_count_note = "" if day_count == 7 else f"（仅 {day_count} 天数据）"

    elements = [
        {
            "tag": "markdown",
            "content": (
                f"**统计周期**：{week_range}{day_count_note}\n"
                f"**日期口径**：Asia/Shanghai 自然周（周一~周日），channel=Overall\n"
                f"**更新时间**：{format_update_time(current_overall, distribution)}"
            ),
            "text_align": "left",
            "margin": "0px 0px 0px 0px",
        },
        build_section_title("① 数据总览"),
        build_overview_table(current_overall, previous_overall),
        build_section_title("② 趋势峰谷"),
    ]

    elements.extend(build_trend_charts(trend_rows))
    elements.append(
        {
            "tag": "markdown",
            "content": build_peak_summary(trend_rows),
            "text_align": "left",
            "margin": "8px 0px 0px 0px",
        }
    )

    elements.append(build_section_title("③ 渠道明细"))
    channel_table = build_channel_table(
        aggregates["current"], aggregates["previous"]
    )
    if channel_table is None:
        elements.append(
            {
                "tag": "markdown",
                "content": "本周各渠道均无流量与交易数据。",
                "text_align": "left",
                "margin": "8px 0px 0px 0px",
            }
        )
    else:
        elements.append(channel_table)

    elements.append(build_section_title("④ 订单分布"))
    elements.append(build_order_distribution_table(distribution))
    elements.append(build_dashboard_button())

    return {
        "msg_type": "interactive",
        "card": {
            "schema": "2.0",
            "header": {
                "template": "blue",
                "title": {
                    "tag": "plain_text",
                    "content": f"Makepod 数据周报｜{week_range}",
                },
            },
            "body": {
                "direction": "vertical",
                "padding": "12px 12px 12px 12px",
                "elements": elements,
            },
        },
    }


def build_alert_payload(week_start: date) -> dict:
    week_end = week_start + timedelta(days=6)
    week_range = f"{week_start.isoformat()} ~ {week_end.isoformat()}"
    return {
        "msg_type": "interactive",
        "card": {
            "schema": "2.0",
            "header": {
                "template": "red",
                "title": {
                    "tag": "plain_text",
                    "content": f"Makepod 数据周报｜{week_range}",
                },
            },
            "body": {
                "direction": "vertical",
                "padding": "12px 12px 12px 12px",
                "elements": [
                    {
                        "tag": "markdown",
                        "content": (
                            f"⚠️ `ads_daily_makepod` 未找到 `{week_range}`、"
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
    week_start = get_week_start(args.week_start)
    logger.info(f"准备推送 Makepod 数据周报，week_start={week_start}")

    aggregates = fetch_channel_aggregates(week_start)
    trend_rows = fetch_daily_trend(week_start)
    distribution = fetch_order_distribution(week_start)

    if not trend_rows or aggregates["current"].get("Overall") is None:
        logger.warning(
            f"ads_daily_makepod 未找到 week_start={week_start} 的 Overall 数据"
        )
        payload = build_alert_payload(week_start)
    else:
        payload = build_card_payload(
            week_start, aggregates, trend_rows, distribution
        )

    if args.dry_run:
        logger.info("dry-run 模式，不实际发送 webhook")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    webhook_url = args.webhook_url or load_lark_webhook()
    send_lark_message(webhook_url, payload)
    logger.info(f"Lark 推送完成，week_start={week_start}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
