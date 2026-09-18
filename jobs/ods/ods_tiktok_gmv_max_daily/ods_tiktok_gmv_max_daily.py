"""ODS TikTok GMV Max 投放日报 ETL。

数据源：TikTok Business API
- `/gmv_max/store/list/`：拿到可投 GMV Max 的 store_id（报表接口单次只接受一个 store）
- `/gmv_max/report/get/`：campaign × 天的 GMV Max 指标

与普通竞价投放（`ods_tiktok_ad_daily`）是两套独立数据，不能混在一张表里：
- GMV Max 没有 adgroup / ad 层，最细的对外层级就是 campaign
- 报表接口是 `/gmv_max/report/get/`，metric 名也不同（`cost` 而不是 `spend`，没有曝光点击）
- `gross_revenue` 是商品全渠道归因（付费 + 自然 + 达人），与广告归因收入不是同一指标

默认按广告主时区回刷最近 7 天。窗口比普通投放的 4 天长，因为 GMV Max 的订单与收入会在
归因窗口内继续回填，campaign 停投后当天的 `gross_revenue` 仍可能变化。
"""

import argparse
from datetime import date, datetime, timedelta, timezone
import json
import logging
import time
from zoneinfo import ZoneInfo

from google.cloud import bigquery, secretmanager
import requests

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ods_tiktok_gmv_max_daily"
ADVERTISER_ID = "7667456906528735239"
# 与 ods_tiktok_ad_daily 同源：GMV Max 报表的 stat_time_day 同样按广告主时区切日
ADVERTISER_TZ = "Etc/GMT+5"
TIKTOK_API_BASE = "https://business-api.tiktok.com/open_api/v1.3"
ACCESS_TOKEN_SECRET_ID = "prod-ads-runner-tiktok-access-token"
# 默认回刷窗口天数，CURRENT_DATE-6 ~ CURRENT_DATE 共 7 天
REBUILD_DAYS = 6
# 接口硬限制：带 stat_time_day 时单次查询最多 30 天，超出直接报 40002
MAX_SPAN_DAYS = 30
API_PAGE_SIZE = 1000

# campaign 级实测可用的 metric，注释为 TikTok 后台中文列名。
# 只取看板真正要的五个指标，接口还支持 net_cost / roas_bid / target_roi_budget / operation_status，
# 当前不落库，需要时再加回来。
# 以下名字实测被拒绝，不要再加：spend / impressions / clicks / product_impressions /
# product_clicks / ad_click_rate / refund_gmv / net_sales / refunded_ad_cost / gmv / sku_orders。
REPORT_METRICS = [
    "cost",  # 成本
    "orders",  # SKU 订单数
    "cost_per_order",  # 平均下单成本
    "gross_revenue",  # 总收入
    "roi",  # ROI
    "campaign_name",
    "currency",
]
FLOAT_METRICS = [
    "cost",
    "cost_per_order",
    "gross_revenue",
    "roi",
]
INT_METRICS = ["orders"]

client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="拉取 TikTok GMV Max 投放日报（dt × advertiser_id × store_id × campaign_id）并替换目标分区"
    )
    parser.add_argument("--start-date", help="起始统计日 YYYY-MM-DD，默认 CURRENT_DATE-6")
    parser.add_argument("--end-date", help="结束统计日 YYYY-MM-DD，默认 CURRENT_DATE")
    return parser.parse_args()


def table_id(table_name: str = TABLE_NAME) -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{table_name}"


def get_access_token() -> str:
    # gRPC 在部分网络出口被拦（tcp handshaker shutdown），REST 走普通 HTTPS，两端都通
    secret_client = secretmanager.SecretManagerServiceClient(transport="rest")
    name = f"projects/{PROJECT_ID}/secrets/{ACCESS_TOKEN_SECRET_ID}/versions/latest"
    return secret_client.access_secret_version(request={"name": name}).payload.data.decode("utf-8")


def get_date_window(args: argparse.Namespace) -> tuple[date, date]:
    today = datetime.now(ZoneInfo(ADVERTISER_TZ)).date()
    start_date = date.fromisoformat(args.start_date) if args.start_date else today - timedelta(days=REBUILD_DAYS)
    end_date = date.fromisoformat(args.end_date) if args.end_date else today
    if start_date > end_date:
        raise ValueError(f"起始统计日晚于结束统计日: {start_date} > {end_date}")
    return start_date, end_date


def split_window(start_date: date, end_date: date) -> list[tuple[date, date]]:
    """接口单次最多 30 天，长窗口切片后逐段查。"""
    segments, cursor = [], start_date
    while cursor <= end_date:
        segment_end = min(cursor + timedelta(days=MAX_SPAN_DAYS - 1), end_date)
        segments.append((cursor, segment_end))
        cursor = segment_end + timedelta(days=1)
    return segments


def to_float(value) -> float | None:
    """API 用 "-" 表示该指标不适用，统一落成空值。"""
    text = str(value).strip() if value is not None else ""
    if text in ("", "-"):
        return None
    return float(text)


def to_int(value) -> int | None:
    number = to_float(value)
    return int(number) if number is not None else None


def fetch_all_pages(session: requests.Session, path: str, params: dict) -> list[dict]:
    rows, page = [], 1
    while True:
        response = session.get(
            f"{TIKTOK_API_BASE}{path}",
            params={**params, "page": page, "page_size": API_PAGE_SIZE},
            timeout=120,
        )
        response.raise_for_status()
        body = response.json()
        if body.get("code") != 0:
            raise RuntimeError(f"TikTok {path} 返回错误: code={body.get('code')} message={body.get('message')}")
        rows.extend(body["data"]["list"])
        if page >= body["data"]["page_info"]["total_page"]:
            return rows
        page += 1


def fetch_store_ids(session: requests.Session) -> list[str]:
    """GMV Max 报表按店铺查，store_id 只能从这个接口拿，不能拿 TikTok Shop 的 seller_id 硬编码。"""
    response = session.get(
        f"{TIKTOK_API_BASE}/gmv_max/store/list/",
        params={"advertiser_id": ADVERTISER_ID},
        timeout=60,
    )
    response.raise_for_status()
    body = response.json()
    if body.get("code") != 0:
        raise RuntimeError(f"TikTok /gmv_max/store/list/ 返回错误: {body.get('message')}")

    stores = body["data"].get("store_list") or []
    store_ids = [s["store_id"] for s in stores if s.get("is_gmv_max_available") and s.get("store_id")]
    skipped = [s.get("store_id") for s in stores if not s.get("is_gmv_max_available")]
    if skipped:
        logging.warning("跳过未开通 GMV Max 的店铺: %s", skipped)
    if not store_ids:
        raise RuntimeError("广告主下没有可投 GMV Max 的店铺，无法确定报表范围")
    logging.info("GMV Max 店铺: %s", store_ids)
    return store_ids


def fetch_report(session: requests.Session, store_id: str, start_date: date, end_date: date) -> list[dict]:
    rows = []
    for segment_start, segment_end in split_window(start_date, end_date):
        segment_rows = fetch_all_pages(
            session,
            "/gmv_max/report/get/",
            {
                "advertiser_id": ADVERTISER_ID,
                "store_ids": json.dumps([store_id]),
                "dimensions": json.dumps(["campaign_id", "stat_time_day"]),
                "metrics": json.dumps(REPORT_METRICS),
                "start_date": segment_start.isoformat(),
                "end_date": segment_end.isoformat(),
            },
        )
        logging.info(
            "GMV Max 报表拉取: store=%s %s ~ %s, rows=%d",
            store_id,
            segment_start,
            segment_end,
            len(segment_rows),
        )
        rows.extend(segment_rows)
    return rows


def build_rows(report_rows: list[dict], store_id: str) -> list[dict]:
    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for item in report_rows:
        metrics, dimensions = item["metrics"], item["dimensions"]
        rows.append(
            {
                "dt": dimensions["stat_time_day"][:10],
                "advertiser_id": ADVERTISER_ID,
                "store_id": store_id,
                "campaign_id": dimensions["campaign_id"],
                "campaign_name": metrics.get("campaign_name"),
                **{name: to_float(metrics.get(name)) for name in FLOAT_METRICS},
                **{name: to_int(metrics.get(name)) for name in INT_METRICS},
                "currency": metrics.get("currency"),
                "report_timezone": ADVERTISER_TZ,
                "fetched_at": fetched_at,
                "update_time": fetched_at,
            }
        )
    return rows


def validate(staging_table: str, start_date: date, end_date: date) -> None:
    result = list(
        client.query(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT CONCAT(
                    CAST(dt AS STRING), '|', advertiser_id, '|', store_id, '|', campaign_id
                )) AS unique_key_count,
                MIN(dt) AS min_dt,
                MAX(dt) AS max_dt,
                COUNTIF(currency IS NULL) AS null_currency_count,
                COUNTIF(campaign_name IS NULL OR campaign_name = '') AS missing_name_count
            FROM `{staging_table}`
            """
        ).result()
    )[0]
    if result.row_count == 0:
        raise RuntimeError("TikTok GMV Max 投放日报校验失败：staging 无数据")
    if result.row_count != result.unique_key_count:
        raise RuntimeError("TikTok GMV Max 投放日报校验失败：统计日、广告主、店铺和计划不唯一")
    if result.null_currency_count:
        raise RuntimeError("TikTok GMV Max 投放日报校验失败：存在没有货币单位的金额行")
    if result.min_dt < start_date or result.max_dt > end_date:
        raise RuntimeError(
            f"TikTok GMV Max 投放日报校验失败：统计日超出窗口 {result.min_dt} ~ {result.max_dt}"
        )
    if result.missing_name_count:
        logging.warning("有 %d 行没有计划名称，仅影响展示", result.missing_name_count)


def replace_partitions(rows: list[dict], start_date: date, end_date: date) -> None:
    """先建 staging，再把目标表窗口外分区与 staging 合并成新表整体换名，避免对目标表做 DELETE。"""
    if not rows:
        logging.warning("本次 TikTok GMV Max 投放日报无可写入记录，跳过表替换")
        return

    target_table = table_id()
    run_id = f"{start_date:%Y%m%d}_{int(time.time())}"
    staging_table = f"{target_table}_tmp_{run_id}"
    changed_table = f"{target_table}_changed_{run_id}"
    swapped = False

    try:
        client.query(
            f"CREATE TABLE `{staging_table}` AS SELECT * FROM `{target_table}` WHERE 1 = 0"
        ).result()

        client.load_table_from_json(
            rows,
            staging_table,
            job_id_prefix=f"tiktok_gmv_max_ods_{run_id}_",
            job_config=bigquery.LoadJobConfig(
                schema=client.get_table(target_table).schema,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ),
        ).result()

        validate(staging_table, start_date, end_date)

        client.query(
            f"""
            CREATE TABLE `{changed_table}`
            PARTITION BY dt
            CLUSTER BY advertiser_id, store_id, campaign_id AS
            SELECT * FROM `{target_table}`
            WHERE dt < DATE('{start_date}') OR dt > DATE('{end_date}')

            UNION ALL BY NAME

            SELECT * FROM `{staging_table}`
            """
        ).result()
        client.query(f"DROP TABLE `{target_table}`").result()
        client.query(f"ALTER TABLE `{changed_table}` RENAME TO {TABLE_NAME}").result()
        swapped = True
        logging.info(
            "TikTok GMV Max 投放日报替换完成: %s ~ %s, rows=%d", start_date, end_date, len(rows)
        )
    finally:
        client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
        if not swapped:
            logging.warning("保留变更表用于排查: %s", changed_table)


def run(args: argparse.Namespace) -> None:
    start_date, end_date = get_date_window(args)

    session = requests.Session()
    session.headers["Access-Token"] = get_access_token()

    rows = []
    for store_id in fetch_store_ids(session):
        rows.extend(build_rows(fetch_report(session, store_id, start_date, end_date), store_id))

    replace_partitions(rows, start_date, end_date)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    started_at = datetime.now(ZoneInfo(ADVERTISER_TZ))
    run(parse_args())
    logging.info(
        "%s 完成，耗时: %.1f 秒",
        TABLE_NAME,
        (datetime.now(ZoneInfo(ADVERTISER_TZ)) - started_at).total_seconds(),
    )
