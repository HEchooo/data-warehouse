"""ODS TikTok 广告投放日报 ETL。

数据源：TikTok Business API
- `/report/integrated/get/`：AUCTION_AD 层级的 ad × 天指标
- `/ad/get/`：补店铺、`tiktok_item_id`、identity
- `/identity/get/`：补 identity username（只展示，不当 join key）

默认按广告主时区回刷最近 4 天（CURRENT_DATE-3 ~ CURRENT_DATE），走 crontab 每日重跑即可覆盖
TikTok 归因回溯带来的历史修正。
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
TABLE_NAME = "ods_tiktok_ad_daily"
ADVERTISER_ID = "7667456906528735239"
# 广告主时区，取自 /advertiser/info/ 的 timezone，固定 UTC-5 不含夏令时；
# 报表 stat_time_day 按这个时区切日，日期窗口必须同源，否则会漏当天分区。
ADVERTISER_TZ = "Etc/GMT+5"
TIKTOK_API_BASE = "https://business-api.tiktok.com/open_api/v1.3"
ACCESS_TOKEN_SECRET_ID = "prod-ads-runner-tiktok-access-token"
# 默认回刷窗口天数，CURRENT_DATE-3 ~ CURRENT_DATE 共 4 天
REBUILD_DAYS = 3
# /ad/get/ 的 filtering.ad_ids 单次上限
AD_ID_BATCH_SIZE = 100
API_PAGE_SIZE = 1000

# 报表指标：注释为 TikTok 后台中文列名
REPORT_METRICS = [
    "spend",  # 消耗
    "cpc",  # 平均点击成本（目标页面）
    "cpm",  # 千次展示成本 CPM
    "impressions",  # 展示量
    "clicks",  # 点击量（目标页面）
    "ctr",  # 点击率（目标页面）
    "conversion",  # 转化量
    "cost_per_conversion",  # 平均转化成本
    "conversion_rate",  # 转化率 CVR
    "result",  # 成效数
    "cost_per_result",  # 平均成效成本
    "result_rate",  # 成效率
    "secondary_goal_result",  # 合格转化
    "cost_per_secondary_goal_result",  # 合格转化平均成本
    "secondary_goal_result_rate",  # 合格转化率
    "shop_gross_revenue_by_order_submission",  # Gross revenue (Shop)
    "total_onsite_shopping_value",  # Total complete payment value (onsite)
    "shop_total_items_purchased",  # Items purchased (Shop)
    "shop_total_purchase_by_order_submission",  # Purchases by order submission（笔数）
    "campaign_id",
    "campaign_name",
    "adgroup_id",
    "adgroup_name",
    "ad_name",
]
FLOAT_METRICS = [
    "spend",
    "cpc",
    "cpm",
    "ctr",
    "conversion",
    "cost_per_conversion",
    "conversion_rate",
    "result",
    "cost_per_result",
    "result_rate",
    "secondary_goal_result",
    "cost_per_secondary_goal_result",
    "secondary_goal_result_rate",
    "shop_gross_revenue_by_order_submission",
    "total_onsite_shopping_value",
    "shop_total_purchase_by_order_submission",
]
INT_METRICS = ["impressions", "clicks", "shop_total_items_purchased"]
AD_FIELDS = [
    "ad_id",
    "adgroup_id",
    "campaign_id",
    "tiktok_item_id",
    "identity_id",
    "identity_type",
    "display_name",
    "showcase_products",
]

client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="拉取 TikTok 广告投放日报（dt × advertiser_id × ad_id）并替换目标分区"
    )
    parser.add_argument("--start-date", help="起始统计日 YYYY-MM-DD，默认 CURRENT_DATE-3")
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


def to_float(value) -> float | None:
    """API 用 "-" 表示该指标不适用（例如广告组未配置深度转化目标），统一落成空值。"""
    text = str(value).strip() if value is not None else ""
    if text in ("", "-"):
        return None
    return float(text)


def to_int(value) -> int | None:
    number = to_float(value)
    return int(number) if number is not None else None


def to_str(value) -> str | None:
    text = str(value).strip() if value is not None else ""
    if text in ("", "-"):
        return None
    return text


def fetch_all_pages(
    session: requests.Session, path: str, params: dict, page_size: int = API_PAGE_SIZE
) -> list[dict]:
    """TikTok 接口统一按 page_info.total_page 翻页。"""
    rows, page = [], 1
    while True:
        response = session.get(
            f"{TIKTOK_API_BASE}{path}",
            params={**params, "page": page, "page_size": page_size},
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


def fetch_currency(session: requests.Session) -> str:
    """金额字段以广告主账户货币计价，报表接口不返回货币，只能从广告主信息取。"""
    response = session.get(
        f"{TIKTOK_API_BASE}/advertiser/info/",
        params={"advertiser_ids": json.dumps([ADVERTISER_ID])},
        timeout=60,
    )
    response.raise_for_status()
    body = response.json()
    if body.get("code") != 0:
        raise RuntimeError(f"TikTok /advertiser/info/ 返回错误: {body.get('message')}")
    currency = body["data"]["list"][0].get("currency")
    if not currency:
        raise RuntimeError("TikTok 广告主信息未返回货币单位，金额口径无法确定")
    logging.info("广告主货币单位: %s", currency)
    return currency


def fetch_report(session: requests.Session, start_date: date, end_date: date) -> list[dict]:
    rows = fetch_all_pages(
        session,
        "/report/integrated/get/",
        {
            "advertiser_id": ADVERTISER_ID,
            "service_type": "AUCTION",
            "report_type": "BASIC",
            "data_level": "AUCTION_AD",
            "dimensions": json.dumps(["ad_id", "stat_time_day"]),
            "metrics": json.dumps(REPORT_METRICS),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
    )
    logging.info("TikTok 报表拉取完成: %s ~ %s, rows=%d", start_date, end_date, len(rows))
    return rows


def fetch_ad_meta(session: requests.Session, ad_ids: list[str]) -> dict[str, dict]:
    """ad_id -> 店铺 / 素材视频 / identity。非带货广告没有店铺，tiktok_item_id 也可能为空。"""
    ads: dict[str, dict] = {}
    for offset in range(0, len(ad_ids), AD_ID_BATCH_SIZE):
        batch = ad_ids[offset : offset + AD_ID_BATCH_SIZE]
        for ad in fetch_all_pages(
            session,
            "/ad/get/",
            {
                "advertiser_id": ADVERTISER_ID,
                "filtering": json.dumps({"ad_ids": batch}),
                "fields": json.dumps(AD_FIELDS),
            },
        ):
            store_ids = sorted(
                {p["store_id"] for p in (ad.get("showcase_products") or []) if p.get("store_id")}
            )
            if len(store_ids) > 1:
                logging.warning("广告命中多个店铺，取最小值: ad_id=%s store_ids=%s", ad["ad_id"], store_ids)
            ads[ad["ad_id"]] = {
                "store_id": store_ids[0] if store_ids else None,
                "tiktok_item_id": to_str(ad.get("tiktok_item_id")),
                "identity_id": to_str(ad.get("identity_id")),
                "identity_type": to_str(ad.get("identity_type")),
                "identity_display_name": to_str(ad.get("display_name")),
            }

    missing = [ad_id for ad_id in ad_ids if ad_id not in ads]
    if missing:
        logging.warning("有 %d 个广告在 /ad/get/ 查不到: %s", len(missing), missing[:10])
    logging.info(
        "广告素材解析完成: 有店铺 %d 个，有 tiktok_item_id %d 个",
        sum(1 for v in ads.values() if v.get("store_id")),
        sum(1 for v in ads.values() if v.get("tiktok_item_id")),
    )
    return ads


def fetch_identities(session: requests.Session) -> dict[str, str | None]:
    """identity_id -> username，只入库展示。返回结构是 identity_list，page_info 可能全 0。"""
    rows, page = [], 1
    while True:
        response = session.get(
            f"{TIKTOK_API_BASE}/identity/get/",
            params={"advertiser_id": ADVERTISER_ID, "page": page, "page_size": 100},
            timeout=60,
        )
        response.raise_for_status()
        body = response.json()
        if body.get("code") != 0:
            raise RuntimeError(
                f"TikTok /identity/get/ 返回错误: code={body.get('code')} message={body.get('message')}"
            )
        chunk = body.get("data", {}).get("identity_list") or []
        rows.extend(chunk)
        if len(chunk) < 100:
            break
        page += 1
    identities = {
        identity["identity_id"]: to_str(identity.get("username"))
        for identity in rows
        if identity.get("identity_id")
    }
    logging.info("identity 拉取完成: %d 个", len(identities))
    return identities


def build_rows(
    report_rows: list[dict],
    ads: dict[str, dict],
    identities: dict[str, str | None],
    currency: str,
) -> list[dict]:
    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for item in report_rows:
        metrics, dimensions = item["metrics"], item["dimensions"]
        ad_id = dimensions["ad_id"]
        ad = ads.get(ad_id, {})
        identity_id = ad.get("identity_id")
        rows.append(
            {
                "dt": dimensions["stat_time_day"][:10],
                "advertiser_id": ADVERTISER_ID,
                "campaign_id": metrics.get("campaign_id"),
                "campaign_name": metrics.get("campaign_name"),
                "adgroup_id": metrics.get("adgroup_id"),
                "adgroup_name": metrics.get("adgroup_name"),
                "ad_id": ad_id,
                "ad_name": metrics.get("ad_name"),
                "store_id": ad.get("store_id"),
                "tiktok_item_id": ad.get("tiktok_item_id"),
                "identity_id": identity_id,
                "identity_type": ad.get("identity_type"),
                "identity_display_name": ad.get("identity_display_name"),
                "identity_username": identities.get(identity_id) if identity_id else None,
                **{name: to_float(metrics.get(name)) for name in FLOAT_METRICS},
                **{name: to_int(metrics.get(name)) for name in INT_METRICS},
                "currency": currency,
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
                COUNT(DISTINCT CONCAT(CAST(dt AS STRING), '|', advertiser_id, '|', ad_id)) AS unique_key_count,
                MIN(dt) AS min_dt,
                MAX(dt) AS max_dt,
                COUNTIF(currency IS NULL) AS null_currency_count
            FROM `{staging_table}`
            """
        ).result()
    )[0]
    if result.row_count == 0:
        raise RuntimeError("TikTok 广告投放日报校验失败：staging 无数据")
    if result.row_count != result.unique_key_count:
        raise RuntimeError("TikTok 广告投放日报校验失败：统计日、广告主和广告不唯一")
    if result.null_currency_count:
        raise RuntimeError("TikTok 广告投放日报校验失败：存在没有货币单位的金额行")
    if result.min_dt < start_date or result.max_dt > end_date:
        raise RuntimeError(
            f"TikTok 广告投放日报校验失败：统计日超出窗口 {result.min_dt} ~ {result.max_dt}"
        )


def replace_partitions(rows: list[dict], start_date: date, end_date: date) -> None:
    """先建 staging，再把目标表窗口外分区与 staging 合并成新表整体换名，避免对目标表做 DELETE。"""
    if not rows:
        logging.warning("本次 TikTok 广告投放日报无可写入记录，跳过表替换")
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

        # 用 load job 而不是 streaming insert：写完立即可查，且 staging 不带 streaming buffer 限制。
        # job_id_prefix 让 SDK 每次自带随机后缀，避免网络抖动触发内部重试时撞 409 Already Exists。
        client.load_table_from_json(
            rows,
            staging_table,
            job_id_prefix=f"tiktok_ods_{run_id}_",
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
            CLUSTER BY advertiser_id, store_id, ad_id AS
            SELECT * FROM `{target_table}`
            WHERE dt < DATE('{start_date}') OR dt > DATE('{end_date}')

            UNION ALL BY NAME

            SELECT * FROM `{staging_table}`
            """
        ).result()
        client.query(f"DROP TABLE `{target_table}`").result()
        client.query(f"ALTER TABLE `{changed_table}` RENAME TO {TABLE_NAME}").result()
        swapped = True
        logging.info("TikTok 广告投放日报替换完成: %s ~ %s, rows=%d", start_date, end_date, len(rows))
    finally:
        client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
        if not swapped:
            logging.warning("保留变更表用于排查: %s", changed_table)


def run(args: argparse.Namespace) -> None:
    start_date, end_date = get_date_window(args)

    session = requests.Session()
    session.headers["Access-Token"] = get_access_token()

    currency = fetch_currency(session)
    report_rows = fetch_report(session, start_date, end_date)
    ad_ids = sorted({item["dimensions"]["ad_id"] for item in report_rows})
    ads = fetch_ad_meta(session, ad_ids) if ad_ids else {}
    identities = fetch_identities(session) if ads else {}

    replace_partitions(build_rows(report_rows, ads, identities, currency), start_date, end_date)


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
