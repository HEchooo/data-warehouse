"""通过本地 Superset REST API 配置和验收 Ever Pretty Demo。

地址固定 localhost，避免误改线上实例。先读 OpenAPI 和资源现状，再创建/更新。
"""
from __future__ import annotations

from datetime import date, timedelta
import json
import os
from pathlib import Path
from urllib.parse import urlencode

import requests

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "logs/ever_pretty_demo"
BASE = "http://127.0.0.1:8089"
TABLE_NAME = "ads_daily_ever_pretty_video"


def main():
    session = requests.Session()
    login = json.loads((RUNTIME / "superset/login.json").read_text())
    auth = session.post(BASE + "/api/v1/security/login", json={**login, "provider": "db", "refresh": True}, timeout=30)
    auth.raise_for_status()
    session.headers["Authorization"] = "Bearer " + auth.json()["access_token"]

    def api(method, path, payload=None, params=None):
        result = session.request(method, BASE + path, json=payload, params=params, timeout=120)
        if not result.ok:
            # 不打印请求体，其中可能含连接配置。
            raise RuntimeError(f"{method} {path}: HTTP {result.status_code}: {result.text[:1200]}")
        return result.json()

    spec = api("GET", "/api/v1/_openapi")
    for path in ("/api/v1/database/", "/api/v1/dataset/", "/api/v1/chart/", "/api/v1/dashboard/", "/api/v1/chart/data"):
        assert "post" in spec["paths"][path]
    # 受保护资源可读，确认本地身份有效；部分4.0.2实例的 me 接口不支持JWT。
    api("GET", "/api/v1/database/")
    session.headers["X-CSRFToken"] = api("GET", "/api/v1/security/csrf_token/")["result"]
    session.headers["Referer"] = BASE + "/"

    def listing(resource):
        rows, page = [], 0
        while True:
            result = api("GET", f"/api/v1/{resource}/", params={"q": json.dumps({"page": page, "page_size": 100})})
            rows.extend(result["result"])
            if len(rows) >= result["count"]:
                return rows
            page += 1

    def upsert(resource, payload, matches):
        found = [row for row in listing(resource) if matches(row)]
        if len(found) > 1:
            raise RuntimeError(f"本地 {resource} 存在重名，拒绝猜测")
        if found:
            ident = found[0]["id"]
            api("GET", f"/api/v1/{resource}/{ident}")
            api("PUT", f"/api/v1/{resource}/{ident}", payload)
        else:
            ident = api("POST", f"/api/v1/{resource}/", payload)["id"]
        api("GET", f"/api/v1/{resource}/{ident}")
        return ident

    credential = Path(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", Path.home() / ".config/ecn-bi-demo/bigquery-service-account.json"))
    if not credential.is_file():
        raise RuntimeError("请通过 GOOGLE_APPLICATION_CREDENTIALS 指向仓库外的 BigQuery 凭据")
    uri = "bigquery://my-project-8584-jetonai/decom?" + urlencode({
        "credentials_path": str(credential), "location": "us-east4", "maximum_bytes_billed": 1000000000,
    })
    db = upsert("database", {
        "database_name": "ECN BigQuery · Demo",
        "sqlalchemy_uri": uri, "allow_dml": False, "allow_ctas": False,
        "allow_cvas": False, "allow_file_upload": False, "expose_in_sqllab": False,
        "extra": json.dumps({"engine_params": {}, "metadata_params": {}}),
    }, lambda x: x["database_name"] == "ECN BigQuery · Demo")
    matches = [r for r in listing("dataset") if r["table_name"] == TABLE_NAME and r["schema"] == "decom"]
    if matches:
        ds = matches[0]["id"]
    else:
        ds = api("POST", "/api/v1/dataset/", {"database": db, "schema": "decom", "table_name": TABLE_NAME})["id"]
    dataset = api("GET", f"/api/v1/dataset/{ds}")["result"]
    labels = {
        "dt": "统计日期（美东）", "blogger_name": "AI 博主", "channel": "平台",
        "video_id": "视频 ID", "video_title": "视频标题", "video_link": "视频链接",
        "account_handle": "平台账号", "published_dt": "发布日期", "published_time_basis": "发布时间依据",
        "total_views": "累计播放", "view_increment": "播放记录增量", "total_likes": "累计点赞",
        "like_increment": "点赞记录增量", "total_comments": "累计评论", "comment_increment": "评论记录增量",
        "total_shares": "累计分享", "share_increment": "分享记录增量", "collection_status": "采集情况",
        "last_snapshot_dt": "最近采集日", "is_complete_day": "完整自然日",
    }
    columns = [{"id": c["id"], "column_name": c["column_name"],
        "verbose_name": labels.get(c["column_name"], c["column_name"]),
        "is_dttm": c["column_name"] in {"dt", "published_dt", "published_ts_utc", "last_snapshot_dt", "update_time", "state_updated_at", "dim_updated_at"},
        "filterable": True, "groupby": True} for c in dataset["columns"]]
    api("PUT", f"/api/v1/dataset/{ds}", {"main_dttm_col": "dt", "filter_select_enabled": True,
        "columns": columns, "cache_timeout": 300,
        "description": "Ever Pretty：一个平台视频×一个美东统计日。累计不跨日相加，记录差值不等于缺采期间的真实当日新增。"})
    dashboard = upsert("dashboard", {"dashboard_title": "Ever Pretty · 视频每日数据", "slug": "ever-pretty-video-daily", "published": True}, lambda x: x.get("slug") == "ever-pretty-video-daily")
    preview = json.loads((RUNTIME / "preview.json").read_text())
    max_dt = max(row["dt"] for row in preview["rows"])
    until = str(date.fromisoformat(max_dt) + timedelta(days=1))
    time_range = f"{preview['config']['start_date']} : {until}"
    from ever_pretty_trends import configure_trends
    configure_trends(api, upsert, ds, dashboard, preview, time_range, max_dt, RUNTIME)


if __name__ == "__main__":
    main()
