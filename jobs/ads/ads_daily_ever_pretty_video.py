"""Ever Pretty 本地 Demo。默认只查询；--write 创建新表，--rebuild-demo 显式重建 Demo。

复用 DIM / DWS，只做名单筛选、展示投影和相邻记录差值。
不调用主 ETL，不刷新上游；重建先校验完整性并保留七天备份。
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
import uuid
from zoneinfo import ZoneInfo

from google.auth import load_credentials_from_dict
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

ROOT = Path(__file__).resolve().parents[2]
PROJECT = "my-project-8584-jetonai"
TABLE = f"{PROJECT}.decom.ads_daily_ever_pretty_video"
MAX_BYTES = 1_000_000_000


def build_client():
    # 使用显式凭据时尊重 ADC；用户 ADC 的 quota project 只在内存中去除。
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return bigquery.Client(project=PROJECT)
    adc = Path.home() / ".config/gcloud/application_default_credentials.json"
    if adc.exists():
        info = json.loads(adc.read_text())
        info.pop("quota_project_id", None)
        credentials, _ = load_credentials_from_dict(
            info, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        return bigquery.Client(project=PROJECT, credentials=credentials)
    return bigquery.Client(project=PROJECT)


def load_config():
    data = json.loads((ROOT / "env/ever_pretty_accounts.json").read_text())
    accounts = data["accounts"]
    assert len(accounts) == 9 and len({a["blogger_id"] for a in accounts}) == 8
    assert len({(a["channel"], a["creator_id"]) for a in accounts}) == 9
    assert sum(a["channel"] == "tiktok" for a in accounts) == 6
    assert sum(a["channel"] == "youtube" for a in accounts) == 3
    return data


def validate_rows(rows, accounts, start_date, end_date):
    if not rows:
        raise ValueError("查询结果为空，拒绝创建表")
    allowed = {(a["channel"], a["creator_id"]) for a in accounts}
    keys = set()
    by_video = {}
    for r in rows:
        key = (str(r["dt"]), r["video_key"])
        if key in keys:
            raise ValueError(f"主键重复: {key}")
        keys.add(key)
        for f in ("dt", "video_key", "video_id", "blogger_id", "blogger_name", "creator_id", "channel", "published_dt", "collection_status"):
            if r[f] is None or r[f] == "":
                raise ValueError(f"必要字段缺失: {f}")
        if (r["channel"], r["creator_id"]) not in allowed:
            raise ValueError("结果包含名单外账号")
        dt, published = date.fromisoformat(str(r["dt"])), date.fromisoformat(str(r["published_dt"]))
        if not start_date <= published <= dt <= end_date:
            raise ValueError("发布日期或统计日期越界")
        if r["has_snapshot"] and not r["state_available"]:
            raise ValueError("快照已到但状态尚未到，等待上游完成后再建表")
        if r["last_snapshot_dt"] is None and any(r[f] is not None for f in ("total_views", "total_likes", "total_comments", "total_shares")):
            raise ValueError("尚无采集不能输出零值")
        if r["channel"] == "youtube" and (r["total_shares"] is not None or r["share_increment"] is not None):
            raise ValueError("YouTube 分享必须为空")
        by_video.setdefault(r["video_key"], []).append(r)
    for video_rows in by_video.values():
        video_rows.sort(key=lambda r: str(r["dt"]))
        expected = (end_date - date.fromisoformat(str(video_rows[0]["published_dt"]))).days + 1
        if len(video_rows) != expected:
            raise ValueError("视频逐日覆盖不完整")
        prev = None
        for row in video_rows:
            for total, increment in [("total_views", "view_increment"), ("total_likes", "like_increment"), ("total_comments", "comment_increment"), ("total_shares", "share_increment")]:
                delta = row[total] - prev[total] if prev is not None and row[total] is not None and prev[total] is not None else None
                if row[increment] != delta:
                    raise ValueError("增量未按相邻日已知记录计算")
            prev = row


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--end-date", type=date.fromisoformat)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--write", action="store_true", help="校验后仅创建新表；已经存在则拒绝")
    parser.add_argument("--rebuild-demo", action="store_true", help="显式重建本 Demo 表；先校验并保存七天备份")
    parser.add_argument("--output", type=Path, help="保存查询结果用于本地核验")
    args = parser.parse_args()
    if sum((args.write, args.rebuild_demo, args.dry_run)) > 1:
        parser.error("--write、--rebuild-demo、--dry-run 不能同时使用")
    config = load_config()
    start = date.fromisoformat(config["start_date"])
    today = datetime.now(ZoneInfo(config["timezone"])).date()
    end = args.end_date or today
    if not start <= end <= today:
        parser.error("统计结束日期须介于起始日期和美东当天之间")
    client = build_client()
    parameters = [
        bigquery.ScalarQueryParameter("accounts_json", "STRING", json.dumps(config["accounts"])),
        bigquery.ScalarQueryParameter("start_date", "DATE", start),
        bigquery.ScalarQueryParameter("end_date", "DATE", end),
    ]
    sql = Path(__file__).with_suffix(".sql").read_text()
    dry = client.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=parameters, dry_run=True, use_query_cache=False,
        maximum_bytes_billed=MAX_BYTES,
    ))
    print(json.dumps({"estimated_bytes": dry.total_bytes_processed, "start_date": str(start), "end_date": str(end)}))
    if args.dry_run:
        return
    staging = None
    promoted = False
    target_removed = False
    backup = None
    job_config = bigquery.QueryJobConfig(query_parameters=parameters, maximum_bytes_billed=MAX_BYTES)
    if args.write or args.rebuild_demo:
        try:
            client.get_table(TABLE)
        except NotFound:
            if args.rebuild_demo:
                raise RuntimeError("Demo 表不存在，请使用 --write 创建")
        else:
            if not args.rebuild_demo:
                raise RuntimeError("目标表已存在，请显式使用 --rebuild-demo 重建本 Demo；不会刷新上游")
        staging = f"{TABLE}_tmp_{uuid.uuid4().hex[:12]}"
        job_config.destination = staging
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
        job_config.time_partitioning = bigquery.TimePartitioning(field="dt")
        job_config.clustering_fields = ["blogger_name", "channel", "creator_id", "video_key"]
    try:
        job = client.query(sql, job_config=job_config)
        rows = [dict(r) for r in job.result()]
        validate_rows(rows, config["accounts"], start, end)
        if args.write or args.rebuild_demo:
            if args.rebuild_demo:
                old_keys = {(str(r['dt']), r['video_key']) for r in client.query(
                    f"SELECT dt, video_key FROM `{TABLE}`",
                    job_config=bigquery.QueryJobConfig(maximum_bytes_billed=MAX_BYTES),
                ).result()}
                new_keys = {(str(r['dt']), r['video_key']) for r in rows}
                if not old_keys.issubset(new_keys):
                    raise RuntimeError("重建将丢失既有视频日记录，拒绝替换")
                backup = f"{TABLE}_backup_{uuid.uuid4().hex[:12]}"
                client.copy_table(TABLE, backup, job_config=bigquery.CopyJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_EMPTY)).result()
                backup_table = client.get_table(backup)
                backup_table.expires = datetime.now(timezone.utc) + timedelta(days=7)
                client.update_table(backup_table, ["expires"])
                if backup_table.num_rows != len(old_keys):
                    raise RuntimeError("备份行数校验失败，拒绝替换")
                client.delete_table(TABLE)
                target_removed = True
            # staging 校验通过后换成目标名；重建时旧表已备份。
            try:
                client.query(f"ALTER TABLE `{staging}` RENAME TO ads_daily_ever_pretty_video").result()
            except Exception:
                if target_removed and backup:
                    client.copy_table(backup, TABLE, job_config=bigquery.CopyJobConfig(
                        write_disposition=bigquery.WriteDisposition.WRITE_EMPTY)).result()
                    target_removed = False
                raise
            promoted = True
            target_removed = False
            table = client.get_table(TABLE)
            table.description = "Ever Pretty 8博主9账号视频逐日记录；America/Toronto；首次采集前为空，增量为记录差值。Demo 尚未接入生产调度。"
            client.update_table(table, ["description"])
            verified = [dict(r) for r in client.query(
                f"SELECT * FROM `{TABLE}` WHERE dt BETWEEN @start_date AND @end_date",
                job_config=bigquery.QueryJobConfig(query_parameters=parameters, maximum_bytes_billed=MAX_BYTES),
            ).result()]
            validate_rows(verified, config["accounts"], start, end)
            if len(verified) != len(rows):
                raise RuntimeError("落表行数与校验结果不同")
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps({"rows": rows, "config": config}, default=str, ensure_ascii=False, indent=2))
        print(json.dumps({"table": TABLE if promoted else None, "backup": backup, "rows": len(rows), "videos": len({r['video_key'] for r in rows}), "bytes_processed": job.total_bytes_processed}, ensure_ascii=False))
    finally:
        if staging and not promoted and not target_removed:
            # 仅清理本次唯一 staging，不碰目标或其他任务临时表。
            client.delete_table(staging, not_found_ok=True)


if __name__ == "__main__":
    main()
