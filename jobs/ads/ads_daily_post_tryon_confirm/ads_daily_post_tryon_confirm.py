import logging
from pathlib import Path
import sys

from google.cloud import bigquery

ADS_ROOT = Path(__file__).resolve().parent.parent
if str(ADS_ROOT) not in sys.path:
    sys.path.insert(0, str(ADS_ROOT))

from ads_daily_content_performance.ads_daily_content_performance import (
    dates_to_sql_list,
    event_ts_expr,
)

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
V3_DATASET_ID = "v3_decom"
TORONTO_TZ = "America/Toronto"

client = bigquery.Client(project=PROJECT_ID)


def run_ads_daily_post_tryon_confirm(dates):
    """
    穿搭工具“点击使用”日汇总（日×帖子）:
    粒度：dt × post_id

    指标：
    - click_use_pv: 点击使用次数（PV，按 raw_event_id 去重）
    - click_use_uv: 点击使用用户数（UV，visitor_id 去重）
    - avg_click_use_count_per_user: 人均点击使用次数（PV / UV）

    事件：c_tryon_confirm
    时区：dt 使用 America/Toronto（多伦多时间）
    丢弃：post_code 为空的事件舍弃；post_code 无法映射到 post_id 的事件也舍弃。
    维度映射：post_code -> creator 使用 v3_decom.community_post.creator 原值。
    """
    dates_str = dates_to_sql_list(dates)
    event_ts = event_ts_expr()

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_post_tryon_confirm`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ads_daily_post_tryon_confirm`
    (dt, post_id, post_code, post_name, creator,
     click_use_pv, click_use_uv, avg_click_use_count_per_user, update_time)
    WITH
    base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            COALESCE(NULLIF(prop_user_id, ''), NULLIF(prop_device_id, '')) AS visitor_id,
            raw_event_id,
            post_code
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND event_name = 'c_tryon_confirm'
    ),
    events_dedup AS (
        SELECT
            dt,
            raw_event_id,
            post_code,
            ANY_VALUE(visitor_id) AS visitor_id
        FROM base
        WHERE raw_event_id IS NOT NULL
            AND visitor_id IS NOT NULL
            AND post_code IS NOT NULL
        GROUP BY dt, raw_event_id, post_code
    ),
    post_map AS (
        SELECT
            CAST(post_code AS STRING) AS post_code,
            ANY_VALUE(SAFE_CAST(id AS INT64)) AS post_id,
            ANY_VALUE(title) AS post_name,
            ANY_VALUE(CAST(creator AS STRING)) AS creator
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_post`
        WHERE post_code IS NOT NULL
        GROUP BY post_code
    ),
    events_enriched AS (
        SELECT
            e.dt,
            pm.post_id,
            e.post_code,
            pm.post_name,
            pm.creator,
            e.raw_event_id,
            e.visitor_id
        FROM events_dedup e
        INNER JOIN post_map pm USING (post_code)
        WHERE pm.post_id IS NOT NULL
    ),
    daily AS (
        SELECT
            dt,
            post_id,
            post_code,
            post_name,
            creator,
            COUNT(*) AS click_use_pv,
            COUNT(DISTINCT visitor_id) AS click_use_uv
        FROM events_enriched
        GROUP BY dt, post_id, post_code, post_name, creator
    )
    SELECT
        dt,
        post_id,
        post_code,
        post_name,
        creator,
        click_use_pv,
        click_use_uv,
        CAST(CASE
            WHEN click_use_uv = 0 THEN 0
            ELSE ROUND(
                CAST(click_use_pv AS NUMERIC) / CAST(click_use_uv AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_click_use_count_per_user,
        CURRENT_TIMESTAMP() AS update_time
    FROM daily;
    """

    logging.info("开始处理: ads_daily_post_tryon_confirm")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_post_tryon_confirm 刷新完成, 处理日期: {dates}")
