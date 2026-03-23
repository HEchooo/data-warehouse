import logging
from pathlib import Path
import sys

from google.cloud import bigquery

ADS_ROOT = Path(__file__).resolve().parent
if str(ADS_ROOT) not in sys.path:
    sys.path.insert(0, str(ADS_ROOT))

from ads_daily_content_performance import dates_to_sql_list, event_ts_expr

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"

client = bigquery.Client(project=PROJECT_ID)


def dates_to_array_sql(dates):
    return ", ".join([f"DATE '{d}'" for d in dates])


def run_ads_daily_tryon_confirm(dates):
    """
    穿搭工具“点击使用”日汇总（每日一行）:
    - click_use_pv: 点击使用次数（PV，按 raw_event_id 去重）
    - click_use_uv: 点击使用用户数（UV，visitor_id 去重）
    - avg_click_use_count_per_user: 人均点击使用次数（PV / UV）

    事件：c_tryon_confirm
    时区：dt 使用 America/Toronto（多伦多时间）
    说明：仅统计包含来源帖子（post_code）的数据；不包含 post_code 的事件舍弃。
    """
    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    event_ts = event_ts_expr()

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_tryon_confirm`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ads_daily_tryon_confirm`
    (dt, click_use_pv, click_use_uv, avg_click_use_count_per_user, update_time)
    WITH
    base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            event_name,
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
            ANY_VALUE(visitor_id) AS visitor_id
        FROM base
        WHERE raw_event_id IS NOT NULL
            AND visitor_id IS NOT NULL
            AND post_code IS NOT NULL
        GROUP BY dt, raw_event_id
    ),
    daily AS (
        SELECT
            dt,
            COUNT(*) AS click_use_pv,
            COUNT(DISTINCT visitor_id) AS click_use_uv
        FROM events_dedup
        GROUP BY dt
    ),
    date_list AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    )
    SELECT
        d.dt,
        COALESCE(m.click_use_pv, 0) AS click_use_pv,
        COALESCE(m.click_use_uv, 0) AS click_use_uv,
        CAST(CASE
            WHEN COALESCE(m.click_use_uv, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(m.click_use_pv, 0) AS NUMERIC)
                / CAST(COALESCE(m.click_use_uv, 0) AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_click_use_count_per_user,
        CURRENT_TIMESTAMP() AS update_time
    FROM date_list d
    LEFT JOIN daily m USING (dt);
    """

    logging.info("开始处理: ads_daily_tryon_confirm")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_tryon_confirm 刷新完成, 处理日期: {dates}")
