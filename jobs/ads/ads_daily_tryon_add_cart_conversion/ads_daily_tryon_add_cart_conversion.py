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
TORONTO_TZ = "America/Toronto"

client = bigquery.Client(project=PROJECT_ID)


def dates_to_array_sql(dates):
    return ", ".join([f"DATE '{d}'" for d in dates])


def run_ads_daily_tryon_add_cart_conversion(dates):
    """
    试穿结果转化日报（每日一行）:
    - tryon_uv: 试穿用户数（UV，visitor_id 去重）
    - add_cart_uv: 加购用户数（UV，visitor_id 去重；全量加购UV，不做试穿归因）
    - add_cart_rate: 加购率（add_cart_uv / tryon_uv；当 tryon_uv=0 时为 0；可大于 1）

    事件：c_tryon、c_add_cart
    时区：dt 使用 America/Toronto（多伦多时间）

    去重说明：dwd_event_log 会展开 args_* 导致重复行，先用
    action_event_id = COALESCE(raw_event_id, hash_id) 去重后再聚合。
    """
    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    event_ts = event_ts_expr()

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_tryon_add_cart_conversion`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ads_daily_tryon_add_cart_conversion`
    (dt, tryon_uv, add_cart_uv, add_cart_rate, update_time)
    WITH
    base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            event_name,
            COALESCE(NULLIF(prop_user_id, ''), NULLIF(prop_device_id, '')) AS visitor_id,
            raw_event_id,
            hash_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
            AND event_name IN ('c_tryon', 'c_add_cart')
    ),
    action_events AS (
        SELECT
            dt,
            event_name,
            COALESCE(raw_event_id, hash_id) AS action_event_id,
            ANY_VALUE(visitor_id) AS visitor_id
        FROM base
        WHERE visitor_id IS NOT NULL
            AND COALESCE(raw_event_id, hash_id) IS NOT NULL
        GROUP BY dt, event_name, action_event_id
    ),
    daily AS (
        SELECT
            dt,
            COUNT(DISTINCT IF(event_name = 'c_tryon', visitor_id, NULL)) AS tryon_uv,
            COUNT(DISTINCT IF(event_name = 'c_add_cart', visitor_id, NULL)) AS add_cart_uv
        FROM action_events
        GROUP BY dt
    ),
    date_list AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    )
    SELECT
        d.dt,
        COALESCE(m.tryon_uv, 0) AS tryon_uv,
        COALESCE(m.add_cart_uv, 0) AS add_cart_uv,
        CAST(CASE
            WHEN COALESCE(m.tryon_uv, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(m.add_cart_uv, 0) AS NUMERIC)
                / CAST(COALESCE(m.tryon_uv, 0) AS NUMERIC),
                4
            )
        END AS NUMERIC) AS add_cart_rate,
        CURRENT_TIMESTAMP() AS update_time
    FROM date_list d
    LEFT JOIN daily m USING (dt);
    """

    logging.info("开始处理: ads_daily_tryon_add_cart_conversion")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_tryon_add_cart_conversion 刷新完成, 处理日期: {dates}")
