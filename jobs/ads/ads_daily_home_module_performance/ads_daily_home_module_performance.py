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
MODULE_MAP_SQL = """
SELECT 'star' AS module, 'v_home_star' AS exposure_event, 'c_home_star' AS click_event
UNION ALL
SELECT 'magazine' AS module, 'v_home_magazine' AS exposure_event, 'c_home_magazine' AS click_event
UNION ALL
SELECT 'brand' AS module, 'v_home_brand' AS exposure_event, 'c_home_brand' AS click_event
UNION ALL
SELECT 'feeds' AS module, 'v_home_feeds' AS exposure_event, 'c_home_feeds' AS click_event
""".strip()
client = bigquery.Client(project=PROJECT_ID)


def dates_to_array_sql(dates):
    return ", ".join([f"DATE '{d}'" for d in dates])


def run_ads_daily_home_module_performance(dates):
    """
    首页模块表现日报（每日每模块一行）:
    - module_exposure_uv: 模块曝光 UV
    - module_click_uv: 模块点击 UV
    - click_rate: 点击率（点击 UV / 曝光 UV）
    - click_pv: 模块点击 PV（按 raw_event_id 去重）
    - avg_click_count_per_user: 点击人均次数（点击 PV / 点击 UV）
    使用多伦多时间（America/Toronto）。
    """
    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    min_date = min(dates)
    max_date = max(dates)
    event_ts = event_ts_expr()

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_home_module_performance`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ads_daily_home_module_performance`
    (dt, module, module_exposure_uv, module_click_uv,
     click_rate, click_pv, avg_click_count_per_user, update_time)
    WITH
    module_map AS (
        {MODULE_MAP_SQL}
    ),
    base AS (
        SELECT
            dt,
            event_name,
            prop_user_id,
            prop_device_id,
            raw_event_id
        FROM (
            SELECT
                DATE({event_ts}, '{TORONTO_TZ}') AS dt,
                event_name,
                prop_user_id,
                prop_device_id,
                raw_event_id
            FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
            WHERE DATE(logAt_timestamp) BETWEEN DATE_SUB(DATE '{min_date}', INTERVAL 1 DAY)
                AND DATE_ADD(DATE '{max_date}', INTERVAL 1 DAY)
                AND event_name IN (
                    SELECT exposure_event AS event FROM module_map
                    UNION DISTINCT
                    SELECT click_event AS event FROM module_map
                )
        )
        WHERE dt IN ({dates_str})
    ),
    events AS (
        SELECT
            b.dt,
            COALESCE(NULLIF(b.prop_user_id, ''), NULLIF(b.prop_device_id, '')) AS visitor_id,
            m.module,
            b.event_name = m.exposure_event AS is_exposure,
            b.event_name = m.click_event AS is_click,
            b.raw_event_id
        FROM base b
        INNER JOIN module_map m
            ON b.event_name IN (m.exposure_event, m.click_event)
    ),
    date_list AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    metric_keys AS (
        SELECT d.dt, m.module
        FROM date_list d
        CROSS JOIN (SELECT module FROM module_map) m
    ),
    metric_daily AS (
        SELECT
            dt,
            module,
            COUNT(DISTINCT IF(is_exposure, visitor_id, NULL)) AS module_exposure_uv,
            COUNT(DISTINCT IF(is_click, visitor_id, NULL)) AS module_click_uv,
            COUNT(DISTINCT IF(is_click, raw_event_id, NULL)) AS click_pv
        FROM events
        GROUP BY dt, module
    )
    SELECT
        k.dt,
        k.module,
        COALESCE(m.module_exposure_uv, 0) AS module_exposure_uv,
        COALESCE(m.module_click_uv, 0) AS module_click_uv,
        CAST(CASE
            WHEN COALESCE(m.module_exposure_uv, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(m.module_click_uv, 0) AS NUMERIC)
                / CAST(COALESCE(m.module_exposure_uv, 0) AS NUMERIC),
                4
            )
        END AS NUMERIC) AS click_rate,
        COALESCE(m.click_pv, 0) AS click_pv,
        CAST(CASE
            WHEN COALESCE(m.module_click_uv, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(m.click_pv, 0) AS NUMERIC)
                / CAST(COALESCE(m.module_click_uv, 0) AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_click_count_per_user,
        CURRENT_TIMESTAMP() AS update_time
    FROM metric_keys k
    LEFT JOIN metric_daily m
        ON k.dt = m.dt AND k.module = m.module;
    """
    logging.info("开始处理: ads_daily_home_module_performance")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_home_module_performance 刷新完成, 处理日期: {dates}")
