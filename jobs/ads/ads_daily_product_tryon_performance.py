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


def clean_sku_expr(field_name: str) -> str:
    """
    dwd_event_log 的 args_sku 有时携带多层双引号/转义：
    - '"123"'、'""123""'、'\\"123\\"'
    这里统一去掉首尾的双引号与反斜杠，并将 '' / 'null'（大小写不敏感）归一为 NULL。
    """
    # BigQuery 标准 SQL：使用 raw string literal，确保反斜杠不被当作转义序列吞掉
    # r'\"' 的字符集包含：反斜杠 + 双引号
    cleaned = f"TRIM(TRIM(CAST({field_name} AS STRING)), r'\\\"')"
    return f"NULLIF(CASE WHEN LOWER({cleaned}) = 'null' THEN NULL ELSE {cleaned} END, '')"


def run_ads_daily_product_tryon_performance(dates):
    """
    商详页试穿维度（日期 × SPU × SKU）:
    - exposure_uv：商品曝光UV（包含商详页 + 首页Feed流 两个场景）
    - tryon_total_count：商品被试穿次数（PV，仅商详页触发，c_tryon 且 args_page_key='product_detail'）
      PV 按 action_event_id=COALESCE(raw_event_id, hash_id) 去重，避免 dwd_event_log 展开导致放大
    - add_cart_uv：商品加购UV（全量加购UV，c_add_cart，visitor_id 去重）

    时区：dt 使用 America/Toronto（多伦多时间）
    """
    dates_str = dates_to_sql_list(dates)
    event_ts = event_ts_expr()
    sku_sql = clean_sku_expr("args_sku")

    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_product_tryon_performance`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ads_daily_product_tryon_performance`
    (dt, spu, sku, exposure_uv, tryon_total_count, add_cart_uv, update_time)
    WITH
    base AS (
        SELECT
            dt,
            event_name,
            visitor_id,
            action_event_id,
            spu,
            COALESCE(sku, '__NULL__') AS sku_key,
            page_key
        FROM (
            SELECT
                DATE({event_ts}, '{TORONTO_TZ}') AS dt,
                event_name,
                COALESCE(NULLIF(prop_user_id, ''), NULLIF(prop_device_id, '')) AS visitor_id,
                COALESCE(raw_event_id, hash_id) AS action_event_id,
                NULLIF(CAST(product_code AS STRING), '') AS spu,
                {sku_sql} AS sku,
                CAST(args_page_key AS STRING) AS page_key
            FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
            WHERE DATE({event_ts}, '{TORONTO_TZ}') IN ({dates_str})
                AND event_name IN ('v_product_detail', 'v_home_feeds', 'c_tryon', 'c_add_cart')
        )
    ),
    exposure_daily AS (
        SELECT
            dt,
            spu,
            sku_key,
            COUNT(DISTINCT visitor_id) AS exposure_uv
        FROM base
        WHERE event_name IN ('v_product_detail', 'v_home_feeds')
            AND spu IS NOT NULL
            AND visitor_id IS NOT NULL
        GROUP BY dt, spu, sku_key
    ),
    tryon_product_detail AS (
        SELECT
            dt,
            spu,
            sku_key,
            COUNT(DISTINCT action_event_id) AS tryon_total_count
        FROM base
        WHERE event_name = 'c_tryon'
            AND page_key = 'product_detail'
            AND spu IS NOT NULL
            AND sku_key != '__NULL__'
            AND action_event_id IS NOT NULL
        GROUP BY dt, spu, sku_key
    ),
    add_cart AS (
        SELECT
            dt,
            spu,
            sku_key,
            COUNT(DISTINCT IF(visitor_id IS NOT NULL, visitor_id, NULL)) AS add_cart_uv
        FROM base
        WHERE event_name = 'c_add_cart'
            AND spu IS NOT NULL
            AND sku_key != '__NULL__'
        GROUP BY dt, spu, sku_key
    ),
    keys AS (
        SELECT dt, spu, sku_key FROM exposure_daily
        UNION DISTINCT
        SELECT dt, spu, sku_key FROM tryon_product_detail
        UNION DISTINCT
        SELECT dt, spu, sku_key FROM add_cart
    )
    SELECT
        k.dt,
        k.spu,
        NULLIF(k.sku_key, '__NULL__') AS sku,
        COALESCE(e.exposure_uv, 0) AS exposure_uv,
        COALESCE(t.tryon_total_count, 0) AS tryon_total_count,
        COALESCE(a.add_cart_uv, 0) AS add_cart_uv,
        CURRENT_TIMESTAMP() AS update_time
    FROM keys k
    LEFT JOIN exposure_daily e USING (dt, spu, sku_key)
    LEFT JOIN tryon_product_detail t USING (dt, spu, sku_key)
    LEFT JOIN add_cart a USING (dt, spu, sku_key);
    """

    logging.info("开始处理: ads_daily_product_tryon_performance")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_product_tryon_performance 刷新完成, 处理日期: {dates}")
