"""ADS 日级素材榜单：【AI-Fashion】素材内容数据看板 Top / Bottom 榜单。

落地 ``decom.ads_daily_ai_fashion_asset_rankings``。

依赖：
- decom.dws_asset_user_daily（曝光 / 试穿 / 负反馈用户事实）
- decom.dim_asset（模块、专栏、运营人员、上线时间等展示维度）

不限「新发布」，不限模块，全站一份榜：
- tryon_uv_top5：当日试穿 UV > 0 的素材，按试穿 UV 降序取前 5
- tryon_uv_bottom50：当日曝光 UV > 0 的素材，按试穿 UV 升序取前 50
- negative_feedback_rate_top50：当日曝光 UV > 0 的素材，按负反馈率降序取前 50
- product_click_uv_top5：当日素材下方商品点击 UV > 0 的素材，按商品点击 UV 降序取前 5
"""

from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_daily_ai_fashion_asset_rankings"
SOURCE_TABLE_NAME = "dws_asset_user_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "ads" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6
TRYON_UV_TOP_N = 5
PRODUCT_CLICK_UV_TOP_N = 5
BOTTOM_N = 50

client = bigquery.Client(project=PROJECT_ID)


def configure_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{datetime.now().strftime('%Y-%m-%d')}_{TABLE_NAME}.log"

    logger = logging.getLogger()
    logger.handlers.clear()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return log_path


def dates_to_sql_list(dates: list[date]) -> str:
    return ", ".join([f"'{d}'" for d in dates])


def target_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


def source_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{SOURCE_TABLE_NAME}"


def dim_asset_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.dim_asset"


def target_table_exists() -> bool:
    try:
        client.get_table(target_table_id())
        return True
    except NotFound:
        return False


def get_target_row_count() -> int:
    if not target_table_exists():
        return 0
    query = f"SELECT COUNT(*) AS row_count FROM `{target_table_id()}`"
    rows = list(client.query(query).result())
    return int(rows[0].row_count) if rows else 0


def get_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    素材榜单同时依赖 dws_asset_user_daily 与 dim_asset。

    规则：
    - 目标表不存在或为空：按 DWS 已落地历史分区全量初始化
    - 目标表已存在：合并
      - 最近 6 小时 DWS 变更的 dt
      - 最近 6 小时 dim_asset 变更所影响的历史 dt（通过目标表已有 asset_code 反查）
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM `{source_table_id()}`
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    dim_impact_sql = ""
    if target_table_exists():
        dim_impact_sql = f"""
        UNION DISTINCT
        SELECT DISTINCT t.dt
        FROM `{target_table_id()}` t
        INNER JOIN changed_assets a USING (asset_code)
        """

    query = f"""
    WITH
    -- 最近 6 小时 DWS 变更，对应直接受影响的业务日。
    changed_dws_dates AS (
        SELECT DISTINCT dt
        FROM `{source_table_id()}`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 最近 6 小时 dim_asset 修正，用于回推受影响历史分区。
    changed_assets AS (
        SELECT DISTINCT asset_code
        FROM `{dim_asset_table_id()}`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND asset_code IS NOT NULL
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt FROM changed_dws_dates
        {dim_impact_sql}
    )
    WHERE dt IS NOT NULL
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run(dates: list[date]) -> None:
    """
    素材日榜单：Top5 高试穿 UV / Bottom50 低试穿 UV / Top50 负反馈率。

    粒度：
    - dt × ranking_type × rank_no
    """
    if not dates:
        logging.info("没有新的 %s 日期需要处理", TABLE_NAME)
        return

    dates_str = dates_to_sql_list(dates)

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        ranking_type,
        rank_no,
        asset_code,
        asset_name,
        module,
        column_id,
        column_name,
        creator,
        publish_time,
        online_days,
        exposure_pv,
        exposure_uv,
        product_click_uv,
        tryon_count,
        tryon_uv,
        negative_feedback_pv,
        negative_feedback_uv,
        negative_feedback_rate,
        update_time
    )
    WITH
    -- 日级素材宽表：曝光 PV/UV、商品点击 UV、试穿次数/UV、负反馈 PV/UV。
    day_asset_agg AS (
        SELECT
            dt,
            asset_code,
            SUM(exposure_pv) AS exposure_pv,
            COUNT(DISTINCT IF(exposure_pv > 0, user_id, NULL)) AS exposure_uv,
            COUNT(DISTINCT IF(product_click_pv > 0, user_id, NULL)) AS product_click_uv,
            SUM(tryon_count) AS tryon_count,
            COUNT(DISTINCT IF(tryon_count > 0, user_id, NULL)) AS tryon_uv,
            SUM(negative_feedback_pv) AS negative_feedback_pv,
            COUNT(DISTINCT IF(negative_feedback_pv > 0, user_id, NULL)) AS negative_feedback_uv
        FROM `{source_table_id()}`
        WHERE dt IN ({dates_str})
        GROUP BY dt, asset_code
    ),
    -- 补齐展示维度：模块、专栏、运营人员、上线时间/天数。
    day_asset_wide AS (
        SELECT
            d.dt,
            d.asset_code,
            a.asset_name,
            a.module,
            a.column_id,
            a.column_name,
            a.creator,
            a.publish_time,
            DATE_DIFF(d.dt, a.publish_dt, DAY) AS online_days,
            d.exposure_pv,
            d.exposure_uv,
            d.product_click_uv,
            d.tryon_count,
            d.tryon_uv,
            d.negative_feedback_pv,
            d.negative_feedback_uv,
            SAFE_DIVIDE(d.negative_feedback_uv, d.exposure_uv) AS negative_feedback_rate
        FROM day_asset_agg d
        LEFT JOIN `{dim_asset_table_id()}` a
            USING (asset_code)
    ),
    -- 每日 Top5 高试穿 UV 素材。
    tryon_uv_top AS (
        SELECT
            *,
            'tryon_uv_top5' AS ranking_type,
            ROW_NUMBER() OVER (
                PARTITION BY dt
                ORDER BY tryon_uv DESC, exposure_uv DESC, asset_code
            ) AS rank_no
        FROM day_asset_wide
        WHERE tryon_uv > 0
        QUALIFY rank_no <= {TRYON_UV_TOP_N}
    ),
    -- 每日 Top5 高商品点击 UV 素材。
    product_click_uv_top AS (
        SELECT
            *,
            'product_click_uv_top5' AS ranking_type,
            ROW_NUMBER() OVER (
                PARTITION BY dt
                ORDER BY product_click_uv DESC, exposure_uv DESC, asset_code
            ) AS rank_no
        FROM day_asset_wide
        WHERE product_click_uv > 0
        QUALIFY rank_no <= {PRODUCT_CLICK_UV_TOP_N}
    ),
    -- 试穿 UV 最低的素材 Bottom50（候选池：当日有曝光的素材）。
    tryon_uv_bottom AS (
        SELECT
            *,
            'tryon_uv_bottom50' AS ranking_type,
            ROW_NUMBER() OVER (
                PARTITION BY dt
                ORDER BY tryon_uv ASC, exposure_uv DESC, asset_code
            ) AS rank_no
        FROM day_asset_wide
        WHERE exposure_uv > 0
        QUALIFY rank_no <= {BOTTOM_N}
    ),
    -- 负反馈率最高的素材 Top50（候选池：当日有曝光的素材）。
    negative_feedback_rate_top AS (
        SELECT
            *,
            'negative_feedback_rate_top50' AS ranking_type,
            ROW_NUMBER() OVER (
                PARTITION BY dt
                ORDER BY negative_feedback_rate DESC, negative_feedback_uv DESC, asset_code
            ) AS rank_no
        FROM day_asset_wide
        WHERE exposure_uv > 0
        QUALIFY rank_no <= {BOTTOM_N}
    )
    SELECT
        dt,
        ranking_type,
        rank_no,
        asset_code,
        asset_name,
        module,
        column_id,
        column_name,
        creator,
        publish_time,
        online_days,
        exposure_pv,
        exposure_uv,
        product_click_uv,
        tryon_count,
        tryon_uv,
        negative_feedback_pv,
        negative_feedback_uv,
        negative_feedback_rate,
        CURRENT_TIMESTAMP() AS update_time
    FROM (
        SELECT * FROM tryon_uv_top
        UNION ALL
        SELECT * FROM product_click_uv_top
        UNION ALL
        SELECT * FROM tryon_uv_bottom
        UNION ALL
        SELECT * FROM negative_feedback_rate_top
    )
    ORDER BY dt, ranking_type, rank_no;
    """
    logging.info("开始处理: %s", TABLE_NAME)
    client.query(query).result()
    logging.info("%s 刷新完成, 处理日期: %s", TABLE_NAME, dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 %s 日期需要处理", TABLE_NAME)
        raise SystemExit(0)

    run(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
