from datetime import date, datetime
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_daily_column_performance"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "ads" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6

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


def target_table_exists() -> bool:
    try:
        client.get_table(target_table_id())
        return True
    except NotFound:
        return False


def get_target_row_count() -> int:
    if not target_table_exists():
        return 0

    query = f"""
    SELECT COUNT(*) AS row_count
    FROM `{target_table_id()}`
    """
    rows = list(client.query(query).result())
    return int(rows[0].row_count) if rows else 0


def get_column_performance_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    专栏主题 ADS 只消费中层事实和维表修正，待处理日期必须单独判断。

    规则：
    - 目标表不存在或为空时，按行为事实 + 快照事实已落地的所有日期初始化
    - 目标表已存在时，读取最近 6 小时内 DWS 变更的 dt
    - 同时读取最近 6 小时内 dim_column 变更影响到目标表的历史 dt
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM (
            SELECT dt
            FROM `{PROJECT_ID}.{DATASET_ID}.dws_column_interaction_daily`

            UNION DISTINCT

            SELECT dt
            FROM `{PROJECT_ID}.{DATASET_ID}.dws_column_post_daily`
        )
        WHERE dt IS NOT NULL
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    WITH
    -- 最近 6 小时写入的专栏行为 DWS 分区，直接对应需要重算的 ADS 日期。
    changed_interaction_dates AS (
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_column_interaction_daily`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 最近 6 小时写入的专栏帖子快照分区，同样需要重算终层报表。
    changed_snapshot_dates AS (
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_column_post_daily`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 最近 6 小时更新过的专栏维表键，用于回推已产出 ADS 中受影响的历史日期。
    changed_columns AS (
        SELECT DISTINCT column_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_column`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND column_id IS NOT NULL
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt
        FROM changed_interaction_dates

        UNION DISTINCT

        SELECT dt
        FROM changed_snapshot_dates

        UNION DISTINCT

        SELECT DISTINCT t.dt
        FROM `{target_table_id()}` t
        JOIN changed_columns c
            ON t.column_id = c.column_id
    )
    WHERE dt IS NOT NULL
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_ads_daily_column_performance(dates: list[date]) -> None:
    """
    专栏追踪明细（日×专栏）。

    粒度：
    - dt × module × column_id

    当前版本只消费：
    - dim_column
    - dws_column_interaction_daily
    - dws_column_post_daily
    """
    if not dates:
        logging.info("没有新的 ads_daily_column_performance 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        module,
        column_id,
        column_name,
        column_exposure_uv,
        follow_total_count,
        follow_rate,
        product_click_uv,
        product_click_pv,
        product_click_penetration_rate,
        product_card_exposure_uv,
        product_exposure_click_rate,
        read_post_count,
        avg_read_post_count_per_user,
        post_total_count,
        tryon_post_count,
        tryon_click_uv,
        tryon_uv_rate,
        ai_tryon_penetration_rate,
        like_post_count,
        like_rate,
        new_follow_count,
        follow_conversion_rate_7d,
        update_time
    )
    WITH
    -- 目标日期列表。
    target_dates AS (
        SELECT DATE(dt_str) AS dt
        FROM UNNEST([{dates_str}]) AS dt_str
    ),
    -- 专栏维表映射：统一 column_id -> module / column_name，优先保留启用中的专栏。
    column_dim AS (
        SELECT
            column_id,
            module,
            column_name
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_column`
        WHERE column_id IS NOT NULL
            AND module IS NOT NULL
            AND status = 0
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY column_id
            ORDER BY IF(status = 0, 0, 1), module, column_name
        ) = 1
    ),
    -- 专栏行为事实层：只取当前批次目标日期。
    interaction_daily AS (
        SELECT
            dt,
            module,
            column_id,
            column_exposure_uv,
            follow_total_count,
            follow_uv,
            product_click_uv,
            product_click_pv,
            product_card_exposure_uv,
            read_post_count,
            tryon_post_count,
            tryon_click_uv,
            like_post_count,
            like_uv,
            new_follow_count,
            follow_like_uv_7d,
            like_uv_7d,
            dau_uv
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_column_interaction_daily`
        WHERE dt IN (SELECT dt FROM target_dates)
    ),
    -- 专栏在线帖子快照层：只承接帖子存量分子。
    post_snapshot_daily AS (
        SELECT
            dt,
            module,
            column_id,
            post_total_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_column_post_daily`
        WHERE dt IN (SELECT dt FROM target_dates)
    ),
    -- 汇总所有可能产出指标的专栏主键。
    keys AS (
        SELECT DISTINCT dt, module, column_id
        FROM interaction_daily
        UNION DISTINCT
        SELECT DISTINCT dt, module, column_id
        FROM post_snapshot_daily
    ),
    -- 汇总到最终报表所需的分子分母。
    daily AS (
        SELECT
            k.dt,
            k.module,
            k.column_id,
            COALESCE(c.column_name, '') AS column_name,
            COALESCE(i.column_exposure_uv, 0) AS column_exposure_uv,
            COALESCE(i.follow_total_count, 0) AS follow_total_count,
            COALESCE(i.follow_uv, 0) AS follow_uv,
            COALESCE(i.product_click_uv, 0) AS product_click_uv,
            COALESCE(i.product_click_pv, 0) AS product_click_pv,
            COALESCE(i.product_card_exposure_uv, 0) AS product_card_exposure_uv,
            COALESCE(i.read_post_count, 0) AS read_post_count,
            COALESCE(s.post_total_count, 0) AS post_total_count,
            COALESCE(i.tryon_post_count, 0) AS tryon_post_count,
            COALESCE(i.tryon_click_uv, 0) AS tryon_click_uv,
            COALESCE(i.like_post_count, 0) AS like_post_count,
            COALESCE(i.like_uv, 0) AS like_uv,
            COALESCE(i.new_follow_count, 0) AS new_follow_count,
            COALESCE(i.follow_like_uv_7d, 0) AS follow_like_uv_7d,
            COALESCE(i.like_uv_7d, 0) AS like_uv_7d,
            COALESCE(i.dau_uv, 0) AS dau_uv
        FROM keys k
        LEFT JOIN interaction_daily i
            ON k.dt = i.dt
            AND k.module = i.module
            AND k.column_id = i.column_id
        LEFT JOIN post_snapshot_daily s
            ON k.dt = s.dt
            AND k.module = s.module
            AND k.column_id = s.column_id
        LEFT JOIN column_dim c
            ON k.column_id = c.column_id
            AND k.module = c.module
    )
    SELECT
        dt,
        module,
        column_id,
        column_name,
        column_exposure_uv,
        follow_total_count,
        CAST(CASE
            WHEN column_exposure_uv = 0 THEN 0
            ELSE ROUND(
                CAST(follow_uv AS NUMERIC) / CAST(column_exposure_uv AS NUMERIC),
                4
            )
        END AS NUMERIC) AS follow_rate,
        product_click_uv,
        product_click_pv,
        CAST(CASE
            WHEN dau_uv = 0 THEN 0
            ELSE ROUND(
                CAST(product_click_uv AS NUMERIC) / CAST(dau_uv AS NUMERIC),
                4
            )
        END AS NUMERIC) AS product_click_penetration_rate,
        product_card_exposure_uv,
        CAST(CASE
            WHEN product_card_exposure_uv = 0 THEN 0
            ELSE ROUND(
                CAST(product_click_uv AS NUMERIC)
                / CAST(product_card_exposure_uv AS NUMERIC),
                4
            )
        END AS NUMERIC) AS product_exposure_click_rate,
        read_post_count,
        CAST(CASE
            WHEN column_exposure_uv = 0 THEN 0
            ELSE ROUND(
                CAST(read_post_count AS NUMERIC) / CAST(column_exposure_uv AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_read_post_count_per_user,
        post_total_count,
        tryon_post_count,
        tryon_click_uv,
        CAST(CASE
            WHEN column_exposure_uv = 0 THEN 0
            ELSE ROUND(
                CAST(tryon_click_uv AS NUMERIC) / CAST(column_exposure_uv AS NUMERIC),
                4
            )
        END AS NUMERIC) AS tryon_uv_rate,
        CAST(CASE
            WHEN post_total_count = 0 THEN 0
            ELSE ROUND(
                CAST(tryon_post_count AS NUMERIC) / CAST(post_total_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS ai_tryon_penetration_rate,
        like_post_count,
        CAST(CASE
            WHEN column_exposure_uv = 0 THEN 0
            ELSE ROUND(
                CAST(like_uv AS NUMERIC) / CAST(column_exposure_uv AS NUMERIC),
                4
            )
        END AS NUMERIC) AS like_rate,
        new_follow_count,
        CAST(CASE
            WHEN like_uv_7d = 0 THEN 0
            ELSE ROUND(
                CAST(follow_like_uv_7d AS NUMERIC) / CAST(like_uv_7d AS NUMERIC),
                4
            )
        END AS NUMERIC) AS follow_conversion_rate_7d,
        CURRENT_TIMESTAMP() AS update_time
    FROM daily
    """

    logging.info("开始处理: ads_daily_column_performance")
    job = client.query(query)
    job.result()
    logging.info("ads_daily_column_performance 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    dates = get_column_performance_dates_to_process()
    if not dates:
        logging.info("没有新的 ads_daily_column_performance 日期需要处理")
    else:
        run_ads_daily_column_performance(dates)
