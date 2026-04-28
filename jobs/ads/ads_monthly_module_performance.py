from datetime import date, datetime
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_monthly_module_performance"
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


def get_module_performance_months_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    模块月报 ADS 只消费月级中层事实，待处理月份必须单独判断。

    规则：
    - 目标表不存在或为空时，按月级行为事实 + 月级快照事实已落地的所有月份初始化
    - 目标表已存在时，读取最近 6 小时内月级 DWS 变更的 month
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT month
        FROM (
            SELECT month
            FROM `{PROJECT_ID}.{DATASET_ID}.dws_module_interaction_monthly`

            UNION DISTINCT

            SELECT month
            FROM `{PROJECT_ID}.{DATASET_ID}.dws_module_post_monthly`
        )
        WHERE month IS NOT NULL
        ORDER BY month
        """
        results = client.query(query).result()
        return [row.month for row in results]

    query = f"""
    WITH
    -- 最近 6 小时写入的模块月级行为 DWS 分区，直接对应需要重算的 ADS 月份。
    changed_interaction_months AS (
        SELECT DISTINCT month
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_module_interaction_monthly`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 最近 6 小时写入的模块月级帖子快照分区，同样需要重算终层报表。
    changed_snapshot_months AS (
        SELECT DISTINCT month
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_module_post_monthly`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    )
    SELECT DISTINCT month
    FROM (
        SELECT month
        FROM changed_interaction_months

        UNION DISTINCT

        SELECT month
        FROM changed_snapshot_months
    )
    WHERE month IS NOT NULL
    ORDER BY month
    """
    results = client.query(query).result()
    return [row.month for row in results]


def run_ads_monthly_module_performance(months: list[date]) -> None:
    """
    模块追踪明细（月×模块）。

    粒度：
    - month × module

    当前版本只消费：
    - dws_module_interaction_monthly
    - dws_module_post_monthly
    """
    if not months:
        logging.info("没有新的 ads_monthly_module_performance 月份需要处理")
        return

    months_str = dates_to_sql_list(months)
    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE month IN ({months_str});

    INSERT INTO `{target_table_id()}`
    (
        month,
        module,
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
        like_follow_rate,
        update_time
    )
    WITH
    -- 目标月份列表。
    target_months AS (
        SELECT DATE(month_str) AS month
        FROM UNNEST([{months_str}]) AS month_str
    ),
    -- 模块月级行为事实层：只取当前批次目标月份。
    interaction_monthly AS (
        SELECT
            month,
            module,
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
            follow_like_uv,
            mau_uv
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_module_interaction_monthly`
        WHERE month IN (SELECT month FROM target_months)
    ),
    -- 模块月级帖子快照层：只承接月末帖子存量分子。
    post_snapshot_monthly AS (
        SELECT
            month,
            module,
            post_total_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_module_post_monthly`
        WHERE month IN (SELECT month FROM target_months)
    ),
    -- 汇总所有可能产出指标的模块主键。
    keys AS (
        SELECT DISTINCT month, module
        FROM interaction_monthly
        UNION DISTINCT
        SELECT DISTINCT month, module
        FROM post_snapshot_monthly
    ),
    -- 汇总到最终报表所需的分子分母。
    monthly AS (
        SELECT
            k.month,
            k.module,
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
            COALESCE(i.follow_like_uv, 0) AS follow_like_uv,
            COALESCE(i.mau_uv, 0) AS mau_uv
        FROM keys k
        LEFT JOIN interaction_monthly i
            ON k.month = i.month
            AND k.module = i.module
        LEFT JOIN post_snapshot_monthly s
            ON k.month = s.month
            AND k.module = s.module
    )
    SELECT
        month,
        module,
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
            WHEN mau_uv = 0 THEN 0
            ELSE ROUND(
                CAST(product_click_uv AS NUMERIC) / CAST(mau_uv AS NUMERIC),
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
            WHEN new_follow_count = 0 THEN 0
            ELSE ROUND(
                CAST(follow_like_uv AS NUMERIC) / CAST(new_follow_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS like_follow_rate,
        CURRENT_TIMESTAMP() AS update_time
    FROM monthly
    """

    logging.info("开始处理: ads_monthly_module_performance")
    job = client.query(query)
    job.result()
    logging.info("ads_monthly_module_performance 刷新完成, 处理月份: %s", months)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    months = get_module_performance_months_to_process()
    if not months:
        logging.info("没有新的 ads_monthly_module_performance 月份需要处理")
    else:
        run_ads_monthly_module_performance(months)
