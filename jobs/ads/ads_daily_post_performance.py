from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_daily_post_performance"
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


def get_post_performance_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    帖子主题 ADS 依赖 DWS 事实层与 DIM 映射修正，待处理日期必须单独判断。

    规则：
    - 目标表不存在或为空时，直接按 DWS 已落地 `platform = 'all'` 的历史分区初始化
    - 目标表已存在时，读取最近 6 小时内 DWS 变更的 `dt`
    - 同时读取最近 6 小时内 DIM 变更影响到目标表的历史 `dt`
    """
    table_exists = target_table_exists()
    target_rows = get_target_row_count()
    is_full_snapshot = (
        not table_exists
        or target_rows == 0
    )

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_post_interaction_daily`
        WHERE platform = 'all'
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    WITH
    -- 最近 6 小时写入的帖子主题 DWS 分区，直接对应需要重算的 ADS 日期。
    changed_dws_dates AS (
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_post_interaction_daily`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND platform = 'all'
    ),
    -- 最近 6 小时更新过的帖子主键，用于回推已产出 ADS 中受影响的历史日期。
    changed_posts AS (
        SELECT DISTINCT post_code
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_post`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND post_code IS NOT NULL
    ),
    -- 最近 6 小时更新过的专栏主键，用于回推已产出 ADS 中受影响的历史日期。
    changed_columns AS (
        SELECT DISTINCT column_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_column`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND column_id IS NOT NULL
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt
        FROM changed_dws_dates

        UNION DISTINCT

        SELECT DISTINCT t.dt
        FROM `{target_table_id()}` t
        JOIN changed_posts p
            ON t.post_code = p.post_code

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


def run_ads_daily_post_performance(dates: list[date]) -> None:
    """
    内容维度日报（日×帖子）:
    粒度：dt × post × module × column

    当前版本只消费：
    - dim_post
    - dim_column
    - dws_post_interaction_daily（仅使用 platform = 'all' 总计口径）
    """
    if not dates:
        logging.info("没有新的 ads_daily_post_performance 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    query = f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_post_performance`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ads_daily_post_performance`
    (
        dt,
        post_id,
        post_code,
        post_name,
        creator,
        module,
        column_id,
        column_name,
        post_exposure_uv,
        like_total_count,
        like_rate,
        read_rate,
        follow_total_count,
        follow_rate,
        tryon_total_count,
        update_time
    )
    WITH
    -- 帖子维表映射：统一 post_code -> post_id / post_name / creator / column_id。
    post_dim AS (
        SELECT
            post_code,
            post_id,
            post_name,
            creator,
            column_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_post`
        WHERE post_code IS NOT NULL
            AND post_id IS NOT NULL
            AND column_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY post_code
            ORDER BY status ASC, gmt_create DESC, post_id DESC
        ) = 1
    ),
    -- 专栏维表映射：统一 column_id -> module / column_name，优先保留启用中的映射。
    column_dim AS (
        SELECT
            column_id,
            module,
            column_name
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_column`
        WHERE column_id IS NOT NULL
            AND module IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY column_id
            ORDER BY IF(status = 0, 0, 1), module, column_name
        ) = 1
    ),
    -- 帖子主题 DWS 总计层：只取 `platform = 'all'`，避免把跨平台 UV 直接相加。
    dws_post_interaction AS (
        SELECT
            dt,
            post_code,
            module,
            column_id,
            post_exposure_uv,
            read_uv,
            like_total_count,
            like_uv,
            follow_total_count,
            follow_uv,
            tryon_total_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_post_interaction_daily`
        WHERE dt IN ({dates_str})
            AND platform = 'all'
    ),
    -- 帖子日报结果层：补齐维度名称，并保留率指标分子分母。
    final AS (
        SELECT
            d.dt,
            p.post_id,
            d.post_code,
            p.post_name,
            p.creator,
            d.module,
            d.column_id,
            c.column_name,
            d.post_exposure_uv,
            d.like_total_count,
            d.like_uv,
            d.read_uv,
            d.follow_total_count,
            d.follow_uv,
            d.tryon_total_count
        FROM dws_post_interaction d
        LEFT JOIN post_dim p
            ON d.post_code = p.post_code
        LEFT JOIN column_dim c
            ON d.column_id = c.column_id
            AND d.module = c.module
    )
    SELECT
        dt,
        post_id,
        post_code,
        post_name,
        creator,
        module,
        column_id,
        column_name,
        post_exposure_uv,
        like_total_count,
        CAST(CASE
            WHEN post_exposure_uv = 0 THEN 0
            ELSE ROUND(CAST(like_uv AS NUMERIC) / CAST(post_exposure_uv AS NUMERIC), 4)
        END AS NUMERIC) AS like_rate,
        CAST(CASE
            WHEN post_exposure_uv = 0 THEN 0
            ELSE ROUND(CAST(read_uv AS NUMERIC) / CAST(post_exposure_uv AS NUMERIC), 4)
        END AS NUMERIC) AS read_rate,
        follow_total_count,
        CAST(CASE
            WHEN post_exposure_uv = 0 THEN 0
            ELSE ROUND(CAST(follow_uv AS NUMERIC) / CAST(post_exposure_uv AS NUMERIC), 4)
        END AS NUMERIC) AS follow_rate,
        tryon_total_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM final;
    """

    logging.info("开始处理: ads_daily_post_performance")
    job = client.query(query)
    job.result()
    logging.info("ads_daily_post_performance 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    dates = get_post_performance_dates_to_process()
    if not dates:
        logging.info("没有新的 ads_daily_post_performance 日期需要处理")
        raise SystemExit(0)

    logging.info("帖子主题 ADS 待处理日期: %s", dates)
    run_ads_daily_post_performance(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("ads_daily_post_performance 执行完成, 耗时: %.1f 秒", elapsed)
