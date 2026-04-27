from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_daily_post_tryon_confirm"
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


def get_post_tryon_confirm_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    帖子级试穿点击 ADS 同时依赖 `dws_tryon_confirm_post_daily` 与 `dim_post`。

    规则：
    - 目标表不存在或为空时，按帖子级 DWS 已落地历史分区全量初始化
    - 目标表已存在时，合并：
      - 最近 6 小时内帖子级 DWS 变更的 `dt`
      - 最近 6 小时内 `dim_post` 更新所影响的历史 `dt`
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_tryon_confirm_post_daily`
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
        INNER JOIN changed_posts p USING (post_code)
        """

    query = f"""
    WITH
    -- 最近 6 小时帖子级 DWS 变更，对应直接受影响的业务日。
    changed_dws_dates AS (
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_tryon_confirm_post_daily`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ),
    -- 最近 6 小时帖子维度修正，用于回推受影响历史分区。
    changed_posts AS (
        SELECT DISTINCT post_code
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_post`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND post_code IS NOT NULL
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


def run_ads_daily_post_tryon_confirm(dates: list[date]) -> None:
    """
    穿搭工具点击使用帖子明细。

    当前版本只消费：
    - dws_tryon_confirm_post_daily
    - dim_post

    ADS 只保留终层展示指标：
    - avg_click_use_count_per_user
    - 帖子展示维度补齐
    """
    if not dates:
        logging.info("没有新的 ads_daily_post_tryon_confirm 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        post_id,
        post_code,
        post_name,
        creator,
        click_use_pv,
        click_use_uv,
        avg_click_use_count_per_user,
        update_time
    )
    WITH
    -- 帖子级试穿点击核心事实层：只取本批目标日期。
    post_fact AS (
        SELECT
            dt,
            post_code,
            click_use_pv,
            click_use_uv
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_tryon_confirm_post_daily`
        WHERE dt IN ({dates_str})
    ),
    -- 帖子维度层：通过 dim_post 补齐帖子主键和展示字段。
    post_dim AS (
        SELECT
            post_code,
            post_id,
            post_name,
            creator
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_post`
        WHERE post_code IS NOT NULL
            AND post_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY post_code
            ORDER BY update_time DESC, post_id DESC
        ) = 1
    ),
    -- 终层补维：仅保留能成功映射到帖子主键的事实行。
    enriched AS (
        SELECT
            f.dt,
            d.post_id,
            f.post_code,
            d.post_name,
            d.creator,
            f.click_use_pv,
            f.click_use_uv
        FROM post_fact f
        INNER JOIN post_dim d USING (post_code)
    )
    SELECT
        dt,
        post_id,
        post_code,
        post_name,
        creator,
        click_use_pv,
        click_use_uv,
        CAST(
            CASE
                WHEN click_use_uv = 0 THEN 0
                ELSE ROUND(
                    CAST(click_use_pv AS NUMERIC) / CAST(click_use_uv AS NUMERIC),
                    4
                )
            END AS NUMERIC
        ) AS avg_click_use_count_per_user,
        CURRENT_TIMESTAMP() AS update_time
    FROM enriched;
    """
    logging.info("开始处理: ads_daily_post_tryon_confirm")
    job = client.query(query)
    job.result()
    logging.info("ads_daily_post_tryon_confirm 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_post_tryon_confirm_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 ads_daily_post_tryon_confirm 日期需要处理")
        raise SystemExit(0)

    run_ads_daily_post_tryon_confirm(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("ads_daily_post_tryon_confirm 完成, 耗时: %.1f 秒", elapsed)
