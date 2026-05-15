from datetime import datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_weekly_formal_video_increment_growth"
SOURCE_TABLE_NAME = "ads_daily_formal_video_increment"
REPORT_START_DATE = "2026-04-13"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "ads" / TABLE_NAME

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


def target_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


def source_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{SOURCE_TABLE_NAME}"


def target_table_exists() -> bool:
    try:
        client.get_table(target_table_id())
        return True
    except NotFound:
        return False


def build_select_sql() -> str:
    return f"""
    WITH
    -- 读取正式号视频日增量，周窗口固定从 2026-04-13 起按 7 天切分。
    daily_formal_video_increment AS (
        SELECT
            dt,
            daily_view_increment,
            DIV(DATE_DIFF(dt, DATE '{REPORT_START_DATE}', DAY), 7) AS week_index
        FROM `{source_table_id()}`
        WHERE dt >= DATE '{REPORT_START_DATE}'
    ),
    -- 每个 7 天窗口计算日新增 views 的算术平均值。
    weekly_base AS (
        SELECT
            week_index,
            DATE_ADD(DATE '{REPORT_START_DATE}', INTERVAL week_index * 7 DAY)
                AS week_start_dt,
            DATE_ADD(
                DATE_ADD(DATE '{REPORT_START_DATE}', INTERVAL week_index * 7 DAY),
                INTERVAL 6 DAY
            ) AS week_end_dt,
            COUNT(*) AS day_count,
            COALESCE(SUM(daily_view_increment), 0) AS weekly_view_increment,
            CAST(ROUND(AVG(CAST(daily_view_increment AS NUMERIC)), 4) AS NUMERIC)
                AS weekly_avg_daily_view_increment
        FROM daily_formal_video_increment
        GROUP BY week_index
        HAVING COUNT(*) = 7
    ),
    -- 计算相邻周之间的增长倍数，首周作为基准展示 1.00。
    weekly_with_growth AS (
        SELECT
            week_index,
            week_start_dt,
            week_end_dt,
            day_count,
            weekly_view_increment,
            weekly_avg_daily_view_increment,
            LAG(weekly_avg_daily_view_increment) OVER (ORDER BY week_index)
                AS previous_week_avg_daily_view_increment
        FROM weekly_base
    ),
    -- 排除首周基准后，计算全周期周均增长倍数。
    weekly_final AS (
        SELECT
            week_index,
            week_start_dt,
            week_end_dt,
            day_count,
            weekly_view_increment,
            weekly_avg_daily_view_increment,
            previous_week_avg_daily_view_increment,
            CAST(CASE
                WHEN week_index = 0 THEN 1
                WHEN COALESCE(previous_week_avg_daily_view_increment, 0) = 0 THEN NULL
                ELSE ROUND(
                    weekly_avg_daily_view_increment
                    / previous_week_avg_daily_view_increment,
                    4
                )
            END AS NUMERIC) AS week_growth_multiple
        FROM weekly_with_growth
    ),
    -- 将周均增长倍数换算成月度和季度增长倍数。
    growth_summary AS (
        SELECT
            CAST(ROUND(AVG(
                IF(week_index = 0, NULL, week_growth_multiple)
            ), 4) AS NUMERIC) AS avg_week_growth_multiple
        FROM weekly_final
        WHERE week_growth_multiple IS NOT NULL
    )
    SELECT
        w.week_index,
        w.week_start_dt,
        w.week_end_dt,
        CONCAT(
            FORMAT_DATE('%m-%d', w.week_start_dt),
            '-',
            FORMAT_DATE('%m-%d', w.week_end_dt)
        ) AS week_label,
        w.day_count,
        w.weekly_view_increment,
        w.weekly_avg_daily_view_increment,
        w.previous_week_avg_daily_view_increment,
        w.week_growth_multiple,
        s.avg_week_growth_multiple,
        CAST(CASE
            WHEN s.avg_week_growth_multiple IS NULL THEN NULL
            ELSE ROUND(POWER(CAST(s.avg_week_growth_multiple AS FLOAT64), 30.5 / 7), 4)
        END AS NUMERIC) AS monthly_growth_multiple,
        CAST(CASE
            WHEN s.avg_week_growth_multiple IS NULL THEN NULL
            ELSE ROUND(POWER(CAST(s.avg_week_growth_multiple AS FLOAT64), 91.5 / 7), 4)
        END AS NUMERIC) AS quarterly_growth_multiple,
        CURRENT_TIMESTAMP() AS update_time
    FROM weekly_final w
    CROSS JOIN growth_summary s
    """


def run_ads_weekly_formal_video_increment_growth() -> None:
    target_table = target_table_id()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    staging_table = f"{target_table}_tmp_{run_id}"
    target_dropped = False

    logging.info(
        "开始处理 %s, 目标表: %s, 临时表: %s",
        TABLE_NAME,
        target_table,
        staging_table,
    )

    try:
        client.query(
            f"""
            CREATE TABLE `{staging_table}`
            CLUSTER BY week_start_dt AS
            {build_select_sql()}
            """
        ).result()

        validation_row = list(client.query(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT week_index) AS week_count
            FROM `{staging_table}`
            """
        ).result())[0]
        if validation_row.row_count != validation_row.week_count:
            raise ValueError(
                "ads_weekly_formal_video_increment_growth staging 主键校验失败: "
                f"row_count={validation_row.row_count}, week_count={validation_row.week_count}"
            )

        if target_table_exists():
            client.query(f"DROP TABLE `{target_table}`").result()
            target_dropped = True
            client.query(f"ALTER TABLE `{staging_table}` RENAME TO {TABLE_NAME}").result()
        else:
            client.query(
                f"""
                CREATE TABLE `{target_table}`
                CLUSTER BY week_start_dt AS
                SELECT *
                FROM `{staging_table}`
                """
            ).result()
            client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
    except Exception:
        if not target_dropped:
            client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
        raise

    logging.info("%s 刷新完成", TABLE_NAME)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    run_ads_weekly_formal_video_increment_growth()
    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
