"""ADS 日级 AI-Fashion Style Me 看板宽表。

落地 ``decom.ads_daily_ai_fashion_styleme``。

依赖：
- decom.dws_ai_fashion_styleme_daily
"""

from datetime import date, datetime, timedelta
import argparse
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "ads_daily_ai_fashion_styleme"
SOURCE_TABLE_NAME = "dws_ai_fashion_styleme_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "ads" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6
TARGET_COLUMNS = (
    "dt",
    "styleme_user_uv",
    "next_day_retention_rate",
    "next_day_retained_uv",
    "day_7_retention_rate",
    "day_7_retained_uv",
    "day_30_retention_rate",
    "day_30_retained_uv",
    "popup_exposure_uv",
    "quick_try_btn_uv",
    "quick_penetration_rate",
    "quick_task_success_count",
    "quick_add_closet_uv",
    "quick_add_closet_calendar_uv",
    "quick_result_success_uv",
    "quick_add_closet_rate",
    "quick_add_closet_calendar_rate",
    "plus_item_try_btn_uv",
    "plus_item_penetration_rate",
    "plus_item_task_success_count",
    "plus_item_add_closet_uv",
    "plus_item_add_closet_calendar_uv",
    "plus_result_success_uv",
    "plus_item_add_closet_rate",
    "plus_item_add_closet_calendar_rate",
    "plus_outfit_try_btn_uv",
    "plus_outfit_penetration_rate",
    "plus_outfit_task_success_count",
    "plus_outfit_add_closet_uv",
    "plus_outfit_add_closet_calendar_uv",
    "plus_outfit_add_closet_rate",
    "plus_outfit_add_closet_calendar_rate",
    "update_time",
)

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


def target_columns_sql(indent: str = "        ") -> str:
    return ",\n".join(f"{indent}{col}" for col in TARGET_COLUMNS)


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
    """按上游 DWS update_time 判定待处理日期。"""
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM `{source_table_id()}`
        WHERE dt IS NOT NULL
        ORDER BY dt
        """
        return [row.dt for row in client.query(query).result()]

    query = f"""
    SELECT DISTINCT dt
    FROM `{source_table_id()}`
    WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
        AND dt IS NOT NULL
    ORDER BY dt
    """
    return [row.dt for row in client.query(query).result()]


def build_select_sql(day_dts: list[date]) -> str:
    day_values = ", ".join([f"DATE '{d}'" for d in day_dts])
    min_dt = min(day_dts)
    max_dt = max(day_dts)

    return f"""
    WITH
    -- 仅保留本次需要重算的业务日。
    day_spine AS (
        SELECT dt FROM UNNEST([{day_values}]) AS dt
    ),
    -- Style Me 日事实。
    src AS (
        SELECT *
        FROM `{source_table_id()}`
        WHERE dt BETWEEN DATE '{min_dt}' AND DATE '{max_dt}'
    )
    SELECT
        s.dt,
        -- 留存
        CAST(src.styleme_user_uv AS NUMERIC) AS styleme_user_uv,
        CAST(
            SAFE_DIVIDE(
                src.styleme_next_day_retained_uv,
                NULLIF(
                    src.styleme_next_day_retained_uv
                    + src.styleme_next_day_not_retained_uv,
                    0
                )
            ) AS NUMERIC
        ) AS next_day_retention_rate,
        CAST(src.styleme_next_day_retained_uv AS NUMERIC) AS next_day_retained_uv,
        CAST(
            SAFE_DIVIDE(
                src.styleme_day_7_retained_uv,
                NULLIF(
                    src.styleme_day_7_retained_uv
                    + src.styleme_day_7_not_retained_uv,
                    0
                )
            ) AS NUMERIC
        ) AS day_7_retention_rate,
        CAST(src.styleme_day_7_retained_uv AS NUMERIC) AS day_7_retained_uv,
        CAST(
            SAFE_DIVIDE(
                src.styleme_day_30_retained_uv,
                NULLIF(
                    src.styleme_day_30_retained_uv
                    + src.styleme_day_30_not_retained_uv,
                    0
                )
            ) AS NUMERIC
        ) AS day_30_retention_rate,
        CAST(src.styleme_day_30_retained_uv AS NUMERIC) AS day_30_retained_uv,
        -- 弹窗曝光（三场景共用）
        CAST(src.popup_exposure_uv AS NUMERIC) AS popup_exposure_uv,
        -- Quick Mode
        CAST(src.quick_try_btn_uv AS NUMERIC) AS quick_try_btn_uv,
        CAST(
            SAFE_DIVIDE(src.quick_try_btn_uv, NULLIF(src.popup_exposure_uv, 0)) AS NUMERIC
        ) AS quick_penetration_rate,
        CAST(src.quick_task_success_count AS NUMERIC) AS quick_task_success_count,
        CAST(src.quick_add_closet_uv AS NUMERIC) AS quick_add_closet_uv,
        CAST(src.quick_add_closet_calendar_uv AS NUMERIC) AS quick_add_closet_calendar_uv,
        CAST(src.quick_result_success_uv AS NUMERIC) AS quick_result_success_uv,
        CAST(
            SAFE_DIVIDE(src.quick_add_closet_uv, NULLIF(src.quick_result_success_uv, 0))
            AS NUMERIC
        ) AS quick_add_closet_rate,
        CAST(
            SAFE_DIVIDE(src.quick_add_closet_calendar_uv, NULLIF(src.quick_result_success_uv, 0))
            AS NUMERIC
        ) AS quick_add_closet_calendar_rate,
        -- Plus Item
        CAST(src.plus_item_try_btn_uv AS NUMERIC) AS plus_item_try_btn_uv,
        CAST(
            SAFE_DIVIDE(src.plus_item_try_btn_uv, NULLIF(src.popup_exposure_uv, 0))
            AS NUMERIC
        ) AS plus_item_penetration_rate,
        CAST(src.plus_item_task_success_count AS NUMERIC) AS plus_item_task_success_count,
        CAST(src.plus_item_add_closet_uv AS NUMERIC) AS plus_item_add_closet_uv,
        CAST(src.plus_item_add_closet_calendar_uv AS NUMERIC)
            AS plus_item_add_closet_calendar_uv,
        CAST(src.plus_result_success_uv AS NUMERIC) AS plus_result_success_uv,
        CAST(
            SAFE_DIVIDE(src.plus_item_add_closet_uv, NULLIF(src.plus_result_success_uv, 0))
            AS NUMERIC
        ) AS plus_item_add_closet_rate,
        CAST(
            SAFE_DIVIDE(
                src.plus_item_add_closet_calendar_uv,
                NULLIF(src.plus_result_success_uv, 0)
            ) AS NUMERIC
        ) AS plus_item_add_closet_calendar_rate,
        -- Plus Outfit
        CAST(src.plus_outfit_try_btn_uv AS NUMERIC) AS plus_outfit_try_btn_uv,
        CAST(
            SAFE_DIVIDE(src.plus_outfit_try_btn_uv, NULLIF(src.popup_exposure_uv, 0))
            AS NUMERIC
        ) AS plus_outfit_penetration_rate,
        CAST(src.plus_outfit_task_success_count AS NUMERIC) AS plus_outfit_task_success_count,
        CAST(src.plus_outfit_add_closet_uv AS NUMERIC) AS plus_outfit_add_closet_uv,
        CAST(src.plus_outfit_add_closet_calendar_uv AS NUMERIC)
            AS plus_outfit_add_closet_calendar_uv,
        CAST(
            SAFE_DIVIDE(src.plus_outfit_add_closet_uv, NULLIF(src.plus_result_success_uv, 0))
            AS NUMERIC
        ) AS plus_outfit_add_closet_rate,
        CAST(
            SAFE_DIVIDE(
                src.plus_outfit_add_closet_calendar_uv,
                NULLIF(src.plus_result_success_uv, 0)
            ) AS NUMERIC
        ) AS plus_outfit_add_closet_calendar_rate,
        CURRENT_TIMESTAMP() AS update_time
    FROM day_spine s
    LEFT JOIN src ON src.dt = s.dt
    """


def build_write_sql(day_dts: list[date]) -> str:
    day_list = dates_to_sql_list(day_dts)
    select_sql = build_select_sql(day_dts)
    return f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({day_list});

    INSERT INTO `{target_table_id()}`
    (
{target_columns_sql()}
    )
    {select_sql}
    """


def dry_run(day_dts: list[date]) -> None:
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = client.query(build_select_sql(day_dts), job_config=job_config)
    logging.info(
        "dry-run 通过：SQL 合法，预计扫描 %.2f MB",
        job.total_bytes_processed / 1024 / 1024,
    )


def run(day_dts: list[date]) -> None:
    if not day_dts:
        logging.info("没有需要处理的日期")
        return
    logging.info("开始处理 %d 天：%s ~ %s", len(day_dts), min(day_dts), max(day_dts))
    client.query(build_write_sql(day_dts)).result()
    logging.info("写入完成")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=str, default=None, help="回刷起始日（含），YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, default=None, help="回刷结束日（含），YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true", help="仅校验 SQL，不写数据")
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = _parse_args()

    if args.start_date and args.end_date:
        start = date.fromisoformat(args.start_date)
        end = date.fromisoformat(args.end_date)
        if start > end:
            raise ValueError("start-date 不能晚于 end-date")
        day_dts = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    else:
        day_dts = get_dates_to_process()

    if not day_dts:
        logging.info("没有需要处理的日期")
        return

    if args.dry_run:
        dry_run(day_dts)
    else:
        run(day_dts)


if __name__ == "__main__":
    main()
