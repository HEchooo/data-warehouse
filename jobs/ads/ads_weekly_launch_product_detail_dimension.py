from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
EASTERN_TZ = "America/New_York"
TABLE_NAME = "ads_weekly_launch_product_detail_dimension"
SOURCE_TABLE_NAME = "dws_launch_product_detail_device_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "ads" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6
WEEK_START_EXPR = "DATE_TRUNC({col}, WEEK(MONDAY))"

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


def dates_to_array_sql(dates: list[date]) -> str:
    return ", ".join([f"DATE '{d}'" for d in dates])


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


def get_target_row_count() -> int:
    if not target_table_exists():
        return 0

    query = f"""
    SELECT COUNT(*) AS row_count
    FROM `{target_table_id()}`
    """
    rows = list(client.query(query).result())
    return int(rows[0].row_count) if rows else 0


def get_week_starts_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    Launch 商详页多维度周报只消费 dws_launch_product_detail_device_daily。

    周窗口固定为周一起、周日止（ISO 周）。

    规则：
    - 目标表不存在或为空时，按 DWS 已落地日期范围推算全部历史周
    - 目标表已存在时，重刷最近 6 小时 DWS 变更日期所在周
    - 同时固定补当前周和上一周
    """
    week_start = WEEK_START_EXPR.format(col="dt")
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT {week_start} AS week_start_dt
        FROM `{source_table_id()}`
        WHERE dt IS NOT NULL
        ORDER BY week_start_dt
        """
        results = client.query(query).result()
        return [row.week_start_dt for row in results]

    query = f"""
    WITH
    changed_weeks AS (
        SELECT DISTINCT {week_start} AS week_start_dt
        FROM `{source_table_id()}`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND dt IS NOT NULL
    ),
    recent_weeks AS (
        SELECT {WEEK_START_EXPR.format(col=f"DATE_SUB(CURRENT_DATE('{EASTERN_TZ}'), INTERVAL 7 DAY)")}
            AS week_start_dt
        UNION DISTINCT
        SELECT {WEEK_START_EXPR.format(col=f"CURRENT_DATE('{EASTERN_TZ}')")} AS week_start_dt
    )
    SELECT DISTINCT week_start_dt
    FROM (
        SELECT week_start_dt FROM changed_weeks
        UNION DISTINCT
        SELECT week_start_dt FROM recent_weeks
    )
    WHERE week_start_dt IS NOT NULL
    ORDER BY week_start_dt
    """
    results = client.query(query).result()
    return [row.week_start_dt for row in results]


def run(week_starts: list[date]) -> None:
    """
    Launch 商详页多维度周报（周一至周日）。

    粒度：
    - week_start_dt × video_id × channel × kol_name × country × merchant

    说明：
    - 只统计商品页 PV/UV（product_detail_pv），不含第三方外跳
    - 贡献占比分母为当周全维度合计，UV 合计按 device_id 去重
    """
    if not week_starts:
        logging.info("没有新的 %s 周需要处理", TABLE_NAME)
        return

    week_starts_str = dates_to_sql_list(week_starts)
    week_starts_array_sql = dates_to_array_sql(week_starts)
    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE week_start_dt IN ({week_starts_str});

    INSERT INTO `{target_table_id()}`
    (
        week_start_dt,
        week_end_dt,
        video_id,
        channel,
        kol_name,
        country,
        merchant,
        product_detail_pv,
        product_detail_uv,
        pv_contribution_rate,
        uv_contribution_rate,
        avg_visit_per_uv,
        update_time
    )
    WITH
    target_weeks AS (
        SELECT
            week_start_dt,
            DATE_ADD(week_start_dt, INTERVAL 6 DAY) AS week_end_dt
        FROM UNNEST([{week_starts_array_sql}]) AS week_start_dt
    ),
    base AS (
        SELECT
            w.week_start_dt,
            w.week_end_dt,
            s.video_id,
            s.channel,
            s.kol_name,
            s.country,
            s.merchant,
            s.device_id,
            s.product_detail_pv
        FROM `{source_table_id()}` s
        JOIN target_weeks w
            ON s.dt BETWEEN w.week_start_dt AND w.week_end_dt
        WHERE s.dt >= (SELECT MIN(week_start_dt) FROM target_weeks)
            AND s.dt <= (SELECT MAX(week_end_dt) FROM target_weeks)
    ),
    dim_weekly AS (
        SELECT
            week_start_dt,
            week_end_dt,
            video_id,
            channel,
            kol_name,
            country,
            merchant,
            SUM(product_detail_pv) AS product_detail_pv,
            COUNT(DISTINCT IF(product_detail_pv > 0, device_id, NULL)) AS product_detail_uv
        FROM base
        GROUP BY week_start_dt, week_end_dt, video_id, channel, kol_name, country, merchant
    ),
    total_weekly AS (
        SELECT
            week_start_dt,
            SUM(product_detail_pv) AS total_pv,
            COUNT(DISTINCT IF(product_detail_pv > 0, device_id, NULL)) AS total_uv
        FROM base
        GROUP BY week_start_dt
    )
    SELECT
        d.week_start_dt,
        d.week_end_dt,
        d.video_id,
        d.channel,
        d.kol_name,
        d.country,
        d.merchant,
        d.product_detail_pv,
        d.product_detail_uv,
        SAFE_DIVIDE(d.product_detail_pv, t.total_pv) AS pv_contribution_rate,
        SAFE_DIVIDE(d.product_detail_uv, t.total_uv) AS uv_contribution_rate,
        SAFE_DIVIDE(d.product_detail_pv, d.product_detail_uv) AS avg_visit_per_uv,
        CURRENT_TIMESTAMP() AS update_time
    FROM dim_weekly d
    JOIN total_weekly t
        ON d.week_start_dt = t.week_start_dt;
    """
    logging.info("开始处理: %s", TABLE_NAME)
    client.query(query).result()
    logging.info("%s 刷新完成, 处理周: %s", TABLE_NAME, week_starts)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    week_starts = get_week_starts_to_process()
    logging.info("待处理周: %s", week_starts if week_starts else "[]")

    if not week_starts:
        logging.info("没有新的 %s 周需要处理", TABLE_NAME)
        raise SystemExit(0)

    run(week_starts)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
