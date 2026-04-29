from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
V3_DATASET_ID = "v3_decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dim_user_closet_first_use"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dim" / TABLE_NAME
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


def service_ts_expr(field_name: str = "gmt_create") -> str:
    """v3 服务表的 gmt_create 是 UTC DATETIME，这里统一转成 TIMESTAMP。"""
    return f"TIMESTAMP({field_name}, 'UTC')"


def service_change_ts_expr() -> str:
    """
    Datastream 源表优先用 source_timestamp 判断最近变更；
    缺失时回退到 gmt_update / gmt_create，避免漏掉最近同步的新行。
    """
    return (
        "COALESCE("
        "TIMESTAMP_MILLIS(datastream_metadata.source_timestamp), "
        "TIMESTAMP(COALESCE(gmt_update, gmt_create), 'UTC')"
        ")"
    )


def get_dim_user_closet_first_use_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    按 dim_user_closet_first_use 自身状态判定待处理日期。

    规则：
    - 目标表不存在或为空时，全量初始化衣橱服务端历史业务日
    - 目标表已存在时，只处理最近 6 小时有变更的衣橱服务端业务日
    """
    service_ts = service_ts_expr()
    service_change_ts = service_change_ts_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_fashion_closet`
        WHERE user_id IS NOT NULL
            AND user_id != ''
            AND type IN ('SINGLE', 'OUTFIT')
            AND DATE({service_ts}, '{TORONTO_TZ}') IS NOT NULL
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT DATE({service_ts}, '{TORONTO_TZ}') AS dt
    FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_fashion_closet`
    WHERE {service_change_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
        AND user_id IS NOT NULL
        AND user_id != ''
        AND type IN ('SINGLE', 'OUTFIT')
        AND DATE({service_ts}, '{TORONTO_TZ}') IS NOT NULL
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dim_user_closet_first_use(
    dates: list[date],
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> None:
    """
    增量维护用户首次衣橱使用维表。

    字段：
    - first_single_dt：首次创建 SINGLE 的业务日
    - first_any_dt：首次创建 SINGLE / OUTFIT 的业务日

    维护方式：
    - 只读取最近 6 小时服务端变更行
    - 对每个用户抽出本批候选首日
    - 仅当候选首日更早时，回写维表
    """
    if not dates:
        logging.info("没有新的 dim_user_closet_first_use 日期需要处理")
        return

    service_ts = service_ts_expr()
    service_change_ts = service_change_ts_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    source_filter = """
        user_id IS NOT NULL
        AND user_id != ''
        AND type IN ('SINGLE', 'OUTFIT')
        AND DATE({service_ts}, '{TORONTO_TZ}') IS NOT NULL
    """.format(service_ts=service_ts, TORONTO_TZ=TORONTO_TZ)
    if not is_full_snapshot:
        source_filter += """
        AND {service_change_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
        """.format(
            service_change_ts=service_change_ts,
            window_hours=window_hours,
        )

    query = f"""
    MERGE `{target_table_id()}` AS dim
    USING (
        WITH
        -- 提取本批最近变更的衣橱服务端记录。
        base AS (
            SELECT
                user_id,
                type,
                DATE({service_ts}, '{TORONTO_TZ}') AS dt
            FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_fashion_closet`
            WHERE {source_filter}
        ),
        -- 汇总本批用户的候选首次衣橱使用日期。
        candidate_first_use AS (
            SELECT
                user_id,
                MIN(IF(type = 'SINGLE', dt, NULL)) AS first_single_dt,
                MIN(dt) AS first_any_dt
            FROM base
            GROUP BY user_id
        )
        SELECT
            user_id,
            first_single_dt,
            first_any_dt
        FROM candidate_first_use
    ) AS src
    ON dim.user_id = src.user_id
    WHEN MATCHED AND (
        (
            src.first_single_dt IS NOT NULL
            AND (
                dim.first_single_dt IS NULL
                OR src.first_single_dt < dim.first_single_dt
            )
        )
        OR (
            src.first_any_dt IS NOT NULL
            AND (
                dim.first_any_dt IS NULL
                OR src.first_any_dt < dim.first_any_dt
            )
        )
    ) THEN
        UPDATE SET
            first_single_dt = CASE
                WHEN src.first_single_dt IS NULL THEN dim.first_single_dt
                WHEN dim.first_single_dt IS NULL THEN src.first_single_dt
                ELSE LEAST(dim.first_single_dt, src.first_single_dt)
            END,
            first_any_dt = CASE
                WHEN src.first_any_dt IS NULL THEN dim.first_any_dt
                WHEN dim.first_any_dt IS NULL THEN src.first_any_dt
                ELSE LEAST(dim.first_any_dt, src.first_any_dt)
            END,
            update_time = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (user_id, first_single_dt, first_any_dt, update_time)
        VALUES (src.user_id, src.first_single_dt, src.first_any_dt, CURRENT_TIMESTAMP());
    """
    logging.info("开始处理: dim_user_closet_first_use")
    client.query(query).result()
    logging.info("dim_user_closet_first_use 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    start_time = datetime.now(timezone.utc)

    dates = get_dim_user_closet_first_use_dates_to_process()
    if not dates:
        logging.info("没有新的 dim_user_closet_first_use 数据需要处理")
        logging.info("日志文件: %s", log_path)
        raise SystemExit(0)

    logging.info("dim_user_closet_first_use 待处理日期: %s", dates)
    run_dim_user_closet_first_use(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dim_user_closet_first_use 执行完成, 耗时: %.1f 秒", elapsed)
    logging.info("日志文件: %s", log_path)
