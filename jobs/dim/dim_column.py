from datetime import datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
V3_DATASET_ID = "v3_decom"
TABLE_NAME = "dim_column"
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


def build_run_suffix() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def target_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


def source_update_expr() -> str:
    return "TIMESTAMP(COALESCE(gmt_update, gmt_create), 'UTC')"


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


def target_columns_sql(alias: str) -> str:
    return f"""
        {alias}.column_id,
        {alias}.module,
        {alias}.column_name,
        {alias}.kol_type,
        {alias}.status,
        {alias}.update_time
    """.strip()


def build_source_snapshot_sql(is_full_snapshot: bool) -> str:
    incremental_filter = ""
    if not is_full_snapshot:
        incremental_filter = (
            "            AND "
            f"{source_update_expr()} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {INCREMENTAL_WINDOW_HOURS} HOUR)\n"
        )

    return f"""
    WITH
    -- 原始专栏层：仅保留内容主题当前使用的 star / magazine / brand 三类实体。
    raw_column AS (
        SELECT
            CAST(user_id AS STRING) AS column_id,
            CASE
                WHEN kol_type = 2 THEN 'star'
                WHEN kol_type = 3 THEN 'magazine'
                WHEN kol_type = 4 THEN 'brand'
                ELSE NULL
            END AS module,
            nickname AS column_name,
            CAST(kol_type AS INT64) AS kol_type,
            CAST(status AS INT64) AS status
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.kol_rel`
        WHERE user_id IS NOT NULL
            AND kol_type IN (2, 3, 4)
{incremental_filter}    ),
    -- 主键去重层：同一 column_id 存在多行时，优先保留源侧最近变更记录。
    deduped_column AS (
        SELECT
            column_id,
            module,
            column_name,
            kol_type,
            status
        FROM raw_column
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY column_id
            ORDER BY IF(status = 0, 0, 1), kol_type, column_name
        ) = 1
    )
    SELECT
        column_id,
        module,
        column_name,
        kol_type,
        status,
        CURRENT_TIMESTAMP() AS update_time
    FROM deduped_column
    """


def get_staging_stats(staging_table_id: str) -> dict:
    query = f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT column_id) AS distinct_column_ids,
        COUNTIF(column_id IS NULL OR column_id = '') AS invalid_column_id_rows
    FROM `{staging_table_id}`
    """
    rows = list(client.query(query).result())
    if not rows:
        raise RuntimeError("dim_column staging 校验失败：未返回结果")

    row = rows[0]
    return {
        "total_rows": int(row.total_rows),
        "distinct_column_ids": int(row.distinct_column_ids),
        "invalid_column_id_rows": int(row.invalid_column_id_rows),
    }


def validate_staging_table(staging_table_id: str, allow_empty: bool) -> int:
    stats = get_staging_stats(staging_table_id)

    if stats["total_rows"] == 0:
        if allow_empty:
            return 0
        raise RuntimeError("dim_column staging 校验失败：结果为空")

    if stats["total_rows"] != stats["distinct_column_ids"]:
        raise RuntimeError("dim_column staging 校验失败：column_id 未做到一键一行")
    if stats["invalid_column_id_rows"] != 0:
        raise RuntimeError("dim_column staging 校验失败：存在空 column_id")

    return stats["total_rows"]


def cleanup_temp_tables(*table_ids: str) -> None:
    for table_id in table_ids:
        try:
            client.delete_table(table_id, not_found_ok=True)
            logging.info("已清理临时表: %s", table_id)
        except Exception as exc:  # pragma: no cover - 仅做兜底清理
            logging.warning("清理临时表失败 %s: %s", table_id, exc)


def build_changed_table(
    staging_table_id: str,
    changed_table_id: str,
    is_full_snapshot: bool,
) -> None:
    if is_full_snapshot or not target_table_exists():
        query = f"""
        CREATE TABLE `{changed_table_id}`
        CLUSTER BY column_id AS
        SELECT
            {target_columns_sql("s")}
        FROM `{staging_table_id}` s
        """
    else:
        query = f"""
        CREATE TABLE `{changed_table_id}`
        CLUSTER BY column_id AS
        SELECT
            {target_columns_sql("t")}
        FROM `{target_table_id()}` t
        WHERE column_id NOT IN (
            SELECT column_id
            FROM `{staging_table_id}`
        )
        UNION ALL
        SELECT
            {target_columns_sql("s")}
        FROM `{staging_table_id}` s
        """

    client.query(query).result()


def swap_changed_table(changed_table_id: str) -> None:
    if target_table_exists():
        query = f"""
        DROP TABLE `{target_table_id()}`;
        ALTER TABLE `{changed_table_id}` RENAME TO {TABLE_NAME};
        """
    else:
        query = f"""
        ALTER TABLE `{changed_table_id}` RENAME TO {TABLE_NAME};
        """

    client.query(query).result()


def run_dim_column() -> None:
    run_suffix = build_run_suffix()
    table_exists = target_table_exists()
    target_rows = get_target_row_count()
    is_full_snapshot = (
        not table_exists
        or target_rows == 0
    )

    staging_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}_tmp_{run_suffix}"
    changed_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}_changed_{run_suffix}"
    staging_query = f"""
    CREATE TABLE `{staging_table_id}` AS
    {build_source_snapshot_sql(is_full_snapshot=is_full_snapshot)}
    """

    logging.info("开始处理: dim_column")
    logging.info("目标表是否存在: %s", table_exists)
    logging.info("目标表当前行数: %s", target_rows)
    logging.info("本次模式: %s", "full_snapshot" if is_full_snapshot else "incremental_upsert")
    if not is_full_snapshot:
        logging.info("本次增量窗口: 最近 %s 小时 gmt_update", INCREMENTAL_WINDOW_HOURS)
    logging.info("本次 staging 表: %s", staging_table_id)
    logging.info("本次 changed 表: %s", changed_table_id)

    try:
        client.query(staging_query).result()
        staging_rows = validate_staging_table(
            staging_table_id=staging_table_id,
            allow_empty=not is_full_snapshot,
        )
        if staging_rows == 0:
            logging.info("dim_column 无源侧增量变更，跳过刷新")
            return

        build_changed_table(
            staging_table_id=staging_table_id,
            changed_table_id=changed_table_id,
            is_full_snapshot=is_full_snapshot,
        )
        swap_changed_table(changed_table_id)
        logging.info("dim_column 刷新完成, 本次写入行数: %s", staging_rows)
    finally:
        cleanup_temp_tables(staging_table_id)


if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)
    log_path = configure_logging()
    logging.info("dim_column 日志文件: %s", log_path)

    run_dim_column()

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dim_column 执行完成, 耗时: %.1f 秒", elapsed)
