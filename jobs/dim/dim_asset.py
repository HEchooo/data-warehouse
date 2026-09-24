"""DIM 素材维度层。

落地 ``decom.dim_asset``，粒度：``asset_code`` 一键一行。

依赖：
- v3_decom.community_fashion_asset（素材主数据）
- v3_decom.community_post（帖子素材映射、素材上线时间）
- decom.dim_column（专栏名称）
"""

from datetime import datetime, timezone
import logging
import os
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
V3_DATASET_ID = "v3_decom"
TABLE_NAME = "dim_asset"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dim" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = int(os.environ.get("ASSET_RECOVERY_WINDOW_HOURS", "6"))
if INCREMENTAL_WINDOW_HOURS <= 0:
    raise ValueError("ASSET_RECOVERY_WINDOW_HOURS 必须是正整数")

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


def change_ts_expr() -> str:
    """
    Datastream 源表优先用 source_timestamp 判断最近变更；
    缺失时回退到 gmt_update / gmt_create。
    """
    return (
        "COALESCE("
        "TIMESTAMP_MILLIS(datastream_metadata.source_timestamp), "
        "TIMESTAMP(COALESCE(gmt_update, gmt_create), 'UTC')"
        ")"
    )


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
        {alias}.asset_code,
        {alias}.asset_name,
        {alias}.creator,
        {alias}.asset_gmt_create,
        {alias}.post_code,
        {alias}.publish_time,
        {alias}.publish_dt,
        {alias}.post_type,
        {alias}.post_status,
        {alias}.module,
        {alias}.column_id,
        {alias}.column_name,
        {alias}.update_time
    """.strip()


def build_source_snapshot_sql(is_full_snapshot: bool) -> str:
    """
    素材维度快照：素材主数据 LEFT JOIN 首发帖，再 LEFT JOIN 专栏名称。

    增量模式下先算出「素材主表变更」或「帖子素材映射变更」涉及的 asset_code 集合，
    再对这批 asset_code 重新扫描全历史做去重和 JOIN，避免只看单侧变更导致
    模块归属、上线时间等字段被漏刷。
    """
    change_ts = change_ts_expr()
    window = INCREMENTAL_WINDOW_HOURS

    if is_full_snapshot:
        changed_codes_cte = "SELECT CAST(NULL AS STRING) AS asset_code LIMIT 0"
        scope_filter = ""
    else:
        changed_codes_cte = f"""
        SELECT DISTINCT CAST(asset_code AS STRING) AS asset_code
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_fashion_asset`
        WHERE asset_code IS NOT NULL AND asset_code != ''
            AND {change_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window} HOUR)
        UNION DISTINCT
        SELECT DISTINCT JSON_VALUE(main_asset, '$.assetCode') AS asset_code
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_post` p,
        UNNEST(JSON_QUERY_ARRAY(p.post_ext, '$.mainAssets')) AS main_asset
        WHERE JSON_VALUE(main_asset, '$.assetCode') IS NOT NULL
            AND JSON_VALUE(main_asset, '$.assetCode') != ''
            AND {change_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window} HOUR)
        """
        scope_filter = "        WHERE a.asset_code IN (SELECT asset_code FROM changed_codes)\n"

    return f"""
    WITH
    -- 增量变更素材集合：全量模式下为空集合占位，不参与过滤。
    changed_codes AS (
        {changed_codes_cte}
    ),
    -- 素材主数据原始层。
    raw_asset AS (
        SELECT
            CAST(asset_code AS STRING) AS asset_code,
            asset_name,
            creator,
            id,
            gmt_create,
            gmt_update
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_fashion_asset`
        WHERE asset_code IS NOT NULL AND asset_code != ''
    ),
    -- 素材去重层：同一 asset_code 存在多条记录时，保留 COALESCE(gmt_update, gmt_create) 最新、id 最大的一条。
    deduped_asset AS (
        SELECT
            asset_code,
            asset_name,
            creator,
            gmt_create
        FROM raw_asset
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY asset_code
            ORDER BY COALESCE(gmt_update, gmt_create) DESC, id DESC
        ) = 1
    ),
    -- 帖子素材映射层：展开 post_ext.mainAssets，得到 asset_code 对应的候选帖子。
    post_asset_map AS (
        SELECT
            JSON_VALUE(main_asset, '$.assetCode') AS asset_code,
            p.post_code,
            p.post_type,
            p.status AS post_status,
            p.poster_user_id,
            p.publish_time
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_post` p,
        UNNEST(JSON_QUERY_ARRAY(p.post_ext, '$.mainAssets')) AS main_asset
        WHERE JSON_VALUE(main_asset, '$.assetCode') IS NOT NULL
            AND JSON_VALUE(main_asset, '$.assetCode') != ''
    ),
    -- 首发帖层：素材上线时间取该素材最早发布的帖子（素材发布时间就是帖子发布时间）。
    first_post AS (
        SELECT
            asset_code,
            post_code,
            post_type,
            post_status,
            poster_user_id,
            publish_time
        FROM post_asset_map
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY asset_code
            ORDER BY publish_time, post_code
        ) = 1
    )
    SELECT
        a.asset_code,
        a.asset_name,
        a.creator,
        TIMESTAMP(a.gmt_create, 'UTC') AS asset_gmt_create,
        fp.post_code,
        TIMESTAMP(fp.publish_time, 'UTC') AS publish_time,
        DATE(TIMESTAMP(fp.publish_time, 'UTC'), 'America/Toronto') AS publish_dt,
        fp.post_type,
        fp.post_status,
        CASE
            WHEN fp.post_type IS NULL THEN NULL
            WHEN fp.post_type IN (2, 4) THEN 'star'
            WHEN fp.post_type = 5 THEN 'magazine'
            WHEN fp.post_type IN (6, 7) THEN 'brand'
            WHEN fp.post_type = 8 THEN 'video'
            WHEN fp.post_type = 9 THEN 'kol'
            ELSE 'other'
        END AS module,
        fp.poster_user_id AS column_id,
        dc.column_name,
        CURRENT_TIMESTAMP() AS update_time
    FROM deduped_asset a
    LEFT JOIN first_post fp
        ON fp.asset_code = a.asset_code
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_column` dc
        ON dc.column_id = fp.poster_user_id
{scope_filter}    """


def get_staging_stats(staging_table_id: str) -> dict:
    query = f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT asset_code) AS distinct_asset_codes,
        COUNTIF(asset_code IS NULL OR asset_code = '') AS invalid_asset_code_rows
    FROM `{staging_table_id}`
    """
    rows = list(client.query(query).result())
    if not rows:
        raise RuntimeError("dim_asset staging 校验失败：未返回结果")

    row = rows[0]
    return {
        "total_rows": int(row.total_rows),
        "distinct_asset_codes": int(row.distinct_asset_codes),
        "invalid_asset_code_rows": int(row.invalid_asset_code_rows),
    }


def validate_staging_table(staging_table_id: str, allow_empty: bool) -> int:
    stats = get_staging_stats(staging_table_id)

    if stats["total_rows"] == 0:
        if allow_empty:
            return 0
        raise RuntimeError("dim_asset staging 校验失败：结果为空")

    if stats["total_rows"] != stats["distinct_asset_codes"]:
        raise RuntimeError("dim_asset staging 校验失败：asset_code 未做到一键一行")
    if stats["invalid_asset_code_rows"] != 0:
        raise RuntimeError("dim_asset staging 校验失败：存在空 asset_code")

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
        CLUSTER BY asset_code, module, column_id AS
        SELECT
            {target_columns_sql("s")}
        FROM `{staging_table_id}` s
        """
    else:
        query = f"""
        CREATE TABLE `{changed_table_id}`
        CLUSTER BY asset_code, module, column_id AS
        SELECT
            {target_columns_sql("t")}
        FROM `{target_table_id()}` t
        WHERE asset_code NOT IN (
            SELECT asset_code
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


def run_dim_asset() -> None:
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

    logging.info("开始处理: dim_asset")
    logging.info("目标表是否存在: %s", table_exists)
    logging.info("目标表当前行数: %s", target_rows)
    logging.info("本次模式: %s", "full_snapshot" if is_full_snapshot else "incremental_upsert")
    if not is_full_snapshot:
        logging.info("本次增量窗口: 最近 %s 小时", INCREMENTAL_WINDOW_HOURS)
    logging.info("本次 staging 表: %s", staging_table_id)
    logging.info("本次 changed 表: %s", changed_table_id)

    try:
        client.query(staging_query).result()
        staging_rows = validate_staging_table(
            staging_table_id=staging_table_id,
            allow_empty=not is_full_snapshot,
        )
        if staging_rows == 0:
            logging.info("dim_asset 无源侧增量变更，跳过刷新")
            return

        build_changed_table(
            staging_table_id=staging_table_id,
            changed_table_id=changed_table_id,
            is_full_snapshot=is_full_snapshot,
        )
        swap_changed_table(changed_table_id)
        logging.info("dim_asset 刷新完成, 本次写入行数: %s", staging_rows)
    finally:
        cleanup_temp_tables(staging_table_id)


if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)
    log_path = configure_logging()
    logging.info("dim_asset 日志文件: %s", log_path)

    run_dim_asset()

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dim_asset 执行完成, 耗时: %.1f 秒", elapsed)
