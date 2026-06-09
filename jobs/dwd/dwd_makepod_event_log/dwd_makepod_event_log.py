from __future__ import annotations

import logging
from datetime import datetime, timezone

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TARGET_TABLE = "dwd_makepod_event_log"
PROCESS_LOG_TABLE = "oss_key_process_log"
MIN_VALID_LOG_DATE = "2000-01-01"
FUTURE_LOG_DATE_GRACE_DAYS = 1
TARGET_COLUMNS = (
    "raw_event_id",
    "event_name",
    "logAt_timestamp",
    "session_id",
    "prop_device_id",
    "prop_user_id",
    "prop_os",
    "prop_url",
    "prop_params",
    "prop_app_type",
    "prop_timezone",
    "prop_app_version",
    "prop_share_code",
    "prop_version_type",
    "country",
    "args",
    "args_page_key",
    "args_module",
    "args_element",
    "args_category_id",
    "args_spu",
    "args_sku",
    "product_code",
    "sku_code",
    "ext",
    "oss_key",
    "update_time",
    "invite_user_id",
)

client = bigquery.Client(project=PROJECT_ID)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def table_id(table_name: str) -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{table_name}"


def table_exists(full_table_id: str) -> bool:
    try:
        client.get_table(full_table_id)
        return True
    except NotFound:
        return False


def get_table_row_count(full_table_id: str) -> int:
    query = f"""
    SELECT COUNT(*) AS row_count
    FROM `{full_table_id}`
    """
    return int(next(iter(client.query(query).result())).row_count)


def build_run_suffix() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")


def build_pending_keys_query(pending_keys_table_id: str) -> str:
    return f"""
    CREATE TABLE `{pending_keys_table_id}` AS
    SELECT DISTINCT
        oss_key,
        SAFE_CAST(SPLIT(oss_key, '/')[SAFE_OFFSET(1)] AS DATE) AS oss_key_date
    FROM `{table_id(PROCESS_LOG_TABLE)}`
    WHERE COALESCE(makepod_analyze_state, 0) = 0
        AND ENDS_WITH(oss_key, '.log')
    """


def build_invite_code_functions_sql() -> str:
    return '''
    CREATE TEMP FUNCTION invite_code_to_user_id(invite_code STRING)
    RETURNS STRING
    LANGUAGE js AS """
    function base64Decode(input) {
        var alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        var index = {};
        for (var i = 0; i < alphabet.length; i += 1) {
            index[alphabet.charAt(i)] = i;
        }

        var normalized = input.replace(/-/g, "+").replace(/_/g, "/");
        var paddingLength = normalized.length % 4;
        if (paddingLength > 0) {
            normalized += "====".slice(paddingLength);
        }

        var output = [];
        for (var i = 0; i < normalized.length; i += 4) {
            var c1 = normalized.charAt(i);
            var c2 = normalized.charAt(i + 1);
            var c3 = normalized.charAt(i + 2);
            var c4 = normalized.charAt(i + 3);

            if (!(c1 in index) || !(c2 in index)) {
                return null;
            }

            var v1 = index[c1];
            var v2 = index[c2];
            var v3 = c3 === "=" ? 0 : index[c3];
            var v4 = c4 === "=" ? 0 : index[c4];

            if ((c3 !== "=" && v3 === undefined) || (c4 !== "=" && v4 === undefined)) {
                return null;
            }

            var combined = (v1 << 18) | (v2 << 12) | (v3 << 6) | v4;
            output.push((combined >> 16) & 0xff);
            if (c3 !== "=") {
                output.push((combined >> 8) & 0xff);
            }
            if (c4 !== "=") {
                output.push(combined & 0xff);
            }
        }
        return output;
    }

    function readBigInt64BE(bytes) {
        var normalized = bytes.slice(0, 8);
        while (normalized.length < 8) {
            normalized.unshift(0);
        }

        var value = 0n;
        for (var i = 0; i < normalized.length; i += 1) {
            value = (value << 8n) | BigInt(normalized[i] & 0xff);
        }

        if (value >= (1n << 63n)) {
            value -= (1n << 64n);
        }
        return value;
    }

    try {
        if (invite_code === null || invite_code === undefined) {
            return null;
        }
        var normalized = String(invite_code).trim();
        if (!normalized) {
            return null;
        }

        var decodedBytes = base64Decode(normalized);
        if (decodedBytes === null) {
            return null;
        }

        var inviteCodeCodeMask = 873645731n;
        var value = readBigInt64BE(decodedBytes);
        return String(value ^ inviteCodeCodeMask);
    } catch (error) {
        return null;
    }
    """;
    '''


def build_target_select_sql(
    alias: str,
    update_time_expr: str | None = None,
) -> str:
    select_lines: list[str] = []
    for column in TARGET_COLUMNS:
        if column == "update_time" and update_time_expr is not None:
            select_lines.append(f"{update_time_expr} AS update_time")
            continue
        select_lines.append(f"{alias}.{column}")
    return ",\n            ".join(select_lines)


def build_staging_query(
    staging_table_id: str,
    pending_keys_table_id: str,
) -> str:
    return build_invite_code_functions_sql() + f"""
    CREATE TABLE `{staging_table_id}`
    PARTITION BY DATE(logAt_timestamp)
    CLUSTER BY oss_key, event_name, prop_device_id, product_code AS
    WITH
    -- 从 ODS 分区中读取 Makepod 原始事件，并展开埋点文档中的通用字段。
    source_events AS (
        SELECT
            LOWER(CAST(o.event_name AS STRING)) AS event_name,
            o.logAt_timestamp,
            CAST(o.session_id AS STRING) AS session_id,
            CAST(o.prop_device_id AS STRING) AS prop_device_id,
            CAST(o.prop_user_id AS STRING) AS prop_user_id,
            CAST(o.prop_os AS STRING) AS prop_os,
            CAST(o.prop_url AS STRING) AS prop_url,
            CAST(o.prop_params AS STRING) AS prop_params,
            CAST(o.prop_app_type AS STRING) AS prop_app_type,
            CAST(o.prop_timezone AS STRING) AS prop_timezone,
            CAST(o.prop_app_version AS STRING) AS prop_app_version,
            CAST(o.prop_share_code AS STRING) AS prop_share_code,
            invite_code_to_user_id(CAST(o.prop_share_code AS STRING)) AS invite_user_id,
            CAST(o.prop_version_type AS STRING) AS prop_version_type,
            CAST(o.country AS STRING) AS country,
            o.args,
            JSON_VALUE(o.args, '$.page_key') AS args_page_key,
            JSON_VALUE(o.args, '$.module') AS args_module,
            JSON_QUERY(o.args, '$.element') AS args_element,
            JSON_VALUE(o.args, '$.category_id') AS args_category_id,
            JSON_QUERY(o.args, '$.spu') AS args_spu,
            JSON_QUERY(o.args, '$.sku') AS args_sku,
            o.ext,
            CAST(o.oss_key AS STRING) AS oss_key
        FROM `{table_id("ods_event_log")}` o
        INNER JOIN `{pending_keys_table_id}` p
            ON o.oss_key = p.oss_key
            AND o.oss_key_date = p.oss_key_date
        WHERE o.prop_version_type = 'makepod'
            AND o.logAt_timestamp IS NOT NULL
            AND DATE(o.logAt_timestamp) >= DATE '{MIN_VALID_LOG_DATE}'
            AND DATE(o.logAt_timestamp) <= DATE_ADD(
                CURRENT_DATE('UTC'),
                INTERVAL {FUTURE_LOG_DATE_GRACE_DAYS} DAY
            )
    ),
    -- 为拆行前的原始事件生成稳定标识。
    source_with_id AS (
        SELECT
            *,
            TO_HEX(MD5(TO_JSON_STRING(STRUCT(
                event_name,
                logAt_timestamp,
                session_id,
                prop_device_id,
                prop_user_id,
                prop_os,
                prop_url,
                prop_params,
                prop_app_type,
                prop_timezone,
                prop_app_version,
                prop_share_code,
                invite_user_id,
                prop_version_type,
                country,
                args,
                ext,
                oss_key
            )))) AS raw_event_id
        FROM source_events
    ),
    -- 把 args.spu 统一为去空值后的商品数组，并保留原数组位置。
    source_with_items AS (
        SELECT
            *,
            ARRAY(
                SELECT AS STRUCT
                    TRIM(JSON_VALUE(spu_item)) AS product_code,
                    CASE JSON_TYPE(args_sku)
                        WHEN 'string' THEN NULLIF(TRIM(JSON_VALUE(args_sku)), '')
                        WHEN 'array' THEN NULLIF(
                            TRIM(
                                JSON_VALUE(
                                    JSON_QUERY_ARRAY(args_sku)[SAFE_OFFSET(item_offset)]
                                )
                            ),
                            ''
                        )
                        ELSE NULL
                    END AS sku_code,
                    item_offset
                FROM UNNEST(
                    CASE JSON_TYPE(args_spu)
                        WHEN 'array' THEN JSON_QUERY_ARRAY(args_spu)
                        WHEN 'string' THEN [args_spu]
                        ELSE ARRAY<JSON>[]
                    END
                ) AS spu_item WITH OFFSET AS item_offset
                WHERE NULLIF(TRIM(JSON_VALUE(spu_item)), '') IS NOT NULL
                    AND LOWER(TRIM(JSON_VALUE(spu_item))) != 'null'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY
                        TRIM(JSON_VALUE(spu_item)),
                        CASE JSON_TYPE(args_sku)
                            WHEN 'string' THEN NULLIF(
                                TRIM(JSON_VALUE(args_sku)),
                                ''
                            )
                            WHEN 'array' THEN NULLIF(
                                TRIM(
                                    JSON_VALUE(
                                        JSON_QUERY_ARRAY(args_sku)[
                                            SAFE_OFFSET(item_offset)
                                        ]
                                    )
                                ),
                                ''
                            )
                            ELSE NULL
                        END
                    ORDER BY item_offset
                ) = 1
            ) AS spu_items
        FROM source_with_id
    ),
    -- 每个 args.spu 商品拆成一行，并按相同 offset 配对 args.sku。
    expanded_events AS (
        SELECT
            s.* EXCEPT (spu_items),
            item.product_code,
            item.sku_code,
            item.item_offset
        FROM source_with_items s
        CROSS JOIN UNNEST(
            IF(
                ARRAY_LENGTH(s.spu_items) = 0,
                [
                    STRUCT(
                        CAST(NULL AS STRING) AS product_code,
                        CAST(NULL AS STRING) AS sku_code,
                        CAST(0 AS INT64) AS item_offset
                    )
                ],
                s.spu_items
            )
        ) AS item
    ),
    -- 投影目标字段，并暂时保留数组位置用于批内去重。
    projected_events AS (
        SELECT
            {build_target_select_sql("e", update_time_expr="CURRENT_TIMESTAMP()")},
            e.item_offset
        FROM expanded_events e
    ),
    -- 对完全重复的 ODS 原始事件和拆行结果做幂等去重。
    deduped_events AS (
        SELECT *
        FROM projected_events
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY raw_event_id, product_code, sku_code, item_offset
            ORDER BY logAt_timestamp, oss_key
        ) = 1
    )
    SELECT * EXCEPT (item_offset)
    FROM deduped_events
    """


def get_staging_stats(staging_table_id: str) -> dict[str, int]:
    query = f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT raw_event_id) AS distinct_raw_event_ids,
        COUNT(DISTINCT TO_JSON_STRING(STRUCT(
            raw_event_id,
            product_code,
            sku_code
        ))) AS distinct_detail_keys,
        COUNT(DISTINCT oss_key) AS distinct_oss_keys,
        COUNTIF(raw_event_id IS NULL OR raw_event_id = '') AS invalid_raw_event_rows,
        COUNTIF(prop_version_type != 'makepod') AS invalid_version_rows
    FROM `{staging_table_id}`
    """
    row = next(iter(client.query(query).result()))
    return {
        "total_rows": int(row.total_rows),
        "distinct_raw_event_ids": int(row.distinct_raw_event_ids),
        "distinct_detail_keys": int(row.distinct_detail_keys),
        "distinct_oss_keys": int(row.distinct_oss_keys),
        "invalid_raw_event_rows": int(row.invalid_raw_event_rows),
        "invalid_version_rows": int(row.invalid_version_rows),
    }


def validate_staging(staging_table_id: str) -> dict[str, int]:
    stats = get_staging_stats(staging_table_id)
    if stats["total_rows"] == 0:
        return stats
    if stats["total_rows"] != stats["distinct_detail_keys"]:
        raise RuntimeError("Makepod staging 校验失败：拆行明细键未做到一键一行")
    if stats["invalid_raw_event_rows"] != 0:
        raise RuntimeError("Makepod staging 校验失败：存在空 raw_event_id")
    if stats["invalid_version_rows"] != 0:
        raise RuntimeError("Makepod staging 校验失败：混入非 makepod 事件")
    return stats


def build_changed_target(
    staging_table_id: str,
    pending_keys_table_id: str,
    changed_table_id: str,
) -> None:
    target_table_id = table_id(TARGET_TABLE)
    if table_exists(target_table_id):
        query = f"""
        CREATE TABLE `{changed_table_id}`
        PARTITION BY DATE(logAt_timestamp)
        CLUSTER BY oss_key, event_name, prop_device_id, product_code AS
        SELECT
            {build_target_select_sql("t")}
        FROM `{target_table_id}` t
        WHERE t.oss_key NOT IN (
            SELECT oss_key
            FROM `{pending_keys_table_id}`
        )
        UNION ALL
        SELECT
            {build_target_select_sql("s")}
        FROM `{staging_table_id}` s
        """
    else:
        query = f"""
        CREATE TABLE `{changed_table_id}`
        PARTITION BY DATE(logAt_timestamp)
        CLUSTER BY oss_key, event_name, prop_device_id, product_code AS
        SELECT
            {build_target_select_sql("s")}
        FROM `{staging_table_id}` s
        """
    client.query(query).result()


def validate_changed_target(
    staging_table_id: str,
    pending_keys_table_id: str,
    changed_table_id: str,
) -> None:
    target_table_id = table_id(TARGET_TABLE)
    current_rows = 0
    replaced_rows = 0
    if table_exists(target_table_id):
        current_rows = get_table_row_count(target_table_id)
        replaced_query = f"""
        SELECT COUNT(*) AS row_count
        FROM `{target_table_id}`
        WHERE oss_key IN (
            SELECT oss_key
            FROM `{pending_keys_table_id}`
        )
        """
        replaced_rows = int(next(iter(client.query(replaced_query).result())).row_count)

    staging_rows = get_table_row_count(staging_table_id)
    changed_rows = get_table_row_count(changed_table_id)
    expected_rows = current_rows - replaced_rows + staging_rows
    if changed_rows != expected_rows:
        raise RuntimeError(
            "Makepod changed 表行数校验失败："
            f"expected={expected_rows}, actual={changed_rows}"
        )


def swap_table(changed_table_id: str, target_table_name: str) -> None:
    target_table_id = table_id(target_table_name)
    if table_exists(target_table_id):
        client.query(f"DROP TABLE `{target_table_id}`").result()
    try:
        client.query(
            f"ALTER TABLE `{changed_table_id}` RENAME TO {target_table_name}"
        ).result()
    except Exception:
        if not table_exists(target_table_id) and table_exists(changed_table_id):
            logging.error(
                "目标表交换失败，保留恢复表: %s",
                changed_table_id,
            )
        raise


def mark_processed_keys(
    pending_keys_table_id: str,
    process_changed_table_id: str,
) -> None:
    process_table_id = table_id(PROCESS_LOG_TABLE)
    query = f"""
    CREATE TABLE `{process_changed_table_id}` AS
    SELECT * REPLACE (
        CASE
            WHEN oss_key IN (
                SELECT oss_key
                FROM `{pending_keys_table_id}`
            )
            THEN 1
            ELSE makepod_analyze_state
        END AS makepod_analyze_state
    )
    FROM `{process_table_id}`
    """
    client.query(query).result()

    original_rows = get_table_row_count(process_table_id)
    changed_rows = get_table_row_count(process_changed_table_id)
    if changed_rows != original_rows:
        raise RuntimeError(
            "oss_key_process_log changed 表行数校验失败："
            f"expected={original_rows}, actual={changed_rows}"
        )

    swap_table(process_changed_table_id, PROCESS_LOG_TABLE)


def cleanup_table(full_table_id: str) -> None:
    try:
        client.delete_table(full_table_id, not_found_ok=True)
        logging.info("已清理临时表: %s", full_table_id)
    except Exception as exc:  # pragma: no cover - 仅做兜底清理
        logging.warning("清理临时表失败 %s: %s", full_table_id, exc)


def run() -> None:
    run_suffix = build_run_suffix()
    pending_keys_table_id = table_id(f"{TARGET_TABLE}_pending_keys_{run_suffix}")
    staging_table_id = table_id(f"{TARGET_TABLE}_tmp_{run_suffix}")
    changed_table_id = table_id(f"{TARGET_TABLE}_changed_{run_suffix}")
    process_changed_table_id = table_id(f"{PROCESS_LOG_TABLE}_changed_{run_suffix}")

    logging.info("开始处理: %s", TARGET_TABLE)
    logging.info("本次待处理 key 表: %s", pending_keys_table_id)
    logging.info("本次 staging 表: %s", staging_table_id)
    logging.info("本次 changed 表: %s", changed_table_id)

    try:
        client.query(build_pending_keys_query(pending_keys_table_id)).result()
        pending_key_count = get_table_row_count(pending_keys_table_id)
        if pending_key_count == 0:
            logging.info("没有新的 Makepod 日志文件需要处理")
            return

        client.query(
            build_staging_query(staging_table_id, pending_keys_table_id)
        ).result()
        stats = validate_staging(staging_table_id)

        logging.info(
            "staging 校验通过: pending_keys=%s, rows=%s, raw_events=%s, event_oss_keys=%s",
            pending_key_count,
            stats["total_rows"],
            stats["distinct_raw_event_ids"],
            stats["distinct_oss_keys"],
        )

        build_changed_target(
            staging_table_id,
            pending_keys_table_id,
            changed_table_id,
        )
        validate_changed_target(
            staging_table_id,
            pending_keys_table_id,
            changed_table_id,
        )
        swap_table(changed_table_id, TARGET_TABLE)

        mark_processed_keys(pending_keys_table_id, process_changed_table_id)
        logging.info(
            "%s 刷新完成，本次处理 %s 个 key，写入 %s 行",
            TARGET_TABLE,
            pending_key_count,
            stats["total_rows"],
        )
    finally:
        cleanup_table(pending_keys_table_id)
        cleanup_table(staging_table_id)
        if table_exists(table_id(TARGET_TABLE)):
            cleanup_table(changed_table_id)
        if table_exists(table_id(PROCESS_LOG_TABLE)):
            cleanup_table(process_changed_table_id)


if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)
    run()
    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 执行完成，耗时 %.1f 秒", TARGET_TABLE, elapsed)
