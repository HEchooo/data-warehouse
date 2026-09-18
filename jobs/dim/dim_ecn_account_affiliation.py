"""DIM ECN 账号联盟归属 ETL。

ODS 是当天接口快照，封号后会变成空标签。本表只插入尚未见过的成功绑定，已有地区永不覆盖。
"""

import logging
from datetime import datetime, timezone

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TABLE_NAME = "dim_ecn_account_affiliation"
ODS_TABLE_NAME = "ods_ecn_account_affiliation"
SCHEMA = [
    bigquery.SchemaField("platform", "STRING"),
    bigquery.SchemaField("account_handle", "STRING"),
    bigquery.SchemaField("affiliation", "STRING"),
    bigquery.SchemaField("affiliation_label", "STRING"),
    bigquery.SchemaField("first_bound_at", "TIMESTAMP"),
    bigquery.SchemaField("update_time", "TIMESTAMP"),
]
HANDLE_KEY = "LOWER(LTRIM({alias}.account_handle, '@'))"
# 接口已经 404、但业务确认过地区的账号。只在维表还没有时补上，不覆盖已有绑定。
MANUAL_US_HANDLES = (
    "alba_romero7x",
    "amelie_rousseau7x",
    "callie_ventura",
    "devon_nash7x",
    "elara_mae7x",
    "evie_blush",
    "gillian_stanton",
    "lacey_quinn7x",
    "liana_skye7x",
    "livia_blake",
    "madison_kelly7x",
    "matilda_fern7x",
    "ramona_rouge",
    "selena_medina7x",
    "summer_waverly",
    "trixie_retro",
    "zoey_nelson7x",
    "chloe.bennett656",
    "chloe_davis_1",
    "chloedavies7x",
    "makepodcustom",
    "makepodcustom1",
    "morganpaige7x",
    "ninachopra7x",
    "styledbysophie_2",
    "user2228616398520",
)
MANUAL_MX_HANDLES = (
    "nguynkiuqunh6297",
    "ashleyguo9",
    "geraldinebein8",
    "jjh.245e",
    "pedroshum4983",
)

client = bigquery.Client(project=PROJECT_ID)


def table_id(table_name: str = TABLE_NAME) -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{table_name}"


def target_table_exists() -> bool:
    try:
        client.get_table(table_id())
        return True
    except NotFound:
        return False


def ensure_table() -> None:
    if target_table_exists():
        return
    table = bigquery.Table(table_id(), schema=SCHEMA)
    table.clustering_fields = ["platform", "account_handle"]
    client.create_table(table)
    logging.info("已创建空表 %s", TABLE_NAME)


def handle_key(alias: str) -> str:
    return HANDLE_KEY.format(alias=alias)


def handles_sql(handles: tuple[str, ...]) -> str:
    return ", ".join(f"'{handle}'" for handle in handles)


def new_bindings_sql() -> str:
    return f"""
    WITH
    -- 当天接口已绑上的标签优先；接口 404 的账号用人工确认过的地区补上。
    bound AS (
        SELECT * EXCEPT(rn)
        FROM (
            SELECT
                platform,
                account_handle,
                affiliation,
                affiliation_label,
                first_bound_at,
                ROW_NUMBER() OVER (
                    PARTITION BY platform, {handle_key("src")}
                    ORDER BY priority, first_bound_at DESC
                ) AS rn
            FROM (
                SELECT
                    platform,
                    account_handle,
                    affiliation,
                    affiliation_label,
                    fetched_at AS first_bound_at,
                    0 AS priority
                FROM `{table_id(ODS_TABLE_NAME)}` ods
                WHERE NULLIF(platform, '') IS NOT NULL
                    AND NULLIF(account_handle, '') IS NOT NULL
                    AND NULLIF(affiliation_label, '') IS NOT NULL

                UNION ALL

                SELECT
                    'tiktok' AS platform,
                    handle AS account_handle,
                    'affiliate_us' AS affiliation,
                    '联盟美国' AS affiliation_label,
                    CURRENT_TIMESTAMP() AS first_bound_at,
                    1 AS priority
                FROM UNNEST([{handles_sql(MANUAL_US_HANDLES)}]) AS handle

                UNION ALL

                SELECT
                    'tiktok' AS platform,
                    handle AS account_handle,
                    'affiliate_mexico' AS affiliation,
                    '联盟墨西哥' AS affiliation_label,
                    CURRENT_TIMESTAMP() AS first_bound_at,
                    1 AS priority
                FROM UNNEST([{handles_sql(MANUAL_MX_HANDLES)}]) AS handle
            ) src
        )
        WHERE rn = 1
    )
    -- 维表还没有的成功绑定；已有行即使 ODS 变空也不再进入。
    SELECT
        bound.platform,
        bound.account_handle,
        bound.affiliation,
        bound.affiliation_label,
        bound.first_bound_at,
        CURRENT_TIMESTAMP() AS update_time
    FROM bound
    LEFT JOIN `{table_id()}` dim
        ON dim.platform = bound.platform
        AND {handle_key("dim")} = {handle_key("bound")}
    WHERE dim.account_handle IS NULL
    """


def validate_table(table_name: str, label: str) -> None:
    result = list(
        client.query(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT CONCAT(platform, '|', {handle_key("t")})) AS unique_key_count,
                COUNTIF(platform IS NULL OR account_handle IS NULL) AS null_key_count,
                COUNTIF(NULLIF(affiliation_label, '') IS NULL) AS null_label_count
            FROM `{table_name}` t
            """
        ).result()
    )[0]
    if result.row_count != result.unique_key_count:
        raise RuntimeError(f"ECN 账号联盟归属维表校验失败：{label} platform + account_handle 不唯一")
    if result.null_key_count:
        raise RuntimeError(f"ECN 账号联盟归属维表校验失败：{label} 主键字段存在空值")
    if result.null_label_count:
        raise RuntimeError(f"ECN 账号联盟归属维表校验失败：{label} affiliation_label 存在空值")


def validate_ods_covered(dim_table: str) -> None:
    missing = list(
        client.query(
            f"""
            SELECT COUNT(*) AS missing_count
            FROM `{table_id(ODS_TABLE_NAME)}` ods
            LEFT JOIN `{dim_table}` dim
                ON dim.platform = ods.platform
                AND {handle_key("dim")} = {handle_key("ods")}
            WHERE NULLIF(ods.affiliation_label, '') IS NOT NULL
                AND dim.account_handle IS NULL
            """
        ).result()
    )[0].missing_count
    if missing:
        raise RuntimeError(
            f"ECN 账号联盟归属维表校验失败：ODS 成功绑定有 {missing} 条未落入维表"
        )


def validate_manual_covered(dim_table: str) -> None:
    missing = list(
        client.query(
            f"""
            SELECT COUNT(*) AS missing_count
            FROM UNNEST([{handles_sql(MANUAL_US_HANDLES + MANUAL_MX_HANDLES)}]) AS handle
            LEFT JOIN `{dim_table}` dim
                ON dim.platform = 'tiktok'
                AND {handle_key("dim")} = LOWER(LTRIM(handle, '@'))
            WHERE dim.account_handle IS NULL
            """
        ).result()
    )[0].missing_count
    if missing:
        raise RuntimeError(
            f"ECN 账号联盟归属维表校验失败：人工确认的账号有 {missing} 条未落入维表"
        )


def run() -> None:
    ensure_table()
    target_table = table_id()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    staging_table = f"{target_table}_tmp_{run_id}"
    changed_table = f"{target_table}_changed_{run_id}"
    target_dropped = False

    try:
        client.query(
            f"""
            CREATE TABLE `{staging_table}`
            CLUSTER BY platform, account_handle AS
            {new_bindings_sql()}
            """
        ).result()
        staging_count = list(
            client.query(f"SELECT COUNT(*) AS row_count FROM `{staging_table}`").result()
        )[0].row_count
        if staging_count == 0:
            validate_table(target_table, "目标表")
            validate_ods_covered(target_table)
            validate_manual_covered(target_table)
            logging.info("%s 无新绑定，保持 %s 不变", TABLE_NAME, TABLE_NAME)
            return

        validate_table(staging_table, "staging")
        client.query(
            f"""
            CREATE TABLE `{changed_table}`
            CLUSTER BY platform, account_handle AS
            SELECT * FROM `{target_table}`
            UNION ALL
            SELECT * FROM `{staging_table}`
            """
        ).result()
        validate_table(changed_table, "changed")
        validate_ods_covered(changed_table)
        validate_manual_covered(changed_table)
        client.query(f"DROP TABLE `{target_table}`").result()
        target_dropped = True
        client.query(f"ALTER TABLE `{changed_table}` RENAME TO {TABLE_NAME}").result()
        target_dropped = False
    except Exception:
        if target_dropped:
            logging.error("目标表已删除但换名未完成，changed 表保留待恢复: %s", changed_table)
        else:
            client.query(f"DROP TABLE IF EXISTS `{changed_table}`").result()
        raise
    finally:
        client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()

    logging.info("%s 刷新完成, 新增绑定=%d", TABLE_NAME, staging_count)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    started_at = datetime.now(timezone.utc)
    run()
    logging.info(
        "%s 完成，耗时: %.1f 秒",
        TABLE_NAME,
        (datetime.now(timezone.utc) - started_at).total_seconds(),
    )
