from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
V3_DATASET_ID = "v3_decom"
SHANGHAI_TZ = "Asia/Shanghai"
TABLE_NAME = "dim_makepod_user_channel"
LOG_DIR = PROJECT_ROOT / "logs" / "dim" / TABLE_NAME
TENANT_CODE = "T18002707"

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


def register_ts_expr() -> str:
    return "TIMESTAMP(gmt_create, 'UTC')"


def service_change_ts_expr() -> str:
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


def build_makepod_user_channel_case_sql(field_expr: str) -> str:
    return f"""
    CASE
        WHEN CAST({field_expr} AS STRING) = '2606091027981' THEN 'TikTok'
        WHEN CAST({field_expr} AS STRING) = '2606090368095' THEN 'Google'
        WHEN CAST({field_expr} AS STRING) = '2606093799415' THEN 'Facebook'
        WHEN CAST({field_expr} AS STRING) = '2607062764531' THEN 'TK-ELSA-AI'
        WHEN CAST({field_expr} AS STRING) = '2607063062524' THEN 'YOUTUBE-ELSA-AI'
        WHEN NULLIF(CAST({field_expr} AS STRING), '') IS NOT NULL
            THEN 'Other Channel'
        ELSE 'Organic'
    END
    """


def get_dim_makepod_user_channel_dates_to_process() -> list[date]:
    """
    按 dim_makepod_user_channel 自身水位判定待处理日期。

    规则：
    - 目标表不存在或为空时，全量初始化 Makepod 注册用户渠道历史日期
    - 目标表已存在时，只处理源表 update_time 晚于目标表最大 update_time 的注册日期
    """
    register_ts = register_ts_expr()
    change_ts = service_change_ts_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT DATE({register_ts}, '{SHANGHAI_TZ}') AS dt
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_info`
        WHERE tenant_code = '{TENANT_CODE}'
            AND NULLIF(CAST(user_id AS STRING), '') IS NOT NULL
            AND DATE({register_ts}, '{SHANGHAI_TZ}') IS NOT NULL
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT DATE({register_ts}, '{SHANGHAI_TZ}') AS dt
    FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_info`
    WHERE tenant_code = '{TENANT_CODE}'
        AND NULLIF(CAST(user_id AS STRING), '') IS NOT NULL
        AND DATE({register_ts}, '{SHANGHAI_TZ}') IS NOT NULL
        AND {change_ts} > (
            SELECT COALESCE(MAX(update_time), TIMESTAMP('1970-01-01'))
            FROM `{target_table_id()}`
        )
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dim_makepod_user_channel(dates: list[date]) -> None:
    """
    增量维护 Makepod 服务端用户渠道维表。
    渠道分类只对服务端用户维度生效，供注册、下单、支付口径复用。
    """
    if not dates:
        logging.info("没有新的 dim_makepod_user_channel 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    register_ts = register_ts_expr()
    change_ts = service_change_ts_expr()
    channel_sql = build_makepod_user_channel_case_sql("invite_user_id")

    query = f"""
    MERGE `{target_table_id()}` AS dim
    USING (
        WITH
        -- 提取本批注册日期内的用户渠道记录，并按用户保留最近一条。
        latest_user_channel AS (
            SELECT
                service_user_id,
                invite_user_id,
                channel,
                register_date
            FROM (
                SELECT
                    CAST(user_id AS STRING) AS service_user_id,
                    NULLIF(CAST(invite_user_id AS STRING), '') AS invite_user_id,
                    {channel_sql} AS channel,
                    DATE({register_ts}, '{SHANGHAI_TZ}') AS register_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY CAST(user_id AS STRING)
                        ORDER BY
                            {change_ts} DESC,
                            {register_ts} DESC
                    ) AS rn
                FROM `{PROJECT_ID}.{V3_DATASET_ID}.user_info`
                WHERE tenant_code = '{TENANT_CODE}'
                    AND NULLIF(CAST(user_id AS STRING), '') IS NOT NULL
                    AND DATE({register_ts}, '{SHANGHAI_TZ}') IN ({dates_str})
            )
            WHERE rn = 1
        )
        SELECT
            service_user_id,
            invite_user_id,
            channel,
            register_date
        FROM latest_user_channel
    ) AS src
    ON dim.service_user_id = src.service_user_id
    WHEN MATCHED AND (
        COALESCE(dim.invite_user_id, '') != COALESCE(src.invite_user_id, '')
        OR COALESCE(dim.channel, '') != COALESCE(src.channel, '')
        OR dim.register_date != src.register_date
    ) THEN
        UPDATE SET
            invite_user_id = src.invite_user_id,
            channel = src.channel,
            register_date = src.register_date,
            update_time = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            service_user_id,
            invite_user_id,
            channel,
            register_date,
            update_time
        )
        VALUES (
            src.service_user_id,
            src.invite_user_id,
            src.channel,
            src.register_date,
            CURRENT_TIMESTAMP()
        );
    """
    logging.info("开始处理: dim_makepod_user_channel")
    client.query(query).result()
    logging.info("dim_makepod_user_channel 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    start_time = datetime.now(timezone.utc)

    dates = get_dim_makepod_user_channel_dates_to_process()
    if not dates:
        logging.info("没有新的 dim_makepod_user_channel 数据需要处理")
        logging.info("日志文件: %s", log_path)
        raise SystemExit(0)

    logging.info("dim_makepod_user_channel 待处理日期: %s", dates)
    run_dim_makepod_user_channel(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dim_makepod_user_channel 执行完成, 耗时: %.1f 秒", elapsed)
    logging.info("日志文件: %s", log_path)
