import argparse
from datetime import date, datetime, timedelta, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "ads_daily_social_content_published_day3_view_bucket_v2"
SOURCE_TABLE_NAME = "dws_social_video_state_daily_v2"
DIM_TABLE_NAME = "dim_video"
AUTO_PUBLISH_DIM_TABLE_NAME = "dim_auto_publish_video_task"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "ads" / TABLE_NAME
DISPLAY_CHANNELS = ["全部渠道", "youtube", "tiktok", "ins"]
PUBLISHED_HORIZON_DAY = 3
SOCIAL_CONTENT_BUCKET_REBUILD_DAYS = 30
YOUTUBE_EXCLUDED_CHANNEL_IDS = (
    "UCynAg7WHdiy8SuQpESO__yw",
    "UCgBJozWjiE0cqwv7UNalyog",
    "UC4rj_IlO3V-d5ebVhgagh1w",
    "UC70Y2-yeuFOoTx6yNkqioKA",
    "UCiX5DzRDNIb9fH_MQvoIN7w",
    "UCLs3oc5eyptqVTEnXaIwHvQ",
    "UCPAQ09Nusb_OZzwfWGAE_Aw",
    "UCV5FBW_l8cayS8L_zAIvlkQ",
    "UCVsLt_8hbY5nG5da8sslD_w",
    "UCzIQMkLCHa63JekjxaA133w",
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="构建 Social Content v2 发布后 3 天累计播放分桶日报（dt × channel × bucket）"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="按 v2 视频状态表可成熟历史全量重刷，而不是默认最近 30 个成熟 cohort 日期",
    )
    return parser.parse_args()


def dates_to_sql_list(dates: list[date]) -> str:
    return ", ".join([f"'{d}'" for d in dates])


def dates_to_array_sql(dates: list[date]) -> str:
    return ", ".join([f"DATE '{d}'" for d in dates])


def strings_to_array_sql(values: list[str] | tuple[str, ...]) -> str:
    return ", ".join([f"'{value}'" for value in values])


def target_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


def source_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{SOURCE_TABLE_NAME}"


def dim_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{DIM_TABLE_NAME}"


def auto_publish_dim_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{AUTO_PUBLISH_DIM_TABLE_NAME}"


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


def get_source_date_bounds() -> tuple[date | None, date | None]:
    query = f"""
    SELECT
        MIN(published_dt) AS min_published_dt,
        MAX(dt) AS max_state_dt
    FROM `{source_table_id()}`
    """
    rows = list(client.query(query).result())
    if not rows:
        return None, None

    row = rows[0]
    return row.min_published_dt, row.max_state_dt


def get_social_content_bucket_dates_to_process(
    rebuild_days: int = SOCIAL_CONTENT_BUCKET_REBUILD_DAYS,
) -> list[date]:
    """
    分桶图依赖发布 cohort 的 T+3 累计播放，且“全部渠道”还要复用自动发布去重。

    规则：
    - 目标表不存在或为空时，按 source 已成熟历史全量初始化
    - 目标表已存在时，固定重刷最近 30 个成熟 cohort 日期
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0
    if is_full_snapshot:
        return get_social_content_bucket_full_dates_to_process()

    min_published_dt, max_state_dt = get_source_date_bounds()
    if min_published_dt is None or max_state_dt is None:
        return []

    mature_end_dt = max_state_dt - timedelta(days=PUBLISHED_HORIZON_DAY)
    if mature_end_dt < min_published_dt:
        return []

    start_dt = max(mature_end_dt - timedelta(days=rebuild_days - 1), min_published_dt)
    total_days = (mature_end_dt - start_dt).days + 1
    return [start_dt + timedelta(days=offset) for offset in range(total_days)]


def get_social_content_bucket_full_dates_to_process() -> list[date]:
    min_published_dt, max_state_dt = get_source_date_bounds()
    if min_published_dt is None or max_state_dt is None:
        return []

    mature_end_dt = max_state_dt - timedelta(days=PUBLISHED_HORIZON_DAY)
    if mature_end_dt < min_published_dt:
        return []

    total_days = (mature_end_dt - min_published_dt).days + 1
    return [
        min_published_dt + timedelta(days=offset) for offset in range(total_days)
    ]


def build_select_sql(dates: list[date]) -> str:
    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    channels_array_sql = strings_to_array_sql(DISPLAY_CHANNELS)
    youtube_excluded_channel_ids_sql = strings_to_array_sql(
        YOUTUBE_EXCLUDED_CHANNEL_IDS
    )
    min_date = min(dates)
    max_date = max(dates)

    return f"""
    WITH
    -- 本次待产出的成熟发布时间 cohort 日期窗口。
    params AS (
        SELECT
            DATE('{min_date}') AS min_dt,
            DATE('{max_date}') AS max_dt,
            DATE_ADD(DATE('{max_date}'), INTERVAL {PUBLISHED_HORIZON_DAY} DAY)
                AS max_observation_dt
    ),
    -- 目标 cohort 日期列表。
    target_dates AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 固定产出 4 个展示渠道。
    channel_list AS (
        SELECT channel
        FROM UNNEST([{channels_array_sql}]) AS channel
    ),
    -- 固定产出展示层使用的 7 个播放分桶。
    bucket_list AS (
        SELECT 1 AS published_day3_view_bucket_order, '0' AS published_day3_view_bucket
        UNION ALL
        SELECT 2, '(0,100]'
        UNION ALL
        SELECT 3, '(100,500]'
        UNION ALL
        SELECT 4, '(500,1000]'
        UNION ALL
        SELECT 5, '(1000,3000]'
        UNION ALL
        SELECT 6, '(3000,10000]'
        UNION ALL
        SELECT 7, '(10000,+∞)'
    ),
    -- 读取成熟 cohort 所需的 T+3 视频状态，并沿用 v2 的 YouTube 排除规则。
    social_video_state AS (
        SELECT
            dt,
            channel,
            creator_id,
            video_key,
            published_dt,
            day_end_views
        FROM `{source_table_id()}`
        WHERE dt BETWEEN DATE_ADD((SELECT min_dt FROM params), INTERVAL {PUBLISHED_HORIZON_DAY} DAY)
            AND (SELECT max_observation_dt FROM params)
            AND published_dt BETWEEN (SELECT min_dt FROM params) AND (SELECT max_dt FROM params)
            AND NOT (
                channel = 'youtube'
                AND creator_id IN ({youtube_excluded_channel_ids_sql})
            )
    ),
    -- 单渠道直接按原始 video_key 取“发布时间 + 3 天”的累计播放。
    base_channel_video_day3_view AS (
        SELECT
            published_dt AS dt,
            channel,
            video_key,
            day_end_views AS published_day3_view_count
        FROM social_video_state
        WHERE dt = DATE_ADD(published_dt, INTERVAL {PUBLISHED_HORIZON_DAY} DAY)
    ),
    -- 自动发布统一视频映射从 DIM 读取，避免在脚本内重复维护。
    auto_publish_video_task_dim AS (
        SELECT
            video_key,
            unified_video_key
        FROM `{auto_publish_dim_table_id()}`
    ),
    -- 为“全部渠道”发布时间 cohort 口径准备统一视频键与 T+3 观察日播放。
    all_channel_published_video_base AS (
        SELECT
            s.dt AS observation_dt,
            s.video_key,
            s.day_end_views,
            COALESCE(m.unified_video_key, s.video_key) AS all_channel_video_key
        FROM social_video_state s
        LEFT JOIN auto_publish_video_task_dim m
            ON s.video_key = m.video_key
    ),
    -- 只回看本批涉及的统一视频键，避免无谓扫描 dim_video 全量分组。
    relevant_all_channel_video_keys AS (
        SELECT DISTINCT all_channel_video_key
        FROM all_channel_published_video_base
    ),
    -- “全部渠道”沿用现有 v2 发布时间 cohort 逻辑：按统一视频全历史最早发布时间对齐 cohort。
    all_channel_video_first_published_dt AS (
        SELECT
            COALESCE(m.unified_video_key, d.video_key) AS all_channel_video_key,
            MIN(d.published_dt) AS first_published_dt
        FROM `{dim_table_id()}` d
        LEFT JOIN auto_publish_video_task_dim m
            ON d.video_key = m.video_key
        INNER JOIN relevant_all_channel_video_keys r
            ON COALESCE(m.unified_video_key, d.video_key) = r.all_channel_video_key
        WHERE d.published_dt IS NOT NULL
            AND d.published_dt <= (SELECT max_dt FROM params)
        GROUP BY all_channel_video_key
    ),
    -- 统一视频在 T+3 观察日把各平台累计播放汇总后，再作为“全部渠道”的单个逻辑视频落桶。
    all_channel_video_day3_view AS (
        SELECT
            f.first_published_dt AS dt,
            '全部渠道' AS channel,
            b.all_channel_video_key AS video_key,
            COALESCE(SUM(b.day_end_views), 0) AS published_day3_view_count
        FROM all_channel_published_video_base b
        INNER JOIN all_channel_video_first_published_dt f
            ON b.all_channel_video_key = f.all_channel_video_key
        WHERE f.first_published_dt IN ({dates_str})
            AND DATE_DIFF(
                b.observation_dt,
                f.first_published_dt,
                DAY
            ) = {PUBLISHED_HORIZON_DAY}
        GROUP BY f.first_published_dt, b.all_channel_video_key
    ),
    -- 合并真实渠道与“全部渠道”的视频级 T+3 累计播放结果。
    video_day3_view_base AS (
        SELECT
            dt,
            channel,
            video_key,
            published_day3_view_count
        FROM base_channel_video_day3_view

        UNION ALL

        SELECT
            dt,
            channel,
            video_key,
            published_day3_view_count
        FROM all_channel_video_day3_view
    ),
    -- 用严格开闭区间把每个视频归到唯一 bucket。
    bucketed_video_day3_view AS (
        SELECT
            dt,
            channel,
            video_key,
            CASE
                WHEN published_day3_view_count = 0 THEN 1
                WHEN published_day3_view_count <= 100 THEN 2
                WHEN published_day3_view_count <= 500 THEN 3
                WHEN published_day3_view_count <= 1000 THEN 4
                WHEN published_day3_view_count <= 3000 THEN 5
                WHEN published_day3_view_count <= 10000 THEN 6
                ELSE 7
            END AS published_day3_view_bucket_order,
            CASE
                WHEN published_day3_view_count = 0 THEN '0'
                WHEN published_day3_view_count <= 100 THEN '(0,100]'
                WHEN published_day3_view_count <= 500 THEN '(100,500]'
                WHEN published_day3_view_count <= 1000 THEN '(500,1000]'
                WHEN published_day3_view_count <= 3000 THEN '(1000,3000]'
                WHEN published_day3_view_count <= 10000 THEN '(3000,10000]'
                ELSE '(10000,+∞)'
            END AS published_day3_view_bucket
        FROM video_day3_view_base
    ),
    -- 统计每个 cohort 日期、渠道、bucket 内的视频数量。
    bucket_metrics AS (
        SELECT
            dt,
            channel,
            published_day3_view_bucket_order,
            published_day3_view_bucket,
            COUNT(DISTINCT video_key) AS video_count
        FROM bucketed_video_day3_view
        GROUP BY
            dt,
            channel,
            published_day3_view_bucket_order,
            published_day3_view_bucket
    ),
    -- 脚手架保证每个成熟日期都产出完整 bucket 维度，便于 Superset 稳定堆积。
    target_bucket_keys AS (
        SELECT
            d.dt,
            c.channel,
            b.published_day3_view_bucket_order,
            b.published_day3_view_bucket
        FROM target_dates d
        CROSS JOIN channel_list c
        CROSS JOIN bucket_list b
    )
    SELECT
        k.dt,
        k.channel,
        k.published_day3_view_bucket_order,
        k.published_day3_view_bucket,
        COALESCE(m.video_count, 0) AS video_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM target_bucket_keys k
    LEFT JOIN bucket_metrics m
        ON k.dt = m.dt
        AND k.channel = m.channel
        AND k.published_day3_view_bucket_order = m.published_day3_view_bucket_order
    """


def run_ads_daily_social_content_published_day3_view_bucket(dates: list[date]) -> None:
    if not dates:
        logging.info("没有新的 %s 日期需要处理", TABLE_NAME)
        return

    if not target_table_exists():
        raise RuntimeError(f"目标表不存在，请先执行 DDL: {target_table_id()}")

    min_date = min(dates)
    max_date = max(dates)
    target_table = target_table_id()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    staging_table = f"{target_table}_tmp_{run_id}"
    changed_table = f"{target_table}_changed_{run_id}"
    target_dropped = False
    insert_staging_sql = f"""
    INSERT INTO `{staging_table}`
    (
        dt,
        channel,
        published_day3_view_bucket_order,
        published_day3_view_bucket,
        video_count,
        update_time
    )
    {build_select_sql(dates)}
    """
    validate_staging_sql = f"""
    SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT dt) AS dt_count,
        COUNT(DISTINCT CONCAT(
            CAST(dt AS STRING),
            '|',
            channel,
            '|',
            CAST(published_day3_view_bucket_order AS STRING)
        )) AS unique_dt_channel_bucket_count
    FROM `{staging_table}`
    """
    create_changed_table_sql = f"""
    CREATE TABLE `{changed_table}`
    PARTITION BY dt
    CLUSTER BY channel, published_day3_view_bucket_order AS
    SELECT *
    FROM `{target_table}`
    WHERE dt < DATE('{min_date}') OR dt > DATE('{max_date}')

    UNION ALL

    SELECT *
    FROM `{staging_table}`
    """

    logging.info(
        "开始处理 %s, 目标表: %s, 临时表: %s, 替换表: %s",
        TABLE_NAME,
        target_table,
        staging_table,
        changed_table,
    )
    try:
        client.query(
            f"""
            CREATE TABLE `{staging_table}` AS
            SELECT *
            FROM `{target_table}`
            WHERE 1 = 0
            """
        ).result()
        client.query(insert_staging_sql).result()

        validation_row = list(client.query(validate_staging_sql).result())[0]
        expected_rows = len(dates) * len(DISPLAY_CHANNELS) * 7
        if validation_row.row_count != expected_rows:
            raise ValueError(
                f"{TABLE_NAME} staging 行数校验失败: "
                f"expected={expected_rows}, actual={validation_row.row_count}"
            )
        if validation_row.dt_count != len(dates):
            raise ValueError(
                f"{TABLE_NAME} staging 日期数校验失败: "
                f"expected={len(dates)}, actual={validation_row.dt_count}"
            )
        if (
            validation_row.row_count
            != validation_row.unique_dt_channel_bucket_count
        ):
            raise ValueError(
                f"{TABLE_NAME} staging 主键校验失败: "
                f"row_count={validation_row.row_count}, "
                f"unique_dt_channel_bucket_count="
                f"{validation_row.unique_dt_channel_bucket_count}"
            )

        client.query(create_changed_table_sql).result()
        client.query(f"DROP TABLE `{target_table}`").result()
        target_dropped = True
        client.query(
            f"ALTER TABLE `{changed_table}` RENAME TO {TABLE_NAME}"
        ).result()
        target_dropped = False
        client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
    except Exception:
        if target_dropped:
            try:
                client.query(
                    f"ALTER TABLE `{changed_table}` RENAME TO {TABLE_NAME}"
                ).result()
            except Exception:
                logging.exception("目标表恢复失败，保留替换表: %s", changed_table)
        else:
            client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
            client.query(f"DROP TABLE IF EXISTS `{changed_table}`").result()
        raise

    logging.info("%s 刷新完成, 处理日期: %s", TABLE_NAME, dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    args = parse_args()
    if args.full_refresh:
        dates = get_social_content_bucket_full_dates_to_process()
    else:
        dates = get_social_content_bucket_dates_to_process()

    logging.info("待处理日期: %s", dates if dates else "[]")
    if not dates:
        logging.info("没有新的 %s 日期需要处理", TABLE_NAME)
        raise SystemExit(0)

    run_ads_daily_social_content_published_day3_view_bucket(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
