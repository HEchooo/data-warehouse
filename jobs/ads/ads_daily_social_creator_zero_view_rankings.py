import argparse
from datetime import date, datetime, timezone
import logging

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from zoneinfo import ZoneInfo


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "ads_daily_social_creator_zero_view_rankings"
SOURCE_TABLE_NAME = "dws_social_creator_view_fluctuation_daily"
INCREMENTAL_WINDOW_HOURS = 6
# 命中条件：最近 3 个发布日的 T+1 播放全为 0。
ZERO_WINDOW_DAY_COUNT = 3

client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="构建 Social Content 连续零播放账号日榜"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="按播放波动中间表的完整统计日期历史重刷",
    )
    return parser.parse_args()


def table_id(table_name: str) -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{table_name}"


def target_table_exists() -> bool:
    try:
        client.get_table(table_id(TABLE_NAME))
        return True
    except NotFound:
        return False


def get_target_row_count() -> int:
    if not target_table_exists():
        return 0
    rows = list(
        client.query(f"SELECT COUNT(*) AS row_count FROM `{table_id(TABLE_NAME)}`").result()
    )
    return int(rows[0].row_count) if rows else 0


def get_full_dates_to_process() -> list[date]:
    query = f"""
    SELECT DISTINCT dt
    FROM `{table_id(SOURCE_TABLE_NAME)}`
    WHERE dt < CURRENT_DATE('{TORONTO_TZ}')
    ORDER BY dt
    """
    return [row.dt for row in client.query(query).result()]


def get_dates_to_process() -> list[date]:
    if not target_table_exists() or get_target_row_count() == 0:
        return get_full_dates_to_process()

    query = f"""
    WITH
    -- 中间表某统计日刷新后，其对应榜单日需要重算。
    changed_dates AS (
        SELECT DISTINCT dt
        FROM `{table_id(SOURCE_TABLE_NAME)}`
        WHERE update_time >= TIMESTAMP_SUB(
            CURRENT_TIMESTAMP(), INTERVAL {INCREMENTAL_WINDOW_HOURS} HOUR
        )
    ),
    -- 目标表最大统计日之后、中间表已就绪的新统计日需要补齐。
    missing_dates AS (
        SELECT dt
        FROM `{table_id(SOURCE_TABLE_NAME)}`
        WHERE dt > (SELECT MAX(dt) FROM `{table_id(TABLE_NAME)}`)
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt FROM changed_dates
        UNION ALL
        SELECT dt FROM missing_dates
    )
    WHERE dt < CURRENT_DATE('{TORONTO_TZ}')
    ORDER BY dt
    """
    return [row.dt for row in client.query(query).result()]


def dates_to_array_sql(dates: list[date]) -> str:
    return ", ".join(f"DATE '{stat_dt}'" for stat_dt in dates)


def build_select_sql(dates: list[date]) -> str:
    dates_sql = dates_to_array_sql(dates)
    return f"""
    WITH
    -- 读取待处理统计日的播放波动准入中间表。
    fluctuation AS (
        SELECT *
        FROM `{table_id(SOURCE_TABLE_NAME)}`
        WHERE dt IN UNNEST([{dates_sql}])
    ),
    -- 仅保留最近 3 个发布日 T+1 播放全为 0 的账号。
    -- 观察发布日恒为 D-1，因此账号只在发视频的次日被评估一次。
    zero_view AS (
        SELECT *
        FROM fluctuation
        WHERE recent_3day_zero_day_count = {ZERO_WINDOW_DAY_COUNT}
    )
    -- 榜单排序由展示层（chart）按历史基准日均完成，此处不存储排名字段。
    SELECT
        dt,
        pool_window_start_dt,
        pool_window_end_dt,
        channel,
        creator_id,
        account_name,
        account_handle,
        account_url,
        -- 零播放观察窗口：最近 3 个发布日，起点为其中最早的发布日。
        recent_3day_start_dt AS zero_window_start_dt,
        recent_end_dt AS zero_window_end_dt,
        {ZERO_WINDOW_DAY_COUNT} AS zero_window_day_count,
        baseline_start_dt,
        baseline_end_dt,
        baseline_active_day_count,
        baseline_video_count,
        baseline_t1_view_count,
        CAST(baseline_t1_avg_view_count AS NUMERIC) AS baseline_t1_avg_view_count,
        recent_start_dt,
        recent_end_dt,
        recent_video_count,
        recent_t1_view_count,
        CAST(recent_t1_avg_view_count AS NUMERIC) AS recent_t1_avg_view_count,
        pool_published_video_count,
        CAST(weekly_avg_published_video_count AS NUMERIC)
            AS weekly_avg_published_video_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM zero_view
    """


def validate(staging_table: str) -> None:
    result = list(
        client.query(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT CONCAT(CAST(dt AS STRING), '|', channel, '|', creator_id))
                    AS unique_account_count,
                COUNTIF(zero_window_day_count != {ZERO_WINDOW_DAY_COUNT})
                    AS invalid_window_day_count,
                -- 零播放账号的观察发布日播放量必须为 0。
                COUNTIF(recent_t1_avg_view_count != 0
                    OR recent_t1_view_count != 0)
                    AS invalid_zero_view_count,
                COUNTIF(zero_window_start_dt IS NULL
                    OR zero_window_end_dt IS NULL
                    OR zero_window_start_dt > zero_window_end_dt
                    OR zero_window_end_dt != DATE_SUB(dt, INTERVAL 1 DAY))
                    AS invalid_zero_window_count,
                COUNTIF(baseline_start_dt > baseline_end_dt
                    OR recent_start_dt > recent_end_dt
                    OR baseline_end_dt > recent_start_dt)
                    AS invalid_window_count
            FROM `{staging_table}`
            """
        ).result()
    )[0]
    if result.row_count != result.unique_account_count:
        raise RuntimeError("连续零播放账号榜校验失败：统计日、渠道和账号不唯一")
    if result.invalid_window_day_count != 0:
        raise RuntimeError("连续零播放账号榜校验失败：零播放观察窗口发布日数错误")
    if result.invalid_zero_view_count != 0:
        raise RuntimeError("连续零播放账号榜校验失败：存在观察发布日播放量非零的账号")
    if result.invalid_zero_window_count != 0:
        raise RuntimeError("连续零播放账号榜校验失败：零播放发布日区间错误")
    if result.invalid_window_count != 0:
        raise RuntimeError("连续零播放账号榜校验失败：日期窗口错误")

    # 交叉核对：榜单键集必须与中间表「最近 3 个发布日全为 0」的账号完全一致。
    # 仅校验观察发布日播放为 0 无法覆盖另外两个发布日。
    diff = list(
        client.query(
            f"""
            WITH
            staging_keys AS (
                SELECT dt, channel, creator_id FROM `{staging_table}`
            ),
            source_keys AS (
                SELECT dt, channel, creator_id
                FROM `{table_id(SOURCE_TABLE_NAME)}`
                WHERE dt IN (SELECT DISTINCT dt FROM staging_keys)
                    AND recent_3day_zero_day_count = {ZERO_WINDOW_DAY_COUNT}
            )
            SELECT
                (SELECT COUNT(*) FROM (
                    SELECT * FROM staging_keys
                    EXCEPT DISTINCT
                    SELECT * FROM source_keys
                )) AS in_staging_not_source,
                (SELECT COUNT(*) FROM (
                    SELECT * FROM source_keys
                    EXCEPT DISTINCT
                    SELECT * FROM staging_keys
                )) AS in_source_not_staging
            """
        ).result()
    )[0]
    if diff.in_staging_not_source != 0 or diff.in_source_not_staging != 0:
        raise RuntimeError(
            "连续零播放账号榜校验失败：榜单键集与中间表零播放账号不一致"
            f"（榜单多 {diff.in_staging_not_source} 行，中间表多 {diff.in_source_not_staging} 行）"
        )


def run(dates: list[date], full_refresh: bool = False) -> None:
    if not dates:
        logging.info("%s 无待处理日期，跳过", TABLE_NAME)
        return

    target_table = table_id(TABLE_NAME)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    staging_table = f"{target_table}_tmp_{run_id}"
    changed_table = f"{target_table}_changed_{run_id}"
    target_dropped = False
    dates_sql = dates_to_array_sql(dates)

    try:
        client.query(
            f"""
            CREATE TABLE `{staging_table}`
            PARTITION BY dt
            CLUSTER BY channel, creator_id AS
            {build_select_sql(dates)}
            """
        ).result()

        validate(staging_table)

        if target_table_exists():
            if full_refresh:
                client.query(
                    f"""
                    CREATE TABLE `{changed_table}`
                    PARTITION BY dt
                    CLUSTER BY channel, creator_id AS
                    SELECT * FROM `{staging_table}`
                    """
                ).result()
            else:
                client.query(
                    f"""
                    CREATE TABLE `{changed_table}`
                    PARTITION BY dt
                    CLUSTER BY channel, creator_id AS
                    SELECT * FROM `{target_table}`
                    WHERE dt NOT IN UNNEST([{dates_sql}])

                    UNION ALL BY NAME

                    SELECT * FROM `{staging_table}`
                    """
                ).result()
            client.query(f"DROP TABLE `{target_table}`").result()
            target_dropped = True
            client.query(f"ALTER TABLE `{changed_table}` RENAME TO {TABLE_NAME}").result()
        else:
            client.query(
                f"""
                CREATE TABLE `{target_table}`
                PARTITION BY dt
                CLUSTER BY channel, creator_id AS
                SELECT * FROM `{staging_table}`
                """
            ).result()

        client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
    except Exception:
        if target_dropped:
            try:
                client.get_table(target_table)
            except NotFound:
                client.query(
                    f"ALTER TABLE `{changed_table}` RENAME TO {TABLE_NAME}"
                ).result()
        else:
            client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
            client.query(f"DROP TABLE IF EXISTS `{changed_table}`").result()
        raise

    logging.info(
        "%s 刷新完成，统计日期: %s ~ %s，共 %s 天",
        TABLE_NAME,
        min(dates),
        max(dates),
        len(dates),
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    started_at = datetime.now(ZoneInfo(TORONTO_TZ))
    args = parse_args()
    dates = get_full_dates_to_process() if args.full_refresh else get_dates_to_process()
    run(dates, full_refresh=args.full_refresh)
    elapsed = (datetime.now(ZoneInfo(TORONTO_TZ)) - started_at).total_seconds()
    logging.info("%s 完成，耗时: %.1f 秒", TABLE_NAME, elapsed)
