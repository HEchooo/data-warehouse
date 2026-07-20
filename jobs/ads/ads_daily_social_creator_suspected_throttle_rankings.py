import argparse
from datetime import date, datetime, timezone
import logging

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from zoneinfo import ZoneInfo


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "ads_daily_social_creator_suspected_throttle_rankings"
SOURCE_TABLE_NAME = "dws_social_creator_view_fluctuation_daily"
INCREMENTAL_WINDOW_HOURS = 6
THRESHOLD_RATIO = 0.3

client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="构建 Social Content 疑似限流账号日榜"
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
    -- 仅保留最近发布日日均 T+1 播放低于历史基准 30% 的疑似限流账号。
    suspected AS (
        SELECT
            *,
            SAFE_DIVIDE(recent_t1_avg_view_count, baseline_t1_avg_view_count)
                AS throttle_ratio
        FROM fluctuation
        WHERE recent_t1_avg_view_count
                < baseline_t1_avg_view_count * {THRESHOLD_RATIO}
    ),
    -- 按近期与历史播放比从低到高生成全局及渠道内排名。
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY dt
                ORDER BY throttle_ratio, recent_t1_avg_view_count, channel, creator_id
            ) AS rank_no,
            ROW_NUMBER() OVER (
                PARTITION BY dt, channel
                ORDER BY throttle_ratio, recent_t1_avg_view_count, creator_id
            ) AS channel_rank_no
        FROM suspected
    )
    SELECT
        dt,
        baseline_start_dt,
        baseline_end_dt,
        recent_start_dt,
        recent_end_dt,
        rank_no,
        channel_rank_no,
        channel,
        creator_id,
        account_name,
        account_handle,
        account_url,
        baseline_active_day_count,
        baseline_video_count,
        baseline_t1_view_count,
        CAST(baseline_t1_avg_view_count AS NUMERIC) AS baseline_t1_avg_view_count,
        recent_video_count,
        recent_t1_view_count,
        CAST(recent_t1_avg_view_count AS NUMERIC) AS recent_t1_avg_view_count,
        -- 兼容旧口径历史分区保留的逐条视频字段，新口径按发布日聚合，固定为空。
        CAST(NULL AS NUMERIC) AS recent_video_1_t1_view_count,
        CAST(NULL AS NUMERIC) AS recent_video_2_t1_view_count,
        CAST(NULL AS NUMERIC) AS recent_video_3_t1_view_count,
        CAST({THRESHOLD_RATIO} AS NUMERIC) AS threshold_ratio,
        CAST(throttle_ratio AS NUMERIC) AS throttle_ratio,
        CAST(1 - throttle_ratio AS NUMERIC) AS decline_rate,
        CAST(baseline_t1_avg_view_count - recent_t1_avg_view_count AS NUMERIC)
            AS decline_abs,
        IF(throttle_ratio <= 0.1, '高度疑似', '疑似') AS severity,
        CURRENT_TIMESTAMP() AS update_time,
        pool_window_start_dt,
        pool_window_end_dt,
        pool_published_video_count,
        CAST(weekly_avg_published_video_count AS NUMERIC)
            AS weekly_avg_published_video_count
    FROM ranked
    """


def validate(staging_table: str) -> None:
    result = list(
        client.query(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT CONCAT(CAST(dt AS STRING), '|', channel, '|', creator_id))
                    AS unique_account_count,
                COUNT(DISTINCT CONCAT(CAST(dt AS STRING), '|', CAST(rank_no AS STRING)))
                    AS unique_rank_count,
                COUNTIF(recent_t1_avg_view_count
                        >= baseline_t1_avg_view_count * threshold_ratio)
                    AS invalid_suspected_count,
                COUNTIF(ABS(
                    throttle_ratio
                        - SAFE_DIVIDE(recent_t1_avg_view_count,
                            baseline_t1_avg_view_count)
                ) > 0.000001) AS invalid_ratio_count,
                COUNTIF(baseline_start_dt > baseline_end_dt
                    OR recent_start_dt > recent_end_dt
                    OR baseline_end_dt > recent_start_dt)
                    AS invalid_window_count
            FROM `{staging_table}`
            """
        ).result()
    )[0]
    if result.row_count != result.unique_account_count:
        raise RuntimeError("疑似限流账号榜校验失败：统计日、渠道和账号不唯一")
    if result.row_count != result.unique_rank_count:
        raise RuntimeError("疑似限流账号榜校验失败：统计日内排名不唯一")
    if result.invalid_suspected_count != 0:
        raise RuntimeError("疑似限流账号榜校验失败：存在最近发布日日均未低于阈值的账号")
    if result.invalid_ratio_count != 0:
        raise RuntimeError("疑似限流账号榜校验失败：限流指数计算错误")
    if result.invalid_window_count != 0:
        raise RuntimeError("疑似限流账号榜校验失败：日期窗口错误")


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
