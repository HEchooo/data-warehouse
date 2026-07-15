import argparse
from datetime import date, datetime, timedelta, timezone
import logging

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from zoneinfo import ZoneInfo


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "ads_daily_social_creator_suspected_throttle_rankings"
SOURCE_TABLE_NAME = "dws_social_creator_published_t1_daily"
INCREMENTAL_WINDOW_HOURS = 6
BASELINE_DAY_COUNT = 7
RECENT_DAY_COUNT = 3
MIN_BASELINE_ACTIVE_DAY_COUNT = 3
MIN_BASELINE_VIDEO_COUNT = 5
THRESHOLD_RATIO = 0.3

client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="构建 Social Content 疑似限流账号日榜"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="按账号 T+1 日事实的完整成熟日期历史重刷",
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
    SELECT
        DATE_ADD(MIN(published_dt), INTERVAL {BASELINE_DAY_COUNT + RECENT_DAY_COUNT} DAY)
            AS min_stat_dt,
        LEAST(
            DATE_ADD(MAX(published_dt), INTERVAL 1 DAY),
            DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY)
        ) AS max_stat_dt
    FROM `{table_id(SOURCE_TABLE_NAME)}`
    """
    rows = list(client.query(query).result())
    if not rows or rows[0].min_stat_dt is None:
        return []
    start_dt = rows[0].min_stat_dt
    end_dt = rows[0].max_stat_dt
    if start_dt > end_dt:
        return []
    return [
        start_dt + timedelta(days=offset)
        for offset in range((end_dt - start_dt).days + 1)
    ]


def get_dates_to_process() -> list[date]:
    if not target_table_exists() or get_target_row_count() == 0:
        return get_full_dates_to_process()

    query = f"""
    WITH
    -- 账号发布日期事实变化会影响其后的第 1 至第 10 个统计日。
    changed_dates AS (
        SELECT DISTINCT published_dt
        FROM `{table_id(SOURCE_TABLE_NAME)}`
        WHERE update_time >= TIMESTAMP_SUB(
            CURRENT_TIMESTAMP(), INTERVAL {INCREMENTAL_WINDOW_HOURS} HOUR
        )
    ),
    candidate_dates AS (
        SELECT DATE_ADD(published_dt, INTERVAL day_offset DAY) AS stat_dt
        FROM changed_dates
        CROSS JOIN UNNEST(GENERATE_ARRAY(1, {BASELINE_DAY_COUNT + RECENT_DAY_COUNT}))
            AS day_offset
    ),
    target_boundary AS (
        SELECT MAX(dt) AS max_dt FROM `{table_id(TABLE_NAME)}`
    ),
    source_boundary AS (
        SELECT DATE_ADD(MAX(published_dt), INTERVAL 1 DAY) AS max_available_dt
        FROM `{table_id(SOURCE_TABLE_NAME)}`
    ),
    missing_dates AS (
        SELECT dt AS stat_dt
        FROM target_boundary
        CROSS JOIN source_boundary
        CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(
            DATE_ADD(max_dt, INTERVAL 1 DAY),
            LEAST(
                max_available_dt,
                DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY)
            )
        )) AS dt
        WHERE max_dt < LEAST(
            max_available_dt,
            DATE_SUB(CURRENT_DATE('{TORONTO_TZ}'), INTERVAL 1 DAY)
        )
    )
    SELECT DISTINCT stat_dt
    FROM (
        SELECT stat_dt FROM candidate_dates
        UNION ALL
        SELECT stat_dt FROM missing_dates
    )
    WHERE stat_dt < CURRENT_DATE('{TORONTO_TZ}')
    ORDER BY stat_dt
    """
    return [row.stat_dt for row in client.query(query).result()]


def dates_to_array_sql(dates: list[date]) -> str:
    return ", ".join(f"DATE '{stat_dt}'" for stat_dt in dates)


def build_select_sql(dates: list[date]) -> str:
    dates_sql = dates_to_array_sql(dates)
    min_source_dt = min(dates) - timedelta(
        days=BASELINE_DAY_COUNT + RECENT_DAY_COUNT
    )
    max_source_dt = max(dates) - timedelta(days=1)
    return f"""
    WITH
    -- 构造榜单统计日及其 7 天基准窗、3 天近期窗。
    stat_dates AS (
        SELECT
            dt,
            DATE_SUB(dt, INTERVAL 10 DAY) AS baseline_start_dt,
            DATE_SUB(dt, INTERVAL 4 DAY) AS baseline_end_dt,
            DATE_SUB(dt, INTERVAL 3 DAY) AS recent_start_dt,
            DATE_SUB(dt, INTERVAL 1 DAY) AS recent_end_dt
        FROM UNNEST([{dates_sql}]) AS dt
    ),
    -- 只读取所有待处理统计日覆盖到的必要账号发布日期分区。
    creator_t1_daily AS (
        SELECT *
        FROM `{table_id(SOURCE_TABLE_NAME)}`
        WHERE published_dt BETWEEN DATE '{min_source_dt}' AND DATE '{max_source_dt}'
    ),
    -- 将账号日事实映射到对应统计日的基准窗或近期窗。
    windowed AS (
        SELECT
            stat.*,
            daily.published_dt,
            daily.channel,
            daily.creator_id,
            daily.account_name,
            daily.account_handle,
            daily.account_url,
            daily.published_video_count,
            daily.observed_t1_view_count,
            daily.t1_avg_view_count,
            daily.is_t1_snapshot_complete,
            daily.published_dt BETWEEN stat.baseline_start_dt AND stat.baseline_end_dt
                AS is_baseline,
            daily.published_dt BETWEEN stat.recent_start_dt AND stat.recent_end_dt
                AS is_recent
        FROM stat_dates stat
        INNER JOIN creator_t1_daily daily
            ON daily.published_dt BETWEEN stat.baseline_start_dt AND stat.recent_end_dt
    ),
    -- 汇总每个账号的样本量、完整性和三个近期发布日的 T+1 篇均播放。
    account_window AS (
        SELECT
            dt,
            baseline_start_dt,
            baseline_end_dt,
            recent_start_dt,
            recent_end_dt,
            channel,
            creator_id,
            ARRAY_AGG(
                STRUCT(account_name, account_handle, account_url)
                ORDER BY published_dt DESC
                LIMIT 1
            )[OFFSET(0)].account_name AS account_name,
            ARRAY_AGG(
                STRUCT(account_name, account_handle, account_url)
                ORDER BY published_dt DESC
                LIMIT 1
            )[OFFSET(0)].account_handle AS account_handle,
            ARRAY_AGG(
                STRUCT(account_name, account_handle, account_url)
                ORDER BY published_dt DESC
                LIMIT 1
            )[OFFSET(0)].account_url AS account_url,
            COUNTIF(is_baseline) AS baseline_active_day_count,
            COUNTIF(is_baseline AND is_t1_snapshot_complete)
                AS baseline_complete_day_count,
            SUM(IF(is_baseline, published_video_count, 0)) AS baseline_video_count,
            SUM(IF(is_baseline, observed_t1_view_count, 0))
                AS baseline_t1_view_count,
            COUNTIF(is_recent) AS recent_active_day_count,
            COUNTIF(is_recent AND is_t1_snapshot_complete)
                AS recent_complete_day_count,
            SUM(IF(is_recent, published_video_count, 0)) AS recent_video_count,
            SUM(IF(is_recent, observed_t1_view_count, 0)) AS recent_t1_view_count,
            MAX(IF(published_dt = recent_start_dt, t1_avg_view_count, NULL))
                AS recent_day_1_t1_avg_view_count,
            MAX(IF(published_dt = DATE_ADD(recent_start_dt, INTERVAL 1 DAY),
                t1_avg_view_count, NULL)) AS recent_day_2_t1_avg_view_count,
            MAX(IF(published_dt = recent_end_dt, t1_avg_view_count, NULL))
                AS recent_day_3_t1_avg_view_count
        FROM windowed
        GROUP BY
            dt,
            baseline_start_dt,
            baseline_end_dt,
            recent_start_dt,
            recent_end_dt,
            channel,
            creator_id
    ),
    -- 计算历史基准、近期均值和下降比例，并执行样本准入规则。
    eligible AS (
        SELECT
            *,
            SAFE_DIVIDE(baseline_t1_view_count, baseline_video_count)
                AS baseline_t1_avg_view_count,
            SAFE_DIVIDE(recent_t1_view_count, recent_video_count)
                AS recent_t1_avg_view_count
        FROM account_window
        WHERE baseline_active_day_count >= {MIN_BASELINE_ACTIVE_DAY_COUNT}
            AND baseline_complete_day_count = baseline_active_day_count
            AND baseline_video_count >= {MIN_BASELINE_VIDEO_COUNT}
            AND recent_active_day_count = {RECENT_DAY_COUNT}
            AND recent_complete_day_count = {RECENT_DAY_COUNT}
            AND baseline_t1_view_count > 0
    ),
    -- 仅保留连续三个近期发布日都低于历史基准 30% 的疑似限流账号。
    suspected AS (
        SELECT
            *,
            SAFE_DIVIDE(recent_t1_avg_view_count, baseline_t1_avg_view_count)
                AS throttle_ratio
        FROM eligible
        WHERE recent_day_1_t1_avg_view_count
                < baseline_t1_avg_view_count * {THRESHOLD_RATIO}
            AND recent_day_2_t1_avg_view_count
                < baseline_t1_avg_view_count * {THRESHOLD_RATIO}
            AND recent_day_3_t1_avg_view_count
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
        CAST(recent_day_1_t1_avg_view_count AS NUMERIC)
            AS recent_day_1_t1_avg_view_count,
        CAST(recent_day_2_t1_avg_view_count AS NUMERIC)
            AS recent_day_2_t1_avg_view_count,
        CAST(recent_day_3_t1_avg_view_count AS NUMERIC)
            AS recent_day_3_t1_avg_view_count,
        CAST({THRESHOLD_RATIO} AS NUMERIC) AS threshold_ratio,
        CAST(throttle_ratio AS NUMERIC) AS throttle_ratio,
        CAST(1 - throttle_ratio AS NUMERIC) AS decline_rate,
        IF(throttle_ratio <= 0.1, '高度疑似', '疑似') AS severity,
        CURRENT_TIMESTAMP() AS update_time
    FROM ranked
    """


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

        validation = list(
            client.query(
                f"""
                SELECT
                    COUNT(*) AS row_count,
                    COUNT(DISTINCT CONCAT(CAST(dt AS STRING), '|', channel, '|', creator_id))
                        AS unique_account_count,
                    COUNT(DISTINCT CONCAT(CAST(dt AS STRING), '|', CAST(rank_no AS STRING)))
                        AS unique_rank_count,
                    COUNTIF(baseline_active_day_count < {MIN_BASELINE_ACTIVE_DAY_COUNT}
                        OR baseline_video_count < {MIN_BASELINE_VIDEO_COUNT})
                        AS invalid_baseline_sample_count,
                    COUNTIF(recent_day_1_t1_avg_view_count
                            >= baseline_t1_avg_view_count * threshold_ratio
                        OR recent_day_2_t1_avg_view_count
                            >= baseline_t1_avg_view_count * threshold_ratio
                        OR recent_day_3_t1_avg_view_count
                            >= baseline_t1_avg_view_count * threshold_ratio)
                        AS invalid_suspected_count,
                    COUNTIF(ABS(
                        throttle_ratio
                            - SAFE_DIVIDE(recent_t1_avg_view_count,
                                baseline_t1_avg_view_count)
                    ) > 0.000001) AS invalid_ratio_count,
                    COUNTIF(baseline_start_dt != DATE_SUB(dt, INTERVAL 10 DAY)
                        OR baseline_end_dt != DATE_SUB(dt, INTERVAL 4 DAY)
                        OR recent_start_dt != DATE_SUB(dt, INTERVAL 3 DAY)
                        OR recent_end_dt != DATE_SUB(dt, INTERVAL 1 DAY))
                        AS invalid_window_count
                FROM `{staging_table}`
                """
            ).result()
        )[0]
        if validation.row_count != validation.unique_account_count:
            raise RuntimeError("疑似限流账号榜校验失败：统计日、渠道和账号不唯一")
        if validation.row_count != validation.unique_rank_count:
            raise RuntimeError("疑似限流账号榜校验失败：统计日内排名不唯一")
        if validation.invalid_baseline_sample_count != 0:
            raise RuntimeError("疑似限流账号榜校验失败：基准样本不满足准入条件")
        if validation.invalid_suspected_count != 0:
            raise RuntimeError("疑似限流账号榜校验失败：存在未连续低于阈值的账号")
        if validation.invalid_ratio_count != 0:
            raise RuntimeError("疑似限流账号榜校验失败：限流指数计算错误")
        if validation.invalid_window_count != 0:
            raise RuntimeError("疑似限流账号榜校验失败：日期窗口错误")

        if target_table_exists():
            preserve_sql = (
                "WHERE FALSE"
                if full_refresh
                else f"WHERE dt NOT IN UNNEST([{dates_sql}])"
            )
            client.query(
                f"""
                CREATE TABLE `{changed_table}`
                PARTITION BY dt
                CLUSTER BY channel, creator_id AS
                SELECT * FROM `{target_table}`
                {preserve_sql}

                UNION ALL

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
