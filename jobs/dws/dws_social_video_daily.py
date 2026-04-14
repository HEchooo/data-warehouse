import argparse
import logging
from datetime import date, datetime, timedelta, timezone
from google.cloud import bigquery
from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
VIDEO_SOURCE_DATASET_ID = "videos"
TORONTO_TZ = "America/Toronto"
SOCIAL_VIDEO_REBUILD_DAYS = 30
YOUTUBE_AI_TEAM_TEST_CHANNEL_IDS = [
    "UCynAg7WHdiy8SuQpESO__yw",
    "UCgBJozWjiE0cqwv7UNalyog",
]
YOUTUBE_AI_TEAM_INTERNAL_CHANNEL_IDS = [
    "UC4rj_IlO3V-d5ebVhgagh1w",
    "UC70Y2-yeuFOoTx6yNkqioKA",
    "UCiX5DzRDNIb9fH_MQvoIN7w",
    "UCLs3oc5eyptqVTEnXaIwHvQ",
    "UCPAQ09Nusb_OZzwfWGAE_Aw",
    "UCV5FBW_l8cayS8L_zAIvlkQ",
    "UCVsLt_8hbY5nG5da8sslD_w",
    "UCzIQMkLCHa63JekjxaA133w",
]
YOUTUBE_AI_TEAM_EXCLUDED_CHANNEL_IDS = (
    YOUTUBE_AI_TEAM_TEST_CHANNEL_IDS + YOUTUBE_AI_TEAM_INTERNAL_CHANNEL_IDS
)
client = bigquery.Client(project=PROJECT_ID)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="构建 Social Content 社媒视频日表（dt × channel × creator_id × video_key）"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="按全历史发布时间范围重刷全量分区，而不是默认最近 30 天",
    )
    return parser.parse_args()


def dates_to_sql_list(dates):
    return ", ".join([f"'{d}'" for d in dates])


def strings_to_sql_list(values):
    return ", ".join([f"'{value}'" for value in values])


def get_social_video_dates_to_process(rebuild_days=SOCIAL_VIDEO_REBUILD_DAYS):
    """
    Social Content 视频日表只依赖视频源表，独立重刷最近 N 天分区。
    这样不会受事件主链路调度与失败状态影响。
    """
    end_date = datetime.now(ZoneInfo(TORONTO_TZ)).date()
    start_date = end_date - timedelta(days=rebuild_days - 1)
    return [start_date + timedelta(days=offset) for offset in range(rebuild_days)]


def get_social_video_full_dates_to_process():
    """
    全量回刷时，从三端视频最早发布时间开始生成完整日期序列。
    """
    query = f"""
    WITH published_dates AS (
        SELECT MIN(DATE(TIMESTAMP(published_at, 'UTC'), '{TORONTO_TZ}')) AS min_dt
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.yt_videos`
        WHERE channel_id NOT IN ({strings_to_sql_list(YOUTUBE_AI_TEAM_EXCLUDED_CHANNEL_IDS)})

        UNION ALL

        SELECT MIN(DATE(TIMESTAMP_SECONDS(SAFE_CAST(create_time AS INT64)), '{TORONTO_TZ}')) AS min_dt
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.videos`

        UNION ALL

        SELECT MIN(DATE(TIMESTAMP(published_at, 'UTC'), '{TORONTO_TZ}')) AS min_dt
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.ig_media`
    )
    SELECT
        MIN(min_dt) AS start_dt,
        CURRENT_DATE('{TORONTO_TZ}') AS end_dt
    FROM published_dates
    WHERE min_dt IS NOT NULL
    """
    rows = list(client.query(query).result())
    if not rows or rows[0].start_dt is None or rows[0].end_dt is None:
        return []

    start_date = rows[0].start_dt
    end_date = rows[0].end_dt
    total_days = (end_date - start_date).days + 1
    return [start_date + timedelta(days=offset) for offset in range(total_days)]


def build_social_video_daily_select_sql(dates: list[date]) -> str:
    dates_str = dates_to_sql_list(dates)
    min_date = min(dates)
    max_date = max(dates)
    youtube_ai_team_excluded_channel_ids_sql = strings_to_sql_list(
        YOUTUBE_AI_TEAM_EXCLUDED_CHANNEL_IDS
    )

    return f"""
    WITH
    -- 计算窗口参数；额外带出 seed_dt 作为窗口起点前一天，用来算首日增量
    params AS (
        SELECT
            DATE('{min_date}') AS min_dt,
            DATE('{max_date}') AS max_dt,
            DATE_SUB(DATE('{min_date}'), INTERVAL 1 DAY) AS seed_dt
    ),
    -- 三端视频维表统一成同一套视频/博主主键和发布时间
    video_dim AS (
        SELECT
            channel,
            creator_id,
            video_id,
            video_key,
            published_ts_utc,
            DATE(published_ts_utc, '{TORONTO_TZ}') AS published_dt
        FROM (
            SELECT
                'youtube' AS channel,
                CAST(channel_id AS STRING) AS creator_id,
                CAST(video_id AS STRING) AS video_id,
                CONCAT('youtube:', CAST(video_id AS STRING)) AS video_key,
                TIMESTAMP(published_at, 'UTC') AS published_ts_utc
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.yt_videos`
            WHERE channel_id NOT IN ({youtube_ai_team_excluded_channel_ids_sql})

            UNION ALL

            SELECT
                'tiktok' AS channel,
                CAST(user_id AS STRING) AS creator_id,
                CAST(id AS STRING) AS video_id,
                CONCAT('tiktok:', CAST(id AS STRING)) AS video_key,
                TIMESTAMP_SECONDS(SAFE_CAST(create_time AS INT64)) AS published_ts_utc
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.videos`

            UNION ALL

            SELECT
                'ins' AS channel,
                CAST(account_id AS STRING) AS creator_id,
                CAST(media_id AS STRING) AS video_id,
                CONCAT('ins:', CAST(media_id AS STRING)) AS video_key,
                TIMESTAMP(published_at, 'UTC') AS published_ts_utc
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.ig_media`
        )
        WHERE published_ts_utc IS NOT NULL
            AND DATE(published_ts_utc, '{TORONTO_TZ}') <= (SELECT max_dt FROM params)
    ),
    -- 三端累计快照日志统一成原始快照流
    raw_snapshot_log AS (
        SELECT
            video_key,
            DATE(collected_ts_utc, '{TORONTO_TZ}') AS dt,
            collected_ts_utc,
            cum_views,
            cum_likes
        FROM (
            SELECT
                CONCAT('youtube:', CAST(video_id AS STRING)) AS video_key,
                TIMESTAMP(collected_at, 'UTC') AS collected_ts_utc,
                SAFE_CAST(views AS INT64) AS cum_views,
                SAFE_CAST(likes AS INT64) AS cum_likes
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.yt_video_analytics_log`
            WHERE channel_id NOT IN ({youtube_ai_team_excluded_channel_ids_sql})

            UNION ALL

            SELECT
                CONCAT('tiktok:', CAST(video_id AS STRING)) AS video_key,
                TIMESTAMP(collected_at, 'UTC') AS collected_ts_utc,
                SAFE_CAST(view_count AS INT64) AS cum_views,
                SAFE_CAST(like_count AS INT64) AS cum_likes
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.video_stats_log`

            UNION ALL

            SELECT
                CONCAT('ins:', CAST(media_id AS STRING)) AS video_key,
                TIMESTAMP(collected_at, 'UTC') AS collected_ts_utc,
                SAFE_CAST(view_count AS INT64) AS cum_views,
                SAFE_CAST(like_count AS INT64) AS cum_likes
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.ig_media_stats_log`
        )
        WHERE collected_ts_utc IS NOT NULL
            AND (cum_views IS NOT NULL OR cum_likes IS NOT NULL)
            AND DATE(collected_ts_utc, '{TORONTO_TZ}') <= (SELECT max_dt FROM params)
    ),
    -- 每个视频每天只保留最后一次采集到的累计值，作为真实采集日的日末快照
    daily_latest_snapshot AS (
        SELECT
            video_key,
            dt,
            cum_views AS raw_day_end_views,
            cum_likes AS raw_day_end_likes
        FROM (
            SELECT
                video_key,
                dt,
                cum_views,
                cum_likes,
                ROW_NUMBER() OVER (
                    PARTITION BY video_key, dt
                    ORDER BY collected_ts_utc DESC
                ) AS rn
            FROM raw_snapshot_log
        )
        WHERE rn = 1
    ),
    -- 识别每个视频首次被采到正播放量的日期，作为新增视频的全历史属性
    first_positive_snapshot AS (
        SELECT
            video_key,
            MIN(dt) AS first_positive_view_dt
        FROM daily_latest_snapshot
        WHERE raw_day_end_views > 0
        GROUP BY video_key
    ),
    -- 取窗口开始前最近一次快照，作为 seed_dt 这一天的承接累计值
    carry_in_snapshot AS (
        SELECT
            video_key,
            DATE((SELECT seed_dt FROM params)) AS anchor_dt,
            raw_day_end_views,
            raw_day_end_likes,
            TRUE AS is_carry_in
        FROM (
            SELECT
                video_key,
                raw_day_end_views,
                raw_day_end_likes,
                ROW_NUMBER() OVER (
                    PARTITION BY video_key
                    ORDER BY dt DESC
                ) AS rn
            FROM daily_latest_snapshot
            WHERE dt < (SELECT min_dt FROM params)
        )
        WHERE rn = 1
    ),
    -- 本窗口内所有真实采集日快照，加上窗口起点前一天的承接快照
    snapshot_anchor AS (
        SELECT
            video_key,
            dt AS anchor_dt,
            raw_day_end_views,
            raw_day_end_likes,
            FALSE AS is_carry_in
        FROM daily_latest_snapshot
        WHERE dt BETWEEN (SELECT min_dt FROM params) AND (SELECT max_dt FROM params)

        UNION ALL

        SELECT
            video_key,
            anchor_dt,
            raw_day_end_views,
            raw_day_end_likes,
            is_carry_in
        FROM carry_in_snapshot
    ),
    -- 每个视频在窗口内第一次“已知累计值”生效的日期
    first_anchor_by_video AS (
        SELECT
            video_key,
            MIN(anchor_dt) AS first_anchor_dt
        FROM snapshot_anchor
        GROUP BY video_key
    ),
    -- 在首个已知累计值出现前，累计值都按 0 处理
    zero_value_range AS (
        SELECT
            v.channel,
            v.creator_id,
            v.video_id,
            v.video_key,
            v.published_ts_utc,
            v.published_dt,
            GREATEST(v.published_dt, (SELECT seed_dt FROM params)) AS start_dt,
            LEAST(
                COALESCE(
                    DATE_SUB(f.first_anchor_dt, INTERVAL 1 DAY),
                    (SELECT max_dt FROM params)
                ),
                (SELECT max_dt FROM params)
            ) AS end_dt,
            0 AS day_end_views,
            0 AS day_end_likes,
            FALSE AS has_snapshot_to_dt,
            CAST(NULL AS DATE) AS actual_snapshot_dt
        FROM video_dim v
        LEFT JOIN first_anchor_by_video f
            ON v.video_key = f.video_key
        WHERE GREATEST(v.published_dt, (SELECT seed_dt FROM params)) <= LEAST(
            COALESCE(
                DATE_SUB(f.first_anchor_dt, INTERVAL 1 DAY),
                (SELECT max_dt FROM params)
            ),
            (SELECT max_dt FROM params)
        )
    ),
    -- 每个已知累计值都会形成一个“生效区间”，直到下一次采集前一天
    anchored_value_range AS (
        SELECT
            v.channel,
            v.creator_id,
            v.video_id,
            v.video_key,
            v.published_ts_utc,
            v.published_dt,
            GREATEST(
                a.anchor_dt,
                v.published_dt,
                (SELECT seed_dt FROM params)
            ) AS start_dt,
            LEAST(
                COALESCE(
                    DATE_SUB(
                        LEAD(a.anchor_dt) OVER (
                            PARTITION BY a.video_key
                            ORDER BY a.anchor_dt
                        ),
                        INTERVAL 1 DAY
                    ),
                    (SELECT max_dt FROM params)
                ),
                (SELECT max_dt FROM params)
            ) AS end_dt,
            a.raw_day_end_views AS day_end_views,
            a.raw_day_end_likes AS day_end_likes,
            TRUE AS has_snapshot_to_dt,
            CASE
                WHEN a.is_carry_in THEN NULL
                ELSE a.anchor_dt
            END AS actual_snapshot_dt
        FROM snapshot_anchor a
        JOIN video_dim v
            ON a.video_key = v.video_key
    ),
    -- 把“值区间”统一起来：先是 0 区间，再是每段真实累计值区间
    video_value_range AS (
        SELECT
            channel,
            creator_id,
            video_id,
            video_key,
            published_ts_utc,
            published_dt,
            start_dt,
            end_dt,
            day_end_views,
            day_end_likes,
            has_snapshot_to_dt,
            actual_snapshot_dt
        FROM zero_value_range

        UNION ALL

        SELECT
            channel,
            creator_id,
            video_id,
            video_key,
            published_ts_utc,
            published_dt,
            start_dt,
            end_dt,
            day_end_views,
            day_end_likes,
            has_snapshot_to_dt,
            actual_snapshot_dt
        FROM anchored_value_range
        WHERE start_dt <= end_dt
    ),
    -- 把每段“值区间”一次性展开成逐日明细；这样每个视频从发布到今天每天都会有一行
    expanded_video_daily AS (
        SELECT
            dt,
            channel,
            creator_id,
            video_id,
            video_key,
            published_ts_utc,
            published_dt,
            day_end_views,
            day_end_likes,
            CASE
                WHEN actual_snapshot_dt = dt THEN day_end_views
                ELSE NULL
            END AS raw_day_end_views,
            CASE
                WHEN actual_snapshot_dt = dt THEN day_end_likes
                ELSE NULL
            END AS raw_day_end_likes,
            CASE
                WHEN actual_snapshot_dt = dt THEN TRUE
                ELSE FALSE
            END AS has_snapshot,
            has_snapshot_to_dt
        FROM video_value_range,
        UNNEST(GENERATE_DATE_ARRAY(start_dt, end_dt)) AS dt
    ),
    -- 基于每天的累计值做差分，得到日增量；seed_dt 只用来保证窗口首日增量正确
    final_daily AS (
        SELECT
            dt,
            channel,
            creator_id,
            video_id,
            video_key,
            published_ts_utc,
            published_dt,
            day_end_views,
            day_end_likes,
            day_end_views - LAG(day_end_views, 1, 0) OVER (
                PARTITION BY video_key
                ORDER BY dt
            ) AS daily_view_increment,
            day_end_likes - LAG(day_end_likes, 1, 0) OVER (
                PARTITION BY video_key
                ORDER BY dt
            ) AS daily_like_increment,
            has_snapshot,
            has_snapshot_to_dt,
            raw_day_end_views,
            raw_day_end_likes
        FROM expanded_video_daily
    )
    SELECT
        f.dt,
        f.channel,
        f.creator_id,
        f.video_id,
        f.video_key,
        f.published_ts_utc,
        f.published_dt,
        f.day_end_views,
        f.day_end_likes,
        f.daily_view_increment,
        f.daily_like_increment,
        f.has_snapshot,
        f.has_snapshot_to_dt,
        (fps.first_positive_view_dt = f.dt) AS is_new_video,
        CASE
            WHEN fps.first_positive_view_dt = f.dt
            THEN COALESCE(f.raw_day_end_views, 0)
            ELSE 0
        END AS new_video_contributed_view_count,
        f.raw_day_end_views,
        f.raw_day_end_likes,
        CURRENT_TIMESTAMP() AS update_time
    FROM final_daily f
    LEFT JOIN first_positive_snapshot fps
        ON f.video_key = fps.video_key
    WHERE f.dt IN ({dates_str})
    """


def run_dws_social_video_daily(dates: list[date]):
    """
    Social Content 视频日表（dt × channel × creator_id × video_key）:
    - 先把真实采集日整理成日末累计快照
    - 再把每个累计值转成“生效区间”，最后一次性展开成逐日明细
    - 如果当天没有采集记录，就沿用最近一次已知累计值
    - `day_end_views / day_end_likes` 是下游主口径；`raw_day_end_*` 仅保留作审计
    - 产出每天的累计 views / likes、对应日增量，以及新增视频标记
    """
    if not dates:
        logging.info("dws_social_video_daily 无待处理日期，跳过")
        return

    min_date = min(dates)
    max_date = max(dates)
    target_table = f"{PROJECT_ID}.{DATASET_ID}.dws_social_video_daily"
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    staging_table = f"{target_table}_tmp_{run_id}"
    changed_table = f"{target_table}_changed_{run_id}"
    target_dropped = False
    select_sql = build_social_video_daily_select_sql(dates)
    insert_into_staging_sql = f"""
    INSERT INTO `{staging_table}`
    (
        dt,
        channel,
        creator_id,
        video_id,
        video_key,
        published_ts_utc,
        published_dt,
        day_end_views,
        day_end_likes,
        daily_view_increment,
        daily_like_increment,
        has_snapshot,
        has_snapshot_to_dt,
        is_new_video,
        new_video_contributed_view_count,
        raw_day_end_views,
        raw_day_end_likes,
        update_time
    )
    {select_sql}
    """
    validate_staging_sql = f"""
    SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT dt) AS dt_count,
        COUNT(DISTINCT CONCAT(CAST(dt AS STRING), '|', video_key)) AS unique_dt_video_count
    FROM `{staging_table}`
    """
    create_changed_table_sql = f"""
    CREATE TABLE `{changed_table}`
    PARTITION BY dt
    CLUSTER BY channel, creator_id, video_key AS
    SELECT *
    FROM `{target_table}`
    WHERE dt < DATE('{min_date}') OR dt > DATE('{max_date}')

    UNION ALL

    SELECT *
    FROM `{staging_table}`
    """

    logging.info(
        "开始处理 dws_social_video_daily, 目标表: %s, 临时表: %s, 替换表: %s",
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
        client.query(insert_into_staging_sql).result()

        validation_row = list(client.query(validate_staging_sql).result())[0]
        if validation_row.row_count == 0:
            raise ValueError("dws_social_video_daily staging 行数校验失败: actual=0")
        if validation_row.dt_count != len(dates):
            raise ValueError(
                "dws_social_video_daily staging 日期数校验失败: "
                f"expected={len(dates)}, actual={validation_row.dt_count}"
            )
        if validation_row.row_count != validation_row.unique_dt_video_count:
            raise ValueError(
                "dws_social_video_daily staging 主键校验失败: "
                f"row_count={validation_row.row_count}, "
                f"unique_dt_video_count={validation_row.unique_dt_video_count}"
            )

        client.query(create_changed_table_sql).result()
        client.query(f"DROP TABLE `{target_table}`").result()
        target_dropped = True
        client.query(
            f"ALTER TABLE `{changed_table}` RENAME TO dws_social_video_daily"
        ).result()
        client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
    except Exception:
        if not target_dropped:
            client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
            client.query(f"DROP TABLE IF EXISTS `{changed_table}`").result()
        raise

    logging.info(
        "dws_social_video_daily 刷新完成, 处理日期: %s ~ %s, 共 %s 天",
        min_date,
        max_date,
        len(dates),
    )


if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)

    args = parse_args()
    if args.full_refresh:
        dates = get_social_video_full_dates_to_process()
    else:
        dates = get_social_video_dates_to_process()
    if not dates:
        logging.info("没有 Social Content 视频数据需要处理")
        raise SystemExit(0)

    logging.info(
        "Social Content 视频日表待处理日期范围: %s ~ %s，共 %s 天",
        min(dates),
        max(dates),
        len(dates),
    )
    run_dws_social_video_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dws_social_video_daily 执行完成, 耗时: %.1f 秒", elapsed)
