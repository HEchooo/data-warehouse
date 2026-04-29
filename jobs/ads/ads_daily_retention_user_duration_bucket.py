from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "ads_daily_retention_user_duration_bucket"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "ads" / TABLE_NAME
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


def dates_to_sql_list(dates: list[date]) -> str:
    return ", ".join([f"'{d}'" for d in dates])


def dates_to_array_sql(dates: list[date]) -> str:
    return ", ".join([f"DATE '{d}'" for d in dates])


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


def get_retention_user_duration_bucket_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    留存用户时长分桶 ADS 只消费 dws_user_activity_daily。

    规则：
    - 目标表不存在或为空时，按 DWS 已落地历史分区全量初始化
    - 目标表已存在时，只读取最近 6 小时内 DWS 变更的 `dt`
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_activity_daily`
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT dt
    FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_activity_daily`
    WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_ads_daily_retention_user_duration_bucket(dates: list[date]) -> None:
    """
    留存用户时长分桶 ADS。

    粒度：
    - dt × user_group × duration_bucket

    口径：
    - 复用 dws_user_activity_daily 的 cohort 与留存标记
    - 时长按用户在 dt 当日聚合后的 duration_sec 分桶
    - 0 秒用户不进入任何时长桶
    """
    if not dates:
        logging.info("没有新的 ads_daily_retention_user_duration_bucket 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        user_group,
        duration_bucket_order,
        duration_bucket,
        next_day_retained_user_count,
        day_7_retained_user_count,
        next_day_not_retained_user_count,
        day_7_not_retained_user_count,
        update_time
    )
    WITH
    -- 当前批次待重算的日期列表。
    target_dates AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 固定产出三类已登录用户分群，保证每个日期都有完整维度行。
    user_group_list AS (
        SELECT '新用户' AS user_group
        UNION ALL
        SELECT '老用户' AS user_group
        UNION ALL
        SELECT '全量登录用户' AS user_group
    ),
    -- 固定产出展示层使用的时长分桶。
    duration_bucket_list AS (
        SELECT
            1 AS duration_bucket_order,
            '(0~1]' AS duration_bucket,
            0 AS min_duration_sec_exclusive,
            60 AS max_duration_sec_inclusive
        UNION ALL
        SELECT
            2 AS duration_bucket_order,
            '(1~3]' AS duration_bucket,
            60 AS min_duration_sec_exclusive,
            180 AS max_duration_sec_inclusive
        UNION ALL
        SELECT
            3 AS duration_bucket_order,
            '(3~5]' AS duration_bucket,
            180 AS min_duration_sec_exclusive,
            300 AS max_duration_sec_inclusive
        UNION ALL
        SELECT
            4 AS duration_bucket_order,
            '(5~10]' AS duration_bucket,
            300 AS min_duration_sec_exclusive,
            600 AS max_duration_sec_inclusive
        UNION ALL
        SELECT
            5 AS duration_bucket_order,
            '(10~15]' AS duration_bucket,
            600 AS min_duration_sec_exclusive,
            900 AS max_duration_sec_inclusive
        UNION ALL
        SELECT
            6 AS duration_bucket_order,
            '15分钟以上' AS duration_bucket,
            900 AS min_duration_sec_exclusive,
            NULL AS max_duration_sec_inclusive
    ),
    -- 目标日期、用户群、时长桶脚手架，并标记观察日是否已开始。
    group_bucket_keys AS (
        SELECT
            d.dt,
            g.user_group,
            b.duration_bucket_order,
            b.duration_bucket,
            DATE_ADD(d.dt, INTERVAL 1 DAY) <= CURRENT_DATE('{TORONTO_TZ}')
                AS is_next_day_observation_started,
            DATE_ADD(d.dt, INTERVAL 7 DAY) <= CURRENT_DATE('{TORONTO_TZ}')
                AS is_day_7_observation_started
        FROM target_dates d
        CROSS JOIN user_group_list g
        CROSS JOIN duration_bucket_list b
    ),
    -- 当前批次用户活动明细，只保留目标日期且排除 0 秒用户。
    user_activity AS (
        SELECT
            dt,
            user_id,
            is_new_user,
            is_next_day_retained,
            is_day_7_retained,
            duration_sec
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_activity_daily`
        WHERE dt IN ({dates_str})
            AND duration_sec > 0
    ),
    -- 把用户活动展开到新用户 / 老用户 / 全量登录用户三种展示分群。
    activity_by_group AS (
        SELECT
            dt,
            user_id,
            '全量登录用户' AS user_group,
            is_next_day_retained,
            is_day_7_retained,
            duration_sec
        FROM user_activity

        UNION ALL

        SELECT
            dt,
            user_id,
            '新用户' AS user_group,
            is_next_day_retained,
            is_day_7_retained,
            duration_sec
        FROM user_activity
        WHERE is_new_user

        UNION ALL

        SELECT
            dt,
            user_id,
            '老用户' AS user_group,
            is_next_day_retained,
            is_day_7_retained,
            duration_sec
        FROM user_activity
        WHERE NOT is_new_user
    ),
    -- 给每个用户归到唯一时长桶，供 cohort 用户数聚合复用。
    bucketed_activity AS (
        SELECT
            a.dt,
            a.user_id,
            a.user_group,
            a.is_next_day_retained,
            a.is_day_7_retained,
            b.duration_bucket_order,
            b.duration_bucket
        FROM activity_by_group a
        INNER JOIN duration_bucket_list b
            ON a.duration_sec > b.min_duration_sec_exclusive
            AND (
                b.max_duration_sec_inclusive IS NULL
                OR a.duration_sec <= b.max_duration_sec_inclusive
            )
    )
    SELECT
        k.dt,
        k.user_group,
        k.duration_bucket_order,
        k.duration_bucket,
        CASE
            WHEN NOT k.is_next_day_observation_started
                THEN NULL
            ELSE COUNTIF(b.is_next_day_retained)
        END AS next_day_retained_user_count,
        CASE
            WHEN NOT k.is_day_7_observation_started
                THEN NULL
            ELSE COUNTIF(b.is_day_7_retained)
        END AS day_7_retained_user_count,
        CASE
            WHEN NOT k.is_next_day_observation_started
                THEN NULL
            ELSE COUNTIF(
                b.user_id IS NOT NULL
                AND NOT COALESCE(b.is_next_day_retained, FALSE)
            )
        END AS next_day_not_retained_user_count,
        CASE
            WHEN NOT k.is_day_7_observation_started
                THEN NULL
            ELSE COUNTIF(
                b.user_id IS NOT NULL
                AND NOT COALESCE(b.is_day_7_retained, FALSE)
            )
        END AS day_7_not_retained_user_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM group_bucket_keys k
    LEFT JOIN bucketed_activity b
        ON b.dt = k.dt
        AND b.user_group = k.user_group
        AND b.duration_bucket_order = k.duration_bucket_order
    GROUP BY
        k.dt,
        k.user_group,
        k.duration_bucket_order,
        k.duration_bucket,
        k.is_next_day_observation_started,
        k.is_day_7_observation_started;
    """
    logging.info("开始处理: ads_daily_retention_user_duration_bucket")
    job = client.query(query)
    job.result()
    logging.info("ads_daily_retention_user_duration_bucket 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_retention_user_duration_bucket_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 ads_daily_retention_user_duration_bucket 日期需要处理")
        raise SystemExit(0)

    run_ads_daily_retention_user_duration_bucket(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("ads_daily_retention_user_duration_bucket 完成, 耗时: %.1f 秒", elapsed)
