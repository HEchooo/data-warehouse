from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
VIDEO_SOURCE_DATASET_ID = "videos"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_video_code_conversion_daily"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "dws" / TABLE_NAME
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


def event_ts_expr() -> str:
    """
    将 logAt_timestamp 先按 prop_timezone 解释为真实时间，再转成 UTC TIMESTAMP。
    logAt_timestamp 存的是“本地时间字面值”，不能直接当 UTC 使用。
    """
    return (
        "TIMESTAMP("
        "DATETIME(logAt_timestamp, 'UTC'), "
        "COALESCE(NULLIF(prop_timezone, ''), 'UTC')"
        ")"
    )


def video_task_change_ts_expr() -> str:
    """
    自动发布任务变更时间。
    Datastream 同步表优先使用 source_timestamp，缺失时回退 updated_at / created_at。
    """
    return (
        "COALESCE("
        "TIMESTAMP_MILLIS(datastream_metadata.source_timestamp), "
        "TIMESTAMP(updated_at, 'UTC'), "
        "TIMESTAMP(created_at, 'UTC')"
        ")"
    )


def get_video_code_conversion_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    视频编码转化 DWS 同时依赖视频播放、搜索事件、首活维表和视频任务映射。

    规则：
    - 目标表不存在或为空时，按 dws_video_daily 全量初始化历史日期
    - 目标表已存在时，按最近 6 小时受影响日期增量重刷
    """
    event_ts = event_ts_expr()
    task_change_ts = video_task_change_ts_expr()
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_video_daily`
        WHERE dt IS NOT NULL
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    WITH
    -- 最近 6 小时变更的视频播放事实日期。
    changed_video_dates AS (
        SELECT DISTINCT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_video_daily`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND dt IS NOT NULL
    ),
    -- 最近 6 小时变更的搜索事件日期（仅保留 8 位编码搜索）。
    changed_search_dates AS (
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND event_name IN ('c_home_search', 'c_shop_search', 'c_tag')
            AND NULLIF(prop_user_id, '') IS NOT NULL
            AND REGEXP_CONTAINS(JSON_VALUE(ext, '$.search_content'), r'^\\d{{8}}$')
            AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
    ),
    -- 最近 6 小时变更的用户首活日期。
    changed_first_active_dates AS (
        SELECT DISTINCT first_active_date AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_user_first_active`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND first_active_date IS NOT NULL
    ),
    -- 最近 6 小时发生变更的自动发布 task_id。
    changed_task_ids AS (
        SELECT DISTINCT task_id
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.video_upload_tasks`
        WHERE {task_change_ts} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
            AND task_id IS NOT NULL
            AND task_id != ''
            AND REGEXP_CONTAINS(JSON_VALUE(ext_info, '$.promotion_code'), r'^\\d{{8}}$')
    ),
    -- 每个 task_id 最新一条 promotion_code 映射。
    latest_task_promotion_map AS (
        SELECT
            task_id,
            promotion_code
        FROM (
            SELECT
                task_id,
                JSON_VALUE(ext_info, '$.promotion_code') AS promotion_code,
                {task_change_ts} AS change_ts,
                ROW_NUMBER() OVER (
                    PARTITION BY task_id
                    ORDER BY {task_change_ts} DESC, id DESC
                ) AS rn
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.video_upload_tasks`
            WHERE task_id IS NOT NULL
                AND task_id != ''
                AND REGEXP_CONTAINS(JSON_VALUE(ext_info, '$.promotion_code'), r'^\\d{{8}}$')
        )
        WHERE rn = 1
    ),
    -- 最近变更 task_id 对应的 promotion_code 列表。
    changed_task_codes AS (
        SELECT DISTINCT m.promotion_code
        FROM changed_task_ids t
        INNER JOIN latest_task_promotion_map m
            ON t.task_id = m.task_id
        WHERE m.promotion_code IS NOT NULL
            AND m.promotion_code != ''
    ),
    -- 最近变更 task_id 对应的视频日期。
    changed_task_video_dates AS (
        SELECT DISTINCT v.dt
        FROM changed_task_ids t
        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.dim_auto_publish_video_task` m
            ON t.task_id = m.auto_publish_task_id
        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.dws_video_daily` v
            ON m.video_key = v.video_key
        WHERE v.dt IS NOT NULL
    ),
    -- 最近变更 promotion_code 对应的搜索日期。
    changed_task_search_dates AS (
        SELECT DISTINCT DATE({event_ts}, '{TORONTO_TZ}') AS dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE event_name IN ('c_home_search', 'c_shop_search', 'c_tag')
            AND NULLIF(prop_user_id, '') IS NOT NULL
            AND REGEXP_CONTAINS(JSON_VALUE(ext, '$.search_content'), r'^\\d{{8}}$')
            AND JSON_VALUE(ext, '$.search_content') IN (
                SELECT promotion_code
                FROM changed_task_codes
            )
            AND DATE({event_ts}, '{TORONTO_TZ}') IS NOT NULL
    )
    SELECT DISTINCT dt
    FROM (
        SELECT dt FROM changed_video_dates
        UNION DISTINCT
        SELECT dt FROM changed_search_dates
        UNION DISTINCT
        SELECT dt FROM changed_first_active_dates
        UNION DISTINCT
        SELECT dt FROM changed_task_video_dates
        UNION DISTINCT
        SELECT dt FROM changed_task_search_dates
    )
    WHERE dt IS NOT NULL
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_dws_video_code_conversion_daily(dates: list[date]) -> None:
    """
    产出视频编码转化主题日事实。

    粒度：
    - dt × task_id

    指标：
    - play_view_increment：task_id 对应视频当日播放增量
    - search_user_count：当日搜索该 8 位编码的去重用户数
    - first_active_search_user_count：search_user_count 中当日首活用户数
    """
    if not dates:
        logging.info("没有新的 dws_video_code_conversion_daily 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    event_ts = event_ts_expr()
    task_change_ts = video_task_change_ts_expr()

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        task_id,
        promotion_code,
        play_view_increment,
        search_user_count,
        first_active_search_user_count,
        update_time
    )
    WITH
    -- 本批待处理日期列表。
    date_list AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 每个 task_id 最新一条 promotion_code 映射。
    task_promotion_map AS (
        SELECT
            task_id,
            promotion_code
        FROM (
            SELECT
                task_id,
                JSON_VALUE(ext_info, '$.promotion_code') AS promotion_code,
                ROW_NUMBER() OVER (
                    PARTITION BY task_id
                    ORDER BY {task_change_ts} DESC, id DESC
                ) AS rn
            FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.video_upload_tasks`
            WHERE task_id IS NOT NULL
                AND task_id != ''
                AND REGEXP_CONTAINS(JSON_VALUE(ext_info, '$.promotion_code'), r'^\\d{{8}}$')
        )
        WHERE rn = 1
    ),
    -- 自动发布 task_id 映射到具体视频键。
    task_video_map AS (
        SELECT DISTINCT
            m.auto_publish_task_id AS task_id,
            p.promotion_code,
            m.video_key
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_auto_publish_video_task` m
        INNER JOIN task_promotion_map p
            ON m.auto_publish_task_id = p.task_id
        WHERE m.auto_publish_task_id IS NOT NULL
            AND m.auto_publish_task_id != ''
    ),
    -- 任务维度播放增量分母。
    task_play_metric_daily AS (
        SELECT
            v.dt,
            t.task_id,
            t.promotion_code,
            COALESCE(SUM(v.daily_view_increment), 0) AS play_view_increment
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_video_daily` v
        INNER JOIN task_video_map t
            ON v.video_key = t.video_key
        INNER JOIN date_list d
            ON v.dt = d.dt
        GROUP BY v.dt, t.task_id, t.promotion_code
    ),
    -- 搜索事件基础明细（仅保留 8 位编码和已登录用户）。
    search_event_daily AS (
        SELECT DISTINCT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            NULLIF(prop_user_id, '') AS user_id,
            JSON_VALUE(ext, '$.search_content') AS promotion_code
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE event_name IN ('c_home_search', 'c_shop_search', 'c_tag')
            AND NULLIF(prop_user_id, '') IS NOT NULL
            AND REGEXP_CONTAINS(JSON_VALUE(ext, '$.search_content'), r'^\\d{{8}}$')
            AND DATE({event_ts}, '{TORONTO_TZ}') IN (
                SELECT dt
                FROM date_list
            )
    ),
    -- 任务维度搜索用户集合。
    task_search_user_daily AS (
        SELECT DISTINCT
            s.dt,
            p.task_id,
            s.promotion_code,
            s.user_id
        FROM search_event_daily s
        INNER JOIN task_promotion_map p
            ON s.promotion_code = p.promotion_code
    ),
    -- 任务维度搜索分子聚合（含首活下载分子）。
    task_search_metric_daily AS (
        SELECT
            s.dt,
            s.task_id,
            s.promotion_code,
            COUNT(DISTINCT s.user_id) AS search_user_count,
            COUNT(
                DISTINCT IF(f.first_active_date = s.dt, s.user_id, NULL)
            ) AS first_active_search_user_count
        FROM task_search_user_daily s
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_user_first_active` f
            ON s.user_id = f.prop_user_id
        GROUP BY s.dt, s.task_id, s.promotion_code
    ),
    -- 本批 task_id 粒度键集合（播放或搜索任一侧出现即保留）。
    task_daily_keys AS (
        SELECT dt, task_id, promotion_code
        FROM task_play_metric_daily
        UNION DISTINCT
        SELECT dt, task_id, promotion_code
        FROM task_search_metric_daily
    )
    SELECT
        k.dt,
        k.task_id,
        k.promotion_code,
        COALESCE(p.play_view_increment, 0) AS play_view_increment,
        COALESCE(s.search_user_count, 0) AS search_user_count,
        COALESCE(s.first_active_search_user_count, 0)
            AS first_active_search_user_count,
        CURRENT_TIMESTAMP() AS update_time
    FROM task_daily_keys k
    LEFT JOIN task_play_metric_daily p
        ON k.dt = p.dt
        AND k.task_id = p.task_id
    LEFT JOIN task_search_metric_daily s
        ON k.dt = s.dt
        AND k.task_id = s.task_id;
    """
    logging.info("开始处理: dws_video_code_conversion_daily")
    client.query(query).result()
    logging.info("dws_video_code_conversion_daily 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_video_code_conversion_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 dws_video_code_conversion_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_video_code_conversion_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dws_video_code_conversion_daily 完成, 耗时: %.1f 秒", elapsed)
