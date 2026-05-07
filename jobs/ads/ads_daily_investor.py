from datetime import date, datetime, timedelta, timezone
import logging
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
VIDEO_SOURCE_DATASET_ID = "videos"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "ads_daily_investor"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "ads" / TABLE_NAME
INCREMENTAL_WINDOW_HOURS = 6
YOUTUBE_BOB_TEAM_CHANNEL_IDS = [
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
]

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


def strings_to_sql_list(values: list[str]) -> str:
    return ", ".join([f"'{value}'" for value in values])


def target_table_id() -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


def target_table_exists() -> bool:
    try:
        client.get_table(target_table_id())
        return True
    except NotFound:
        return False


def get_latest_mature_dt() -> date:
    """
    投资人日报默认只处理多伦多昨日及以前的成熟日期。

    原因：
    - 视频与 AppsFlyer 源数据存在明显晚到
    - 如果继续处理多伦多当天，ADS 容易读到跨上游成熟度不一致的半成品
    """
    return datetime.now(ZoneInfo(TORONTO_TZ)).date() - timedelta(days=1)


def get_target_row_count() -> int:
    if not target_table_exists():
        return 0

    query = f"""
    SELECT COUNT(*) AS row_count
    FROM `{target_table_id()}`
    """
    rows = list(client.query(query).result())
    return int(rows[0].row_count) if rows else 0


def get_ads_daily_investor_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    投资人报表当前只消费中间层事实。

    规则：
    - 目标表不存在或为空时，按上游事实已落地日期全量初始化
    - 目标表已存在时，只读取最近 6 小时内上游事实变更的 dt
    - 为避免跨上游成熟度不一致，默认只处理多伦多昨日及以前的日期
    """
    is_full_snapshot = (not target_table_exists()) or get_target_row_count() == 0
    latest_mature_dt = get_latest_mature_dt()

    if is_full_snapshot:
        query = f"""
        SELECT DISTINCT dt
        FROM (
            SELECT dt FROM `{PROJECT_ID}.{DATASET_ID}.dws_video_daily`
            UNION DISTINCT
            SELECT dt FROM `{PROJECT_ID}.{DATASET_ID}.dws_appsflyer_download_daily`
            UNION DISTINCT
            SELECT dt FROM `{PROJECT_ID}.{DATASET_ID}.dws_registered_app_device_daily`
            UNION DISTINCT
            SELECT dt FROM `{PROJECT_ID}.{DATASET_ID}.dws_registered_app_user_daily`
        )
        WHERE dt <= DATE '{latest_mature_dt}'
        ORDER BY dt
        """
        results = client.query(query).result()
        return [row.dt for row in results]

    query = f"""
    SELECT DISTINCT dt
    FROM (
        SELECT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_video_daily`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)

        UNION DISTINCT

        SELECT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_appsflyer_download_daily`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)

        UNION DISTINCT

        SELECT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_registered_app_device_daily`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)

        UNION DISTINCT

        SELECT dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_registered_app_user_daily`
        WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
    )
    WHERE dt <= DATE '{latest_mature_dt}'
    ORDER BY dt
    """
    results = client.query(query).result()
    return [row.dt for row in results]


def run_ads_daily_investor(dates: list[date]) -> None:
    """
    投资人主题日报（dt 一行）。

    当前版本只消费：
    - dim_auto_publish_video_task
    - dws_video_daily
    - dws_appsflyer_download_daily
    - dws_registered_app_device_daily
    - dws_registered_app_user_daily

    ADS 只保留：
    - 投资人口径视频聚合
    - 下载转化派生
    - 设备 / 注册用户展示层派生
    """
    latest_mature_dt = get_latest_mature_dt()
    dates = sorted({dt for dt in dates if dt <= latest_mature_dt})

    if not dates:
        logging.info("没有新的 ads_daily_investor 日期需要处理")
        return

    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    max_date = max(dates)
    youtube_bob_team_channel_ids_sql = strings_to_sql_list(
        YOUTUBE_BOB_TEAM_CHANNEL_IDS
    )

    query = f"""
    DELETE FROM `{target_table_id()}`
    WHERE dt IN ({dates_str});

    INSERT INTO `{target_table_id()}`
    (
        dt,
        new_video_count,
        active_video_count,
        new_video_view_count,
        new_video_contributed_view_count,
        avg_new_video_view_count,
        new_video_7d_avg_view_count,
        video_production_cost_usd,
        cost_per_thousand_views_usd,
        new_download_count,
        play_download_conversion_rate,
        avg_video_download_conversion_count,
        new_device_count,
        active_device_count,
        active_registered_user_count,
        avg_duration_sec,
        avg_content_exposure_count,
        avg_content_click_count,
        content_ctr,
        product_detail_exposure_uv,
        product_detail_exposure_pv,
        avg_product_detail_exposure_count,
        next_day_retention_rate,
        day_7_retention_rate,
        update_time
    )
    WITH
    -- 本批待处理日期列表。
    date_list AS (
        SELECT dt
        FROM UNNEST([{dates_array_sql}]) AS dt
    ),
    -- 生成 jhonson 团队视频键。
    -- 当前视频规则只对 YouTube 定义了 bob / jhonson 边界，
    -- 因此这里对 YouTube 排除 bob 团队渠道；TikTok / Ins 维持现有全量口径。
    johnson_video_keys AS (
        SELECT CONCAT('youtube:', CAST(video_id AS STRING)) AS video_key
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.yt_videos`
        WHERE channel_id NOT IN ({youtube_bob_team_channel_ids_sql})

        UNION DISTINCT

        SELECT CONCAT('tiktok:', CAST(id AS STRING)) AS video_key
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.videos`

        UNION DISTINCT

        SELECT CONCAT('ins:', CAST(media_id AS STRING)) AS video_key
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.ig_media`
    ),
    -- 自动发布统一视频映射现在统一从 DIM 读取。
    auto_publish_video_task_dim AS (
        SELECT
            video_key,
            unified_video_key
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_auto_publish_video_task`
    ),
    -- 目标日期内的投资人视频事实。
    investor_video_base AS (
        SELECT
            v.dt,
            v.channel,
            v.video_id,
            v.video_key,
            v.is_new_video,
            v.is_active_video,
            v.daily_view_increment,
            v.day_end_views,
            v.new_video_contributed_view_count,
            COALESCE(m.unified_video_key, v.video_key) AS investor_video_key
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_video_daily` v
        INNER JOIN johnson_video_keys j
            ON v.video_key = j.video_key
        LEFT JOIN auto_publish_video_task_dim m
            ON v.video_key = m.video_key
        WHERE v.dt IN ({dates_str})
    ),
    -- 取待刷窗口涉及的统一视频键，后续只回看这些键的历史新增日。
    relevant_investor_video_keys AS (
        SELECT DISTINCT investor_video_key
        FROM investor_video_base
    ),
    -- 回看统一视频键的历史新增日，确保自动发布跨平台视频只在最早首次正播放日期记一次新增。
    investor_video_first_positive_dt AS (
        SELECT
            COALESCE(m.unified_video_key, v.video_key) AS investor_video_key,
            MIN(v.dt) AS first_positive_view_dt
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_video_daily` v
        INNER JOIN johnson_video_keys j
            ON v.video_key = j.video_key
        LEFT JOIN auto_publish_video_task_dim m
            ON v.video_key = m.video_key
        INNER JOIN relevant_investor_video_keys r
            ON COALESCE(m.unified_video_key, v.video_key) = r.investor_video_key
        WHERE v.is_new_video
            AND v.dt <= DATE '{max_date}'
        GROUP BY investor_video_key
    ),
    -- 读取 YouTube 视频时长，供制作成本口径复用。
    youtube_video_duration_dim AS (
        SELECT
            CAST(video_id AS STRING) AS video_id,
            SAFE_CAST(duration AS INT64) AS duration_sec
        FROM `{PROJECT_ID}.{VIDEO_SOURCE_DATASET_ID}.yt_videos`
        WHERE channel_id NOT IN ({youtube_bob_team_channel_ids_sql})
    ),
    -- 汇率表当前是单值字典，直接取 CNY -> USD 汇率。
    cny_usd_exchange_rate AS (
        SELECT
            MAX(exchange_rate) AS cny_to_usd_rate
        FROM `{PROJECT_ID}.{DATASET_ID}.currency_conversion_dict`
        WHERE UPPER(from_currency) = 'CNY'
            AND UPPER(to_currency) = 'USD'
    ),
    -- 汇总投资人口径的视频相关指标；只有视频数量按统一视频键去重，播放量保持原口径。
    video_metrics AS (
        SELECT
            v.dt,
            COUNT(
                DISTINCT IF(
                    f.first_positive_view_dt = v.dt,
                    v.investor_video_key,
                    NULL
                )
            ) AS new_video_count,
            COUNT(
                DISTINCT IF(v.is_active_video, v.investor_video_key, NULL)
            ) AS active_video_count,
            COALESCE(SUM(v.daily_view_increment), 0) AS new_video_view_count,
            COALESCE(SUM(v.new_video_contributed_view_count), 0)
                AS new_video_contributed_view_count,
            COUNT(
                DISTINCT IF(
                    f.first_positive_view_dt = DATE_SUB(v.dt, INTERVAL 6 DAY),
                    v.investor_video_key,
                    NULL
                )
            ) AS published_7d_cohort_video_count,
            COALESCE(SUM(IF(
                f.first_positive_view_dt = DATE_SUB(v.dt, INTERVAL 6 DAY),
                v.day_end_views,
                0
            )), 0) AS published_7d_cohort_view_count,
            COALESCE(SUM(IF(
                v.channel = 'youtube' AND v.is_new_video,
                COALESCE(y.duration_sec, 0),
                0
            )), 0) AS new_youtube_video_duration_sec
        FROM investor_video_base v
        LEFT JOIN investor_video_first_positive_dt f
            ON v.investor_video_key = f.investor_video_key
        LEFT JOIN youtube_video_duration_dim y
            ON v.channel = 'youtube'
            AND v.video_id = y.video_id
        GROUP BY v.dt
    ),
    -- 汇总当日新增下载数。
    download_metrics AS (
        SELECT
            dt,
            COALESCE(SUM(new_download_count), 0) AS new_download_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_appsflyer_download_daily`
        WHERE dt IN ({dates_str})
        GROUP BY dt
    ),
    -- 汇总 app 设备规模和已注册设备分母。
    device_metrics AS (
        SELECT
            dt,
            COUNT(DISTINCT IF(is_new_app_device, prop_device_id, NULL))
                AS new_device_count,
            COUNT(DISTINCT prop_device_id) AS active_device_count,
            COUNT(
                DISTINCT IF(has_registered_user_activity, prop_device_id, NULL)
            ) AS active_registered_app_device_count,
            COALESCE(SUM(registered_duration_sec), 0) AS total_duration_sec,
            COALESCE(SUM(registered_content_exposure_count), 0)
                AS exposure_item_count,
            COALESCE(SUM(registered_content_click_count), 0)
                AS click_item_count
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_registered_app_device_daily`
        WHERE dt IN ({dates_str})
        GROUP BY dt
    ),
    -- 汇总 app 已注册用户规模、留存和商详页指标。
    registered_user_metrics AS (
        SELECT
            dt,
            COUNT(DISTINCT prop_user_id) AS active_registered_user_count,
            COUNT(DISTINCT IF(is_next_day_retained, prop_user_id, NULL))
                AS retained_1d_user_count,
            COUNT(DISTINCT IF(is_day_7_retained, prop_user_id, NULL))
                AS retained_7d_user_count,
            COUNT(DISTINCT IF(product_detail_exposure_pv > 0, prop_user_id, NULL))
                AS product_detail_exposure_uv,
            COALESCE(SUM(product_detail_exposure_pv), 0)
                AS product_detail_exposure_pv
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_registered_app_user_daily`
        WHERE dt IN ({dates_str})
        GROUP BY dt
    )
    SELECT
        d.dt,
        COALESCE(v.new_video_count, 0) AS new_video_count,
        COALESCE(v.active_video_count, 0) AS active_video_count,
        COALESCE(v.new_video_view_count, 0) AS new_video_view_count,
        COALESCE(v.new_video_contributed_view_count, 0)
            AS new_video_contributed_view_count,
        CAST(CASE
            WHEN COALESCE(v.new_video_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(v.new_video_contributed_view_count, 0) AS NUMERIC)
                / CAST(v.new_video_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_new_video_view_count,
        CAST(CASE
            WHEN COALESCE(v.published_7d_cohort_video_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(v.published_7d_cohort_view_count, 0) AS NUMERIC)
                / CAST(v.published_7d_cohort_video_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS new_video_7d_avg_view_count,
        CAST(ROUND(
            CAST(COALESCE(v.new_youtube_video_duration_sec, 0) AS NUMERIC)
            * 1.05
            * COALESCE(r.cny_to_usd_rate, 0),
            4
        ) AS NUMERIC) AS video_production_cost_usd,
        CAST(CASE
            WHEN COALESCE(v.new_video_view_count, 0) = 0 THEN 0
            ELSE ROUND(
                (
                    CAST(COALESCE(v.new_youtube_video_duration_sec, 0) AS NUMERIC)
                    * 1.05
                    * COALESCE(r.cny_to_usd_rate, 0)
                )
                / CAST(v.new_video_view_count AS NUMERIC)
                * 1000,
                4
            )
        END AS NUMERIC) AS cost_per_thousand_views_usd,
        COALESCE(dl.new_download_count, 0) AS new_download_count,
        CAST(CASE
            WHEN COALESCE(v.new_video_view_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(dl.new_download_count, 0) AS NUMERIC)
                / CAST(v.new_video_view_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS play_download_conversion_rate,
        CAST(NULL AS NUMERIC) AS avg_video_download_conversion_count,
        COALESCE(dev.new_device_count, 0) AS new_device_count,
        COALESCE(dev.active_device_count, 0) AS active_device_count,
        COALESCE(u.active_registered_user_count, 0) AS active_registered_user_count,
        CAST(CASE
            WHEN COALESCE(dev.active_registered_app_device_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(dev.total_duration_sec, 0) AS NUMERIC)
                / CAST(dev.active_registered_app_device_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_duration_sec,
        CAST(CASE
            WHEN COALESCE(dev.active_registered_app_device_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(dev.exposure_item_count, 0) AS NUMERIC)
                / CAST(dev.active_registered_app_device_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_content_exposure_count,
        CAST(CASE
            WHEN COALESCE(dev.active_registered_app_device_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(dev.click_item_count, 0) AS NUMERIC)
                / CAST(dev.active_registered_app_device_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_content_click_count,
        CAST(CASE
            WHEN COALESCE(dev.exposure_item_count, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(dev.click_item_count, 0) AS NUMERIC)
                / CAST(dev.exposure_item_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS content_ctr,
        COALESCE(u.product_detail_exposure_uv, 0) AS product_detail_exposure_uv,
        COALESCE(u.product_detail_exposure_pv, 0) AS product_detail_exposure_pv,
        CAST(CASE
            WHEN COALESCE(u.product_detail_exposure_uv, 0) = 0 THEN 0
            ELSE ROUND(
                CAST(COALESCE(u.product_detail_exposure_pv, 0) AS NUMERIC)
                / CAST(u.product_detail_exposure_uv AS NUMERIC),
                4
            )
        END AS NUMERIC) AS avg_product_detail_exposure_count,
        CAST(CASE
            WHEN COALESCE(u.active_registered_user_count, 0) = 0 THEN 0
            WHEN DATE_ADD(d.dt, INTERVAL 1 DAY) > DATE_SUB(
                CURRENT_DATE('{TORONTO_TZ}'),
                INTERVAL 1 DAY
            )
                THEN NULL
            ELSE ROUND(
                CAST(COALESCE(u.retained_1d_user_count, 0) AS NUMERIC)
                / CAST(u.active_registered_user_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS next_day_retention_rate,
        CAST(CASE
            WHEN COALESCE(u.active_registered_user_count, 0) = 0 THEN 0
            WHEN DATE_ADD(d.dt, INTERVAL 7 DAY) > DATE_SUB(
                CURRENT_DATE('{TORONTO_TZ}'),
                INTERVAL 1 DAY
            )
                THEN NULL
            ELSE ROUND(
                CAST(COALESCE(u.retained_7d_user_count, 0) AS NUMERIC)
                / CAST(u.active_registered_user_count AS NUMERIC),
                4
            )
        END AS NUMERIC) AS day_7_retention_rate,
        CURRENT_TIMESTAMP() AS update_time
    FROM date_list d
    LEFT JOIN video_metrics v
        ON d.dt = v.dt
    LEFT JOIN download_metrics dl
        ON d.dt = dl.dt
    LEFT JOIN device_metrics dev
        ON d.dt = dev.dt
    LEFT JOIN registered_user_metrics u
        ON d.dt = u.dt
    CROSS JOIN cny_usd_exchange_rate r
    """

    logging.info("开始处理: %s", TABLE_NAME)
    client.query(query).result()
    logging.info("%s 刷新完成, 处理日期: %s", TABLE_NAME, dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_ads_daily_investor_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 ads_daily_investor 日期需要处理")
        raise SystemExit(0)

    run_ads_daily_investor(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("%s 完成, 耗时: %.1f 秒", TABLE_NAME, elapsed)
