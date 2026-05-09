from datetime import date, datetime, timezone
import logging
from pathlib import Path
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
TORONTO_TZ = "America/Toronto"
TABLE_NAME = "dws_user_retention_feature_daily"
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


def get_user_retention_feature_dates_to_process(
    window_hours: int = INCREMENTAL_WINDOW_HOURS,
) -> list[date]:
    """
    留存特征 DWS 只消费 dws_user_activity_daily。

    规则：
    - 目标表不存在或为空时，按用户活动 DWS 已落地日期全量初始化
    - 目标表已存在时，只取最近 6 小时内用户活动 DWS 变更的 `dt`
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


def run_dws_user_retention_feature_daily(dates: list[date]) -> None:
    """
    产出留存用户特征事实层。

    粒度：
    - dt × user_group × feature_name

    指标：
    - retained / not retained 的用户数
    - 八类特征的 retained_value / not_retained_value
    - 停留时长的四类分子字段
    """
    if not dates:
        logging.info("没有新的 dws_user_retention_feature_daily 日期需要处理")
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
        feature_order,
        feature_name,
        next_day_retained_value,
        day_7_retained_value,
        next_day_not_retained_value,
        day_7_not_retained_value,
        next_day_retained_user_count,
        day_7_retained_user_count,
        next_day_not_retained_user_count,
        day_7_not_retained_user_count,
        next_day_retained_duration_sum_sec,
        day_7_retained_duration_sum_sec,
        next_day_not_retained_duration_sum_sec,
        day_7_not_retained_duration_sum_sec,
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
    -- 目标日期与用户群脚手架。
    group_keys AS (
        SELECT
            d.dt,
            g.user_group
        FROM target_dates d
        CROSS JOIN user_group_list g
    ),
    -- 给每个日期和用户群标记观察日是否已开始，避免 0 cohort 被误判成 NULL。
    group_keys_with_observation_status AS (
        SELECT
            dt,
            user_group,
            DATE_ADD(dt, INTERVAL 1 DAY) <= CURRENT_DATE('{TORONTO_TZ}')
                AS is_next_day_observation_started,
            DATE_ADD(dt, INTERVAL 7 DAY) <= CURRENT_DATE('{TORONTO_TZ}')
                AS is_day_7_observation_started
        FROM group_keys
    ),
    -- 固定产出展示层排序使用的特征顺序。
    feature_list AS (
        SELECT 1 AS feature_order, '日均停留时长' AS feature_name
        UNION ALL
        SELECT 2 AS feature_order, 'AI试穿' AS feature_name
        UNION ALL
        SELECT 3 AS feature_order, '关注账号' AS feature_name
        UNION ALL
        SELECT 4 AS feature_order, '浏览商详' AS feature_name
        UNION ALL
        SELECT 5 AS feature_order, '创建模特' AS feature_name
        UNION ALL
        SELECT 6 AS feature_order, '搜索' AS feature_name
        UNION ALL
        SELECT 7 AS feature_order, '衣橱渗透' AS feature_name
        UNION ALL
        SELECT 8 AS feature_order, '非衣橱渗透' AS feature_name
    ),
    -- 当前批次用户活动明细，只保留目标日期。
    user_activity AS (
        SELECT
            dt,
            user_id,
            first_active_date,
            is_new_user,
            is_next_day_observation_started,
            is_day_7_observation_started,
            is_next_day_retained,
            is_day_7_retained,
            duration_sec,
            viewed_product_detail,
            used_search,
            used_ai_tryon,
            created_model,
            followed_account,
            closet_penetrated
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_activity_daily`
        WHERE dt IN ({dates_str})
    ),
    -- 把用户活动展开到新用户 / 老用户 / 全量登录用户三种展示分群。
    activity_by_group AS (
        SELECT
            dt,
            user_id,
            '全量登录用户' AS user_group,
            is_next_day_observation_started,
            is_day_7_observation_started,
            is_next_day_retained,
            is_day_7_retained,
            duration_sec,
            viewed_product_detail,
            used_search,
            used_ai_tryon,
            created_model,
            followed_account,
            closet_penetrated
        FROM user_activity

        UNION ALL

        SELECT
            dt,
            user_id,
            '新用户' AS user_group,
            is_next_day_observation_started,
            is_day_7_observation_started,
            is_next_day_retained,
            is_day_7_retained,
            duration_sec,
            viewed_product_detail,
            used_search,
            used_ai_tryon,
            created_model,
            followed_account,
            closet_penetrated
        FROM user_activity
        WHERE is_new_user

        UNION ALL

        SELECT
            dt,
            user_id,
            '老用户' AS user_group,
            is_next_day_observation_started,
            is_day_7_observation_started,
            is_next_day_retained,
            is_day_7_retained,
            duration_sec,
            viewed_product_detail,
            used_search,
            used_ai_tryon,
            created_model,
            followed_account,
            closet_penetrated
        FROM user_activity
        WHERE NOT is_new_user
    ),
    -- 四类 cohort 用户数聚合层，供所有特征行复用。
    cohort_group_counts AS (
        SELECT
            k.dt,
            k.user_group,
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
            END AS day_7_not_retained_user_count
        FROM group_keys_with_observation_status k
        LEFT JOIN activity_by_group b
            ON b.dt = k.dt
            AND b.user_group = k.user_group
        GROUP BY
            k.dt,
            k.user_group,
            k.is_next_day_observation_started,
            k.is_day_7_observation_started
    ),
    -- 日均停留时长特征聚合层。
    duration_feature AS (
        SELECT
            k.dt,
            k.user_group,
            1 AS feature_order,
            '日均停留时长' AS feature_name,
            CASE
                WHEN NOT k.is_next_day_observation_started
                    THEN NULL
                WHEN COUNTIF(b.is_next_day_retained) = 0 THEN 0
                ELSE CAST(ROUND(AVG(IF(b.is_next_day_retained, b.duration_sec, NULL))) AS INT64)
            END AS next_day_retained_value,
            CASE
                WHEN NOT k.is_day_7_observation_started
                    THEN NULL
                WHEN COUNTIF(b.is_day_7_retained) = 0 THEN 0
                ELSE CAST(ROUND(AVG(IF(b.is_day_7_retained, b.duration_sec, NULL))) AS INT64)
            END AS day_7_retained_value,
            CASE
                WHEN NOT k.is_next_day_observation_started
                    THEN NULL
                WHEN COUNTIF(
                    b.user_id IS NOT NULL
                    AND NOT COALESCE(b.is_next_day_retained, FALSE)
                ) = 0 THEN 0
                ELSE CAST(
                    ROUND(
                        AVG(
                            IF(
                                b.user_id IS NOT NULL
                                AND NOT COALESCE(b.is_next_day_retained, FALSE),
                                b.duration_sec,
                                NULL
                            )
                        )
                    )
                    AS INT64
                )
            END AS next_day_not_retained_value,
            CASE
                WHEN NOT k.is_day_7_observation_started
                    THEN NULL
                WHEN COUNTIF(
                    b.user_id IS NOT NULL
                    AND NOT COALESCE(b.is_day_7_retained, FALSE)
                ) = 0 THEN 0
                ELSE CAST(
                    ROUND(
                        AVG(
                            IF(
                                b.user_id IS NOT NULL
                                AND NOT COALESCE(b.is_day_7_retained, FALSE),
                                b.duration_sec,
                                NULL
                            )
                        )
                    )
                    AS INT64
                )
            END AS day_7_not_retained_value,
            CASE
                WHEN NOT k.is_next_day_observation_started
                    THEN NULL
                ELSE CAST(ROUND(COALESCE(SUM(IF(b.is_next_day_retained, b.duration_sec, NULL)), 0)) AS INT64)
            END AS next_day_retained_duration_sum_sec,
            CASE
                WHEN NOT k.is_day_7_observation_started
                    THEN NULL
                ELSE CAST(ROUND(COALESCE(SUM(IF(b.is_day_7_retained, b.duration_sec, NULL)), 0)) AS INT64)
            END AS day_7_retained_duration_sum_sec,
            CASE
                WHEN NOT k.is_next_day_observation_started
                    THEN NULL
                ELSE CAST(
                    ROUND(
                        COALESCE(
                            SUM(
                                IF(
                                    b.user_id IS NOT NULL
                                    AND NOT COALESCE(b.is_next_day_retained, FALSE),
                                    b.duration_sec,
                                    NULL
                                )
                            ),
                            0
                        )
                    )
                    AS INT64
                )
            END AS next_day_not_retained_duration_sum_sec,
            CASE
                WHEN NOT k.is_day_7_observation_started
                    THEN NULL
                ELSE CAST(
                    ROUND(
                        COALESCE(
                            SUM(
                                IF(
                                    b.user_id IS NOT NULL
                                    AND NOT COALESCE(b.is_day_7_retained, FALSE),
                                    b.duration_sec,
                                    NULL
                                )
                            ),
                            0
                        )
                    )
                    AS INT64
                )
            END AS day_7_not_retained_duration_sum_sec
        FROM group_keys_with_observation_status k
        LEFT JOIN activity_by_group b
            ON b.dt = k.dt
            AND b.user_group = k.user_group
        GROUP BY
            k.dt,
            k.user_group,
            k.is_next_day_observation_started,
            k.is_day_7_observation_started
    ),
    -- AI 试穿特征聚合层。
    ai_tryon_feature AS (
        SELECT
            k.dt,
            k.user_group,
            2 AS feature_order,
            'AI试穿' AS feature_name,
            CASE
                WHEN NOT k.is_next_day_observation_started
                    THEN NULL
                ELSE COUNTIF(b.is_next_day_retained AND b.used_ai_tryon = 1)
            END AS next_day_retained_value,
            CASE
                WHEN NOT k.is_day_7_observation_started
                    THEN NULL
                ELSE COUNTIF(b.is_day_7_retained AND b.used_ai_tryon = 1)
            END AS day_7_retained_value,
            CASE
                WHEN NOT k.is_next_day_observation_started
                    THEN NULL
                ELSE COUNTIF(
                    b.user_id IS NOT NULL
                    AND NOT COALESCE(b.is_next_day_retained, FALSE)
                    AND b.used_ai_tryon = 1
                )
            END AS next_day_not_retained_value,
            CASE
                WHEN NOT k.is_day_7_observation_started
                    THEN NULL
                ELSE COUNTIF(
                    b.user_id IS NOT NULL
                    AND NOT COALESCE(b.is_day_7_retained, FALSE)
                    AND b.used_ai_tryon = 1
                )
            END AS day_7_not_retained_value,
            NULL AS next_day_retained_duration_sum_sec,
            NULL AS day_7_retained_duration_sum_sec,
            NULL AS next_day_not_retained_duration_sum_sec,
            NULL AS day_7_not_retained_duration_sum_sec
        FROM group_keys_with_observation_status k
        LEFT JOIN activity_by_group b
            ON b.dt = k.dt
            AND b.user_group = k.user_group
        GROUP BY
            k.dt,
            k.user_group,
            k.is_next_day_observation_started,
            k.is_day_7_observation_started
    ),
    -- 关注账号特征聚合层。
    follow_feature AS (
        SELECT
            k.dt,
            k.user_group,
            3 AS feature_order,
            '关注账号' AS feature_name,
            CASE
                WHEN NOT k.is_next_day_observation_started
                    THEN NULL
                ELSE COUNTIF(b.is_next_day_retained AND b.followed_account = 1)
            END AS next_day_retained_value,
            CASE
                WHEN NOT k.is_day_7_observation_started
                    THEN NULL
                ELSE COUNTIF(b.is_day_7_retained AND b.followed_account = 1)
            END AS day_7_retained_value,
            CASE
                WHEN NOT k.is_next_day_observation_started
                    THEN NULL
                ELSE COUNTIF(
                    b.user_id IS NOT NULL
                    AND NOT COALESCE(b.is_next_day_retained, FALSE)
                    AND b.followed_account = 1
                )
            END AS next_day_not_retained_value,
            CASE
                WHEN NOT k.is_day_7_observation_started
                    THEN NULL
                ELSE COUNTIF(
                    b.user_id IS NOT NULL
                    AND NOT COALESCE(b.is_day_7_retained, FALSE)
                    AND b.followed_account = 1
                )
            END AS day_7_not_retained_value,
            NULL AS next_day_retained_duration_sum_sec,
            NULL AS day_7_retained_duration_sum_sec,
            NULL AS next_day_not_retained_duration_sum_sec,
            NULL AS day_7_not_retained_duration_sum_sec
        FROM group_keys_with_observation_status k
        LEFT JOIN activity_by_group b
            ON b.dt = k.dt
            AND b.user_group = k.user_group
        GROUP BY
            k.dt,
            k.user_group,
            k.is_next_day_observation_started,
            k.is_day_7_observation_started
    ),
    -- 浏览商详特征聚合层。
    product_detail_feature AS (
        SELECT
            k.dt,
            k.user_group,
            4 AS feature_order,
            '浏览商详' AS feature_name,
            CASE
                WHEN NOT k.is_next_day_observation_started
                    THEN NULL
                ELSE COUNTIF(b.is_next_day_retained AND b.viewed_product_detail = 1)
            END AS next_day_retained_value,
            CASE
                WHEN NOT k.is_day_7_observation_started
                    THEN NULL
                ELSE COUNTIF(b.is_day_7_retained AND b.viewed_product_detail = 1)
            END AS day_7_retained_value,
            CASE
                WHEN NOT k.is_next_day_observation_started
                    THEN NULL
                ELSE COUNTIF(
                    b.user_id IS NOT NULL
                    AND NOT COALESCE(b.is_next_day_retained, FALSE)
                    AND b.viewed_product_detail = 1
                )
            END AS next_day_not_retained_value,
            CASE
                WHEN NOT k.is_day_7_observation_started
                    THEN NULL
                ELSE COUNTIF(
                    b.user_id IS NOT NULL
                    AND NOT COALESCE(b.is_day_7_retained, FALSE)
                    AND b.viewed_product_detail = 1
                )
            END AS day_7_not_retained_value,
            NULL AS next_day_retained_duration_sum_sec,
            NULL AS day_7_retained_duration_sum_sec,
            NULL AS next_day_not_retained_duration_sum_sec,
            NULL AS day_7_not_retained_duration_sum_sec
        FROM group_keys_with_observation_status k
        LEFT JOIN activity_by_group b
            ON b.dt = k.dt
            AND b.user_group = k.user_group
        GROUP BY
            k.dt,
            k.user_group,
            k.is_next_day_observation_started,
            k.is_day_7_observation_started
    ),
    -- 创建模特特征聚合层。
    model_feature AS (
        SELECT
            k.dt,
            k.user_group,
            5 AS feature_order,
            '创建模特' AS feature_name,
            CASE
                WHEN NOT k.is_next_day_observation_started
                    THEN NULL
                ELSE COUNTIF(b.is_next_day_retained AND b.created_model = 1)
            END AS next_day_retained_value,
            CASE
                WHEN NOT k.is_day_7_observation_started
                    THEN NULL
                ELSE COUNTIF(b.is_day_7_retained AND b.created_model = 1)
            END AS day_7_retained_value,
            CASE
                WHEN NOT k.is_next_day_observation_started
                    THEN NULL
                ELSE COUNTIF(
                    b.user_id IS NOT NULL
                    AND NOT COALESCE(b.is_next_day_retained, FALSE)
                    AND b.created_model = 1
                )
            END AS next_day_not_retained_value,
            CASE
                WHEN NOT k.is_day_7_observation_started
                    THEN NULL
                ELSE COUNTIF(
                    b.user_id IS NOT NULL
                    AND NOT COALESCE(b.is_day_7_retained, FALSE)
                    AND b.created_model = 1
                )
            END AS day_7_not_retained_value,
            NULL AS next_day_retained_duration_sum_sec,
            NULL AS day_7_retained_duration_sum_sec,
            NULL AS next_day_not_retained_duration_sum_sec,
            NULL AS day_7_not_retained_duration_sum_sec
        FROM group_keys_with_observation_status k
        LEFT JOIN activity_by_group b
            ON b.dt = k.dt
            AND b.user_group = k.user_group
        GROUP BY
            k.dt,
            k.user_group,
            k.is_next_day_observation_started,
            k.is_day_7_observation_started
    ),
    -- 搜索特征聚合层。
    search_feature AS (
        SELECT
            k.dt,
            k.user_group,
            6 AS feature_order,
            '搜索' AS feature_name,
            CASE
                WHEN NOT k.is_next_day_observation_started
                    THEN NULL
                ELSE COUNTIF(b.is_next_day_retained AND b.used_search = 1)
            END AS next_day_retained_value,
            CASE
                WHEN NOT k.is_day_7_observation_started
                    THEN NULL
                ELSE COUNTIF(b.is_day_7_retained AND b.used_search = 1)
            END AS day_7_retained_value,
            CASE
                WHEN NOT k.is_next_day_observation_started
                    THEN NULL
                ELSE COUNTIF(
                    b.user_id IS NOT NULL
                    AND NOT COALESCE(b.is_next_day_retained, FALSE)
                    AND b.used_search = 1
                )
            END AS next_day_not_retained_value,
            CASE
                WHEN NOT k.is_day_7_observation_started
                    THEN NULL
                ELSE COUNTIF(
                    b.user_id IS NOT NULL
                    AND NOT COALESCE(b.is_day_7_retained, FALSE)
                    AND b.used_search = 1
                )
            END AS day_7_not_retained_value,
            NULL AS next_day_retained_duration_sum_sec,
            NULL AS day_7_retained_duration_sum_sec,
            NULL AS next_day_not_retained_duration_sum_sec,
            NULL AS day_7_not_retained_duration_sum_sec
        FROM group_keys_with_observation_status k
        LEFT JOIN activity_by_group b
            ON b.dt = k.dt
            AND b.user_group = k.user_group
        GROUP BY
            k.dt,
            k.user_group,
            k.is_next_day_observation_started,
            k.is_day_7_observation_started
    ),
    -- 衣橱渗透特征聚合层。
    closet_penetration_feature AS (
        SELECT
            k.dt,
            k.user_group,
            7 AS feature_order,
            '衣橱渗透' AS feature_name,
            CASE
                WHEN NOT k.is_next_day_observation_started
                    THEN NULL
                ELSE COUNTIF(b.is_next_day_retained AND b.closet_penetrated = 1)
            END AS next_day_retained_value,
            CASE
                WHEN NOT k.is_day_7_observation_started
                    THEN NULL
                ELSE COUNTIF(b.is_day_7_retained AND b.closet_penetrated = 1)
            END AS day_7_retained_value,
            CASE
                WHEN NOT k.is_next_day_observation_started
                    THEN NULL
                ELSE COUNTIF(
                    b.user_id IS NOT NULL
                    AND NOT COALESCE(b.is_next_day_retained, FALSE)
                    AND b.closet_penetrated = 1
                )
            END AS next_day_not_retained_value,
            CASE
                WHEN NOT k.is_day_7_observation_started
                    THEN NULL
                ELSE COUNTIF(
                    b.user_id IS NOT NULL
                    AND NOT COALESCE(b.is_day_7_retained, FALSE)
                    AND b.closet_penetrated = 1
                )
            END AS day_7_not_retained_value,
            NULL AS next_day_retained_duration_sum_sec,
            NULL AS day_7_retained_duration_sum_sec,
            NULL AS next_day_not_retained_duration_sum_sec,
            NULL AS day_7_not_retained_duration_sum_sec
        FROM group_keys_with_observation_status k
        LEFT JOIN activity_by_group b
            ON b.dt = k.dt
            AND b.user_group = k.user_group
        GROUP BY
            k.dt,
            k.user_group,
            k.is_next_day_observation_started,
            k.is_day_7_observation_started
    ),
    -- 非衣橱渗透特征聚合层。
    non_closet_penetration_feature AS (
        SELECT
            k.dt,
            k.user_group,
            8 AS feature_order,
            '非衣橱渗透' AS feature_name,
            CASE
                WHEN NOT k.is_next_day_observation_started
                    THEN NULL
                ELSE COUNTIF(
                    b.is_next_day_retained
                    AND b.closet_penetrated = 0
                )
            END AS next_day_retained_value,
            CASE
                WHEN NOT k.is_day_7_observation_started
                    THEN NULL
                ELSE COUNTIF(
                    b.is_day_7_retained
                    AND b.closet_penetrated = 0
                )
            END AS day_7_retained_value,
            CASE
                WHEN NOT k.is_next_day_observation_started
                    THEN NULL
                ELSE COUNTIF(
                    b.user_id IS NOT NULL
                    AND NOT COALESCE(b.is_next_day_retained, FALSE)
                    AND b.closet_penetrated = 0
                )
            END AS next_day_not_retained_value,
            CASE
                WHEN NOT k.is_day_7_observation_started
                    THEN NULL
                ELSE COUNTIF(
                    b.user_id IS NOT NULL
                    AND NOT COALESCE(b.is_day_7_retained, FALSE)
                    AND b.closet_penetrated = 0
                )
            END AS day_7_not_retained_value,
            NULL AS next_day_retained_duration_sum_sec,
            NULL AS day_7_retained_duration_sum_sec,
            NULL AS next_day_not_retained_duration_sum_sec,
            NULL AS day_7_not_retained_duration_sum_sec
        FROM group_keys_with_observation_status k
        LEFT JOIN activity_by_group b
            ON b.dt = k.dt
            AND b.user_group = k.user_group
        GROUP BY
            k.dt,
            k.user_group,
            k.is_next_day_observation_started,
            k.is_day_7_observation_started
    ),
    -- 汇总所有特征行，供最终结果统一回写。
    feature_values AS (
        SELECT * FROM duration_feature
        UNION ALL
        SELECT * FROM ai_tryon_feature
        UNION ALL
        SELECT * FROM follow_feature
        UNION ALL
        SELECT * FROM product_detail_feature
        UNION ALL
        SELECT * FROM model_feature
        UNION ALL
        SELECT * FROM search_feature
        UNION ALL
        SELECT * FROM closet_penetration_feature
        UNION ALL
        SELECT * FROM non_closet_penetration_feature
    ),
    -- 生成最终结果主键，保证每个日期和用户群都有完整 8 行。
    keys AS (
        SELECT
            g.dt,
            g.user_group,
            f.feature_order,
            f.feature_name
        FROM group_keys g
        CROSS JOIN feature_list f
    )
    SELECT
        k.dt,
        k.user_group,
        k.feature_order,
        k.feature_name,
        v.next_day_retained_value,
        v.day_7_retained_value,
        v.next_day_not_retained_value,
        v.day_7_not_retained_value,
        c.next_day_retained_user_count,
        c.day_7_retained_user_count,
        c.next_day_not_retained_user_count,
        c.day_7_not_retained_user_count,
        v.next_day_retained_duration_sum_sec,
        v.day_7_retained_duration_sum_sec,
        v.next_day_not_retained_duration_sum_sec,
        v.day_7_not_retained_duration_sum_sec,
        CURRENT_TIMESTAMP() AS update_time
    FROM keys k
    LEFT JOIN feature_values v
        ON v.dt = k.dt
        AND v.user_group = k.user_group
        AND v.feature_order = k.feature_order
    LEFT JOIN cohort_group_counts c
        ON c.dt = k.dt
        AND c.user_group = k.user_group;
    """
    logging.info("开始处理: dws_user_retention_feature_daily")
    job = client.query(query)
    job.result()
    logging.info("dws_user_retention_feature_daily 刷新完成, 处理日期: %s", dates)


if __name__ == "__main__":
    log_path = configure_logging()
    logging.info("日志文件: %s", log_path)

    start_time = datetime.now(timezone.utc)
    dates = get_user_retention_feature_dates_to_process()
    logging.info("待处理日期: %s", dates if dates else "[]")

    if not dates:
        logging.info("没有新的 dws_user_retention_feature_daily 日期需要处理")
        raise SystemExit(0)

    run_dws_user_retention_feature_daily(dates)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logging.info("dws_user_retention_feature_daily 完成, 耗时: %.1f 秒", elapsed)
