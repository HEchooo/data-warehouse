import logging
from datetime import date

from google.cloud import bigquery

PROJECT_ID = "my-project-8584-jetonai"
DATASET_ID = "decom"
V3_DATASET_ID = "v3_decom"
TORONTO_TZ = "America/Toronto"
client = bigquery.Client(project=PROJECT_ID)


def dates_to_sql_list(dates: list[date]) -> str:
    return ", ".join([f"'{d}'" for d in dates])


def dates_to_array_sql(dates: list[date]) -> str:
    return ", ".join([f"DATE '{d}'" for d in dates])


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


def service_ts_expr(field_name: str = "gmt_create") -> str:
    """v3 服务表的 gmt_create 是 UTC DATETIME，这里统一转成 TIMESTAMP。"""
    return f"TIMESTAMP({field_name}, 'UTC')"


def build_ads_daily_retention_user_feature_query(dates: list[date]) -> str:
    dates_str = dates_to_sql_list(dates)
    dates_array_sql = dates_to_array_sql(dates)
    min_date = min(dates)
    max_date = max(dates)
    event_ts = event_ts_expr()
    service_ts = service_ts_expr()

    return f"""
    DELETE FROM `{PROJECT_ID}.{DATASET_ID}.ads_daily_retention_user_feature`
    WHERE dt IN ({dates_str});

    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.ads_daily_retention_user_feature`
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
    -- 当前批次要刷新的目标日期列表。
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
    -- 生成日期与用户群脚手架，保证没有 cohort 的分群也能稳定产出 0 行。
    group_keys AS (
        SELECT
            d.dt,
            g.user_group
        FROM target_dates d
        CROSS JOIN user_group_list g
    ),
    -- 给每个日期与用户群打上“次日/7日观察日是否已开始”的标记，供后续统一复用。
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
    -- 固定产出报表行顺序，避免展示层再做人为排序。
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
    ),
    -- 目标日期内的已登录活跃用户 cohort，按 user 粒度去重后收敛新老用户口径。
    cohort_users AS (
        SELECT
            dt,
            prop_user_id AS user_id,
            MIN(first_active_date) AS first_active_date
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        WHERE dt IN (SELECT dt FROM target_dates)
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
        GROUP BY dt, user_id
    ),
    -- 目标日期向后 7 天内的已登录活跃用户集合，用于判断次留和 7 留。
    future_active_users AS (
        SELECT DISTINCT
            dt,
            prop_user_id AS user_id
        FROM `{PROJECT_ID}.{DATASET_ID}.dws_user_daily`
        WHERE dt BETWEEN DATE_ADD(DATE '{min_date}', INTERVAL 1 DAY)
            AND LEAST(
                DATE_ADD(DATE '{max_date}', INTERVAL 7 DAY),
                CURRENT_DATE('{TORONTO_TZ}')
            )
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
    ),
    -- 给每个 cohort 用户打上次留、7留以及观察日是否已开始的标记。
    cohort_users_with_retention AS (
        SELECT
            c.dt,
            c.user_id,
            c.first_active_date,
            DATE_ADD(c.dt, INTERVAL 1 DAY) <= CURRENT_DATE('{TORONTO_TZ}')
                AS is_next_day_observation_started,
            DATE_ADD(c.dt, INTERVAL 7 DAY) <= CURRENT_DATE('{TORONTO_TZ}')
                AS is_day_7_observation_started,
            n1.user_id IS NOT NULL AS is_next_day_retained,
            n7.user_id IS NOT NULL AS is_day_7_retained
        FROM cohort_users c
        LEFT JOIN future_active_users n1
            ON n1.user_id = c.user_id
            AND n1.dt = DATE_ADD(c.dt, INTERVAL 1 DAY)
        LEFT JOIN future_active_users n7
            ON n7.user_id = c.user_id
            AND n7.dt = DATE_ADD(c.dt, INTERVAL 7 DAY)
    ),
    -- 把 cohort 用户展开到“新用户 / 老用户 / 全量登录用户”三种展示分群。
    cohort_users_by_group AS (
        SELECT
            dt,
            user_id,
            '全量登录用户' AS user_group,
            is_next_day_observation_started,
            is_day_7_observation_started,
            is_next_day_retained,
            is_day_7_retained
        FROM cohort_users_with_retention

        UNION ALL

        SELECT
            dt,
            user_id,
            '新用户' AS user_group,
            is_next_day_observation_started,
            is_day_7_observation_started,
            is_next_day_retained,
            is_day_7_retained
        FROM cohort_users_with_retention
        WHERE first_active_date = dt

        UNION ALL

        SELECT
            dt,
            user_id,
            '老用户' AS user_group,
            is_next_day_observation_started,
            is_day_7_observation_started,
            is_next_day_retained,
            is_day_7_retained
        FROM cohort_users_with_retention
        WHERE first_active_date < dt
    ),
    -- 目标日期内已登录事件底表，统一为行为口径提供用户级输入。
    logged_in_event_base AS (
        SELECT
            DATE({event_ts}, '{TORONTO_TZ}') AS dt,
            prop_user_id AS user_id,
            event_name,
            COALESCE(raw_event_id, hash_id) AS event_id,
            CAST(args_session_duration AS FLOAT64) AS duration_ms
        FROM `{PROJECT_ID}.{DATASET_ID}.dwd_event_log`
        WHERE DATE({event_ts}, '{TORONTO_TZ}') IN (SELECT dt FROM target_dates)
            AND prop_user_id IS NOT NULL
            AND prop_user_id != ''
    ),
    -- 对 app_launch 事件按用户和原始事件标识去重，再汇总昨日停留时长（秒）。
    user_duration_daily AS (
        SELECT
            dt,
            user_id,
            CAST(ROUND(COALESCE(SUM(duration_ms), 0) / 1000.0) AS INT64) AS duration_sec
        FROM (
            SELECT
                dt,
                user_id,
                event_id,
                MAX(duration_ms) AS duration_ms
            FROM logged_in_event_base
            WHERE event_name = 'app_launch'
                AND event_id IS NOT NULL
                AND duration_ms IS NOT NULL
            GROUP BY dt, user_id, event_id
        )
        GROUP BY dt, user_id
    ),
    -- 统计事件侧“浏览商详 / 搜索”两类行为是否发生过。
    event_behavior_daily AS (
        SELECT
            dt,
            user_id,
            MAX(IF(event_name = 'v_product_detail', 1, 0)) AS viewed_product_detail,
            MAX(IF(event_name IN ('c_home_search', 'c_shop_search'), 1, 0)) AS used_search
        FROM logged_in_event_base
        GROUP BY dt, user_id
    ),
    -- 服务端 AI 试穿行为，按天去重到用户级。
    tryon_behavior_daily AS (
        SELECT
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            COALESCE(
                NULLIF(JSON_VALUE(request_param, '$.user_id'), ''),
                NULLIF(JSON_VALUE(request_param, '$.userId'), ''),
                NULLIF(CAST(user_id AS STRING), '')
            ) AS user_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE DATE({service_ts}, '{TORONTO_TZ}') IN (SELECT dt FROM target_dates)
            AND task_type IN ('TRY_ON', 'STYLE_ME')
        GROUP BY dt, user_id
    ),
    -- 服务端创建模特行为，按天去重到用户级。
    model_behavior_daily AS (
        SELECT
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            COALESCE(
                NULLIF(JSON_VALUE(request_param, '$.user_id'), ''),
                NULLIF(JSON_VALUE(request_param, '$.userId'), ''),
                NULLIF(CAST(user_id AS STRING), '')
            ) AS user_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.common_ext_ai_task`
        WHERE DATE({service_ts}, '{TORONTO_TZ}') IN (SELECT dt FROM target_dates)
            AND task_type = 'MODEL'
        GROUP BY dt, user_id
    ),
    -- 服务端关注行为，按天去重到用户级。
    follow_behavior_daily AS (
        SELECT
            DATE({service_ts}, '{TORONTO_TZ}') AS dt,
            CAST(user_id AS STRING) AS user_id
        FROM `{PROJECT_ID}.{V3_DATASET_ID}.community_star_follow`
        WHERE DATE({service_ts}, '{TORONTO_TZ}') IN (SELECT dt FROM target_dates)
            AND user_id IS NOT NULL
            AND CAST(user_id AS STRING) != ''
        GROUP BY dt, user_id
    ),
    -- 把留存 cohort 用户与各类特征合并成统一的用户级底座。
    cohort_user_feature_base AS (
        SELECT
            c.dt,
            c.user_group,
            c.user_id,
            c.is_next_day_observation_started,
            c.is_day_7_observation_started,
            c.is_next_day_retained,
            c.is_day_7_retained,
            COALESCE(d.duration_sec, 0) AS duration_sec,
            COALESCE(e.viewed_product_detail, 0) AS viewed_product_detail,
            COALESCE(e.used_search, 0) AS used_search,
            IF(t.user_id IS NOT NULL, 1, 0) AS used_ai_tryon,
            IF(m.user_id IS NOT NULL, 1, 0) AS created_model,
            IF(f.user_id IS NOT NULL, 1, 0) AS followed_account
        FROM cohort_users_by_group c
        LEFT JOIN user_duration_daily d
            ON d.dt = c.dt
            AND d.user_id = c.user_id
        LEFT JOIN event_behavior_daily e
            ON e.dt = c.dt
            AND e.user_id = c.user_id
        LEFT JOIN tryon_behavior_daily t
            ON t.dt = c.dt
            AND t.user_id = c.user_id
        LEFT JOIN model_behavior_daily m
            ON m.dt = c.dt
            AND m.user_id = c.user_id
        LEFT JOIN follow_behavior_daily f
            ON f.dt = c.dt
            AND f.user_id = c.user_id
    ),
    -- 先按日期和用户群收敛四类 cohort 用户数，供每个特征行直接复用。
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
        LEFT JOIN cohort_user_feature_base b
            ON b.dt = k.dt
            AND b.user_group = k.user_group
        GROUP BY
            k.dt,
            k.user_group,
            k.is_next_day_observation_started,
            k.is_day_7_observation_started
    ),
    -- 汇总“日均停留时长”特征的四类 cohort 值。
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
                ELSE CAST(
                    ROUND(
                        COALESCE(
                            SUM(IF(b.is_next_day_retained, b.duration_sec, NULL)),
                            0
                        )
                    )
                    AS INT64
                )
            END AS next_day_retained_duration_sum_sec,
            CASE
                WHEN NOT k.is_day_7_observation_started
                    THEN NULL
                ELSE CAST(
                    ROUND(
                        COALESCE(
                            SUM(IF(b.is_day_7_retained, b.duration_sec, NULL)),
                            0
                        )
                    )
                    AS INT64
                )
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
        LEFT JOIN cohort_user_feature_base b
            ON b.dt = k.dt
            AND b.user_group = k.user_group
        GROUP BY
            k.dt,
            k.user_group,
            k.is_next_day_observation_started,
            k.is_day_7_observation_started
    ),
    -- 汇总“AI试穿”特征的四类 cohort 值。
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
        LEFT JOIN cohort_user_feature_base b
            ON b.dt = k.dt
            AND b.user_group = k.user_group
        GROUP BY
            k.dt,
            k.user_group,
            k.is_next_day_observation_started,
            k.is_day_7_observation_started
    ),
    -- 汇总“关注账号”特征的四类 cohort 值。
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
        LEFT JOIN cohort_user_feature_base b
            ON b.dt = k.dt
            AND b.user_group = k.user_group
        GROUP BY
            k.dt,
            k.user_group,
            k.is_next_day_observation_started,
            k.is_day_7_observation_started
    ),
    -- 汇总“浏览商详”特征的四类 cohort 值。
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
                    AND
                    NOT COALESCE(b.is_next_day_retained, FALSE)
                    AND b.viewed_product_detail = 1
                )
            END AS next_day_not_retained_value,
            CASE
                WHEN NOT k.is_day_7_observation_started
                    THEN NULL
                ELSE COUNTIF(
                    b.user_id IS NOT NULL
                    AND
                    NOT COALESCE(b.is_day_7_retained, FALSE)
                    AND b.viewed_product_detail = 1
                )
            END AS day_7_not_retained_value,
            NULL AS next_day_retained_duration_sum_sec,
            NULL AS day_7_retained_duration_sum_sec,
            NULL AS next_day_not_retained_duration_sum_sec,
            NULL AS day_7_not_retained_duration_sum_sec
        FROM group_keys_with_observation_status k
        LEFT JOIN cohort_user_feature_base b
            ON b.dt = k.dt
            AND b.user_group = k.user_group
        GROUP BY
            k.dt,
            k.user_group,
            k.is_next_day_observation_started,
            k.is_day_7_observation_started
    ),
    -- 汇总“创建模特”特征的四类 cohort 值。
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
        LEFT JOIN cohort_user_feature_base b
            ON b.dt = k.dt
            AND b.user_group = k.user_group
        GROUP BY
            k.dt,
            k.user_group,
            k.is_next_day_observation_started,
            k.is_day_7_observation_started
    ),
    -- 汇总“搜索”特征的四类 cohort 值。
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
        LEFT JOIN cohort_user_feature_base b
            ON b.dt = k.dt
            AND b.user_group = k.user_group
        GROUP BY
            k.dt,
            k.user_group,
            k.is_next_day_observation_started,
            k.is_day_7_observation_started
    ),
    -- 合并所有特征行，供最终结果统一回写。
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
    ),
    -- 生成最终明细主键，保证每个日期和用户群都有完整 6 行。
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


def run_ads_daily_retention_user_feature(dates: list[date]):
    """
    留存用户特征数据（日×已登录用户分群×特征）:
    - user_group: 新用户 / 老用户 / 全量登录用户
    - feature_name: 日均停留时长、AI试穿、关注账号、浏览商详、创建模特、搜索
    - next_day_* / day_7_*: 以 dt 当日活跃登录用户为 cohort，在对应观察日开始后按实时回访进度对比留存与非留存用户特征
    - user_count 字段: 对应四类 cohort 的用户数，供展示层校验分母
    - duration_sum 字段: 仅“日均停留时长”特征行写入时长分子，供展示层做跨天加权平均

    使用多伦多时间（America/Toronto）。
    """
    query = build_ads_daily_retention_user_feature_query(dates)

    logging.info("开始处理: ads_daily_retention_user_feature")
    job = client.query(query)
    job.result()
    logging.info(f"ads_daily_retention_user_feature 刷新完成, 处理日期: {dates}")
