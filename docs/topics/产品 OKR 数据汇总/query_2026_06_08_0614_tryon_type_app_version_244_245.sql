WITH
    -- 取数日期骨架：按 Toronto 业务日补齐 0608-0614，避免日均分母漂移。
    date_spine AS (
        SELECT
            dt
        FROM
            UNNEST (
                GENERATE_DATE_ARRAY(DATE '2026-06-08', DATE '2026-06-14')
            ) AS dt
    ),
    -- 活跃注册用户基础层：复用全量报表上游，排除 send_push / send_email 等被动触达事件。
    active_user_base AS (
        SELECT
            dt,
            CAST(prop_user_id AS STRING) AS user_id
        FROM
            `my-project-8584-jetonai.decom.dws_user_daily`
        WHERE
            dt BETWEEN DATE '2026-06-08' AND DATE  '2026-06-14'
            AND prop_user_id IS NOT NULL
            AND CAST(prop_user_id AS STRING) != ''
    ),
    -- 事件基础层：取当天命中过的完整 app_version。
    event_base AS (
        SELECT
            DATE(
                TIMESTAMP(
                    DATETIME(logAt_timestamp, 'UTC'),
                    COALESCE(NULLIF(prop_timezone, ''), 'UTC')
                ),
                'America/Toronto'
            ) AS dt,
            CAST(prop_user_id AS STRING) AS user_id,
            COALESCE(NULLIF(TRIM(prop_app_version), ''), 'unknown') AS app_version
        FROM
            `my-project-8584-jetonai.decom.dwd_event_log`
        WHERE
            DATE(logAt_timestamp) BETWEEN DATE '2026-06-07' AND DATE  '2026-06-15'
            AND DATE(
                TIMESTAMP(
                    DATETIME(logAt_timestamp, 'UTC'),
                    COALESCE(NULLIF(prop_timezone, ''), 'UTC')
                ),
                'America/Toronto'
            ) BETWEEN DATE '2026-06-08' AND DATE  '2026-06-14'
            AND prop_user_id IS NOT NULL
            AND CAST(prop_user_id AS STRING) != ''
    ),
    -- 目标版本活跃用户集合：先限定全量报表活跃注册用户，再用同日版本命中关系贴版本口径。
    target_version_active_users AS (
        SELECT DISTINCT
            a.dt,
            a.user_id
        FROM
            active_user_base a
            INNER JOIN event_base e ON a.dt = e.dt
            AND a.user_id = e.user_id
        WHERE
            e.app_version IN ('2.4.4', '2.4.5')
    ),
    -- 每日目标版本 DAU。
    dau_daily AS (
        SELECT
            dt,
            COUNT(DISTINCT user_id) AS dau_uv
        FROM
            target_version_active_users
        GROUP BY
            dt
    ),
    -- 服务端试穿任务基础层：整体试穿沿用 OKR 口径，包含 QUICK_TRY_ON / TRY_ON / OUTFIT。
    service_tryon_base AS (
        SELECT
            DATE(TIMESTAMP(gmt_create, 'UTC'), 'America/Toronto') AS dt,
            COALESCE(
                NULLIF(JSON_VALUE(request_param, '$.user_id'), ''),
                NULLIF(JSON_VALUE(request_param, '$.userId'), ''),
                NULLIF(CAST(user_id AS STRING), '')
            ) AS user_id,
            task_type
        FROM
            `my-project-8584-jetonai.v3_decom.common_ext_ai_task`
        WHERE
            DATE(TIMESTAMP(gmt_create, 'UTC'), 'America/Toronto') BETWEEN DATE '2026-06-08' AND DATE  '2026-06-14'
            AND task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')
    ),
    -- 目标版本试穿用户-日标签：按同日命中 2.4.4 或 2.4.5 的活跃用户约束服务端任务。
    tryon_user_flags AS (
        SELECT
            t.dt,
            t.user_id,
            MAX(IF(t.task_type = 'QUICK_TRY_ON', 1, 0)) AS used_quick,
            MAX(IF(t.task_type = 'TRY_ON', 1, 0)) AS used_plus
        FROM
            service_tryon_base t
            INNER JOIN target_version_active_users a ON t.dt = a.dt
            AND t.user_id = a.user_id
        WHERE
            t.user_id IS NOT NULL
            AND t.user_id != ''
        GROUP BY
            t.dt,
            t.user_id
    ),
    -- 目标版本试穿日聚合。
    tryon_daily AS (
        SELECT
            dt,
            COUNT(DISTINCT user_id) AS tryon_uv,
            COUNT(DISTINCT IF(used_quick = 1, user_id, NULL)) AS quick_tryon_uv,
            COUNT(DISTINCT IF(used_plus = 1, user_id, NULL)) AS plus_tryon_uv,
            COUNT(
                DISTINCT IF(
                    used_quick = 1
                    AND used_plus = 1,
                    user_id,
                    NULL
                )
            ) AS both_quick_plus_tryon_uv
        FROM
            tryon_user_flags
        GROUP BY
            dt
    ),
    -- 每日结果：补 0 后再聚合日均。
    daily_result AS (
        SELECT
            d.dt,
            COALESCE(da.dau_uv, 0) AS dau_uv,
            COALESCE(t.tryon_uv, 0) AS tryon_uv,
            COALESCE(t.quick_tryon_uv, 0) AS quick_tryon_uv,
            COALESCE(t.plus_tryon_uv, 0) AS plus_tryon_uv,
            COALESCE(t.both_quick_plus_tryon_uv, 0) AS both_quick_plus_tryon_uv
        FROM
            date_spine d
            LEFT JOIN dau_daily da USING (dt)
            LEFT JOIN tryon_daily t USING (dt)
    )
SELECT
    "0608-0614_2.4.4_2.4.5" AS date_range,
    COUNT(1) AS day_count,
    SAFE_DIVIDE(SUM(dau_uv), COUNT(1)) AS avg_dau_uv,
    SAFE_DIVIDE(SUM(tryon_uv), COUNT(1)) AS avg_tryon_uv,
    SAFE_DIVIDE(SUM(quick_tryon_uv), COUNT(1)) AS avg_quick_tryon_uv,
    SAFE_DIVIDE(SUM(plus_tryon_uv), COUNT(1)) AS avg_plus_tryon_uv,
    SAFE_DIVIDE(SUM(both_quick_plus_tryon_uv), COUNT(1)) AS avg_both_quick_plus_tryon_uv
FROM
    daily_result;
