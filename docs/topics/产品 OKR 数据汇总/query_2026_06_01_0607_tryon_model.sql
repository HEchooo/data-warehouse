WITH
    -- 取数日期骨架：按 Toronto 业务日补齐 0601-0607，避免日均分母漂移。
    date_spine AS (
        SELECT
            dt
        FROM
            UNNEST (
                GENERATE_DATE_ARRAY(DATE '2026-06-01', DATE '2026-06-07')
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
            dt BETWEEN DATE '2026-06-01' AND DATE  '2026-06-07'
            AND prop_user_id IS NOT NULL
            AND CAST(prop_user_id AS STRING) != ''
    ),
    -- 每日 DAU：与【AI-Fashion】全量报表“到访注册用户”同源。
    dau_daily AS (
        SELECT
            dt,
            COUNT(DISTINCT user_id) AS dau_uv
        FROM
            active_user_base
        GROUP BY
            dt
    ),
    -- 模特维表：按 model_id 去重，保留最新 group_id。
    model_dim AS (
        SELECT
            CAST(model_id AS STRING) AS model_id,
            group_id
        FROM
            `my-project-8584-jetonai.v3_decom.user_fashion_model`
        WHERE
            model_id IS NOT NULL
            AND CAST(model_id AS STRING) != ''
        QUALIFY
            ROW_NUMBER() OVER (
                PARTITION BY
                    CAST(model_id AS STRING)
                ORDER BY
                    gmt_create DESC
            ) = 1
    ),
    -- 服务端试穿任务基础层：整体试穿沿用 OKR 口径，包含 QUICK_TRY_ON / TRY_ON / OUTFIT。
    service_tryon_base AS (
        SELECT
            DATE(TIMESTAMP(t.gmt_create, 'UTC'), 'America/Toronto') AS dt,
            COALESCE(
                NULLIF(JSON_VALUE(t.request_param, '$.user_id'), ''),
                NULLIF(JSON_VALUE(t.request_param, '$.userId'), ''),
                NULLIF(CAST(t.user_id AS STRING), '')
            ) AS user_id,
            m.group_id AS model_group_id
        FROM
            `my-project-8584-jetonai.v3_decom.common_ext_ai_task` t
            LEFT JOIN model_dim m ON CAST(t.custom_model_id AS STRING) = m.model_id
        WHERE
            DATE(TIMESTAMP(t.gmt_create, 'UTC'), 'America/Toronto') BETWEEN DATE '2026-06-01' AND DATE  '2026-06-07'
            AND t.task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')
    ),
    -- 试穿用户-日标签：用于拆自定义模特、系统模特以及两者都用。
    tryon_user_flags AS (
        SELECT
            dt,
            user_id,
            MAX(IF(model_group_id = 1, 1, 0)) AS used_custom_model,
            MAX(IF(model_group_id = 0, 1, 0)) AS used_system_model
        FROM
            service_tryon_base
        WHERE
            user_id IS NOT NULL
            AND user_id != ''
        GROUP BY
            dt,
            user_id
    ),
    -- 试穿日聚合。
    tryon_daily AS (
        SELECT
            dt,
            COUNT(DISTINCT user_id) AS tryon_uv,
            COUNT(DISTINCT IF(used_custom_model = 1, user_id, NULL)) AS custom_model_tryon_uv,
            COUNT(DISTINCT IF(used_system_model = 1, user_id, NULL)) AS system_model_tryon_uv,
            COUNT(
                DISTINCT IF(
                    used_custom_model = 1
                    AND used_system_model = 1,
                    user_id,
                    NULL
                )
            ) AS both_custom_system_model_tryon_uv
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
            COALESCE(t.custom_model_tryon_uv, 0) AS custom_model_tryon_uv,
            COALESCE(t.system_model_tryon_uv, 0) AS system_model_tryon_uv,
            COALESCE(t.both_custom_system_model_tryon_uv, 0) AS both_custom_system_model_tryon_uv
        FROM
            date_spine d
            LEFT JOIN dau_daily da USING (dt)
            LEFT JOIN tryon_daily t USING (dt)
    )
SELECT
    "0601-0607" AS date_range,
    COUNT(1) AS day_count,
    SAFE_DIVIDE(SUM(dau_uv), COUNT(1)) AS avg_dau_uv,
    SAFE_DIVIDE(SUM(tryon_uv), COUNT(1)) AS avg_tryon_uv,
    SAFE_DIVIDE(SUM(custom_model_tryon_uv), COUNT(1)) AS avg_custom_model_tryon_uv,
    SAFE_DIVIDE(SUM(system_model_tryon_uv), COUNT(1)) AS avg_system_model_tryon_uv,
    SAFE_DIVIDE(SUM(both_custom_system_model_tryon_uv), COUNT(1)) AS avg_both_custom_system_model_tryon_uv
FROM
    daily_result;
