WITH
    -- 取数周期配置：固定两段 7 天周期，按 Toronto 业务日统计。
    period_config AS (
        SELECT
            '0601-0607' AS date_range,
            DATE '2026-06-01' AS start_dt,
            DATE '2026-06-07' AS end_dt
        UNION ALL
        SELECT
            '0608-0614' AS date_range,
            DATE '2026-06-08' AS start_dt,
            DATE '2026-06-14' AS end_dt
    ),
    -- 日期骨架：补齐每个周期内所有自然日，避免无点击日导致日均分母漂移。
    date_spine AS (
        SELECT
            p.date_range,
            dt
        FROM
            period_config p
            CROSS JOIN UNNEST (GENERATE_DATE_ARRAY(p.start_dt, p.end_dt)) AS dt
    ),
    -- 客户端 tryon 点击事件基础层：按 Toronto 业务日切天，UV 使用已登录用户 prop_user_id。
    client_tryon_event_base AS (
        SELECT
            DATE(
                TIMESTAMP(
                    DATETIME(logAt_timestamp, 'UTC'),
                    COALESCE(NULLIF(prop_timezone, ''), 'UTC')
                ),
                'America/Toronto'
            ) AS dt,
            CAST(prop_user_id AS STRING) AS user_id
        FROM
            `my-project-8584-jetonai.decom.dwd_event_log`
        WHERE
            DATE(logAt_timestamp) BETWEEN DATE '2026-05-31' AND DATE  '2026-06-15'
            AND DATE(
                TIMESTAMP(
                    DATETIME(logAt_timestamp, 'UTC'),
                    COALESCE(NULLIF(prop_timezone, ''), 'UTC')
                ),
                'America/Toronto'
            ) BETWEEN DATE '2026-06-01' AND DATE  '2026-06-14'
            AND event_name = 'c_tryon'
            AND prop_user_id IS NOT NULL
            AND CAST(prop_user_id AS STRING) != ''
    ),
    -- 每日客户端 tryon 按钮点击 UV：同一用户同一天多次点击只计 1 个 UV。
    client_tryon_daily AS (
        SELECT
            dt,
            COUNT(DISTINCT user_id) AS client_tryon_click_uv
        FROM
            client_tryon_event_base
        GROUP BY
            dt
    ),
    -- 每日结果：按日期骨架补 0 后再进入周期聚合。
    daily_result AS (
        SELECT
            d.date_range,
            d.dt,
            COALESCE(t.client_tryon_click_uv, 0) AS client_tryon_click_uv
        FROM
            date_spine d
            LEFT JOIN client_tryon_daily t USING (dt)
    )
SELECT
    date_range,
    COUNT(1) AS day_count,
    SUM(client_tryon_click_uv) AS sum_client_tryon_click_uv,
    SAFE_DIVIDE(SUM(client_tryon_click_uv), COUNT(1)) AS avg_client_tryon_click_uv
FROM
    daily_result
GROUP BY
    date_range
ORDER BY
    date_range;
