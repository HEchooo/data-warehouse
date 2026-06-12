-- 首页模块内容曝光 UV Top 30 临时取数
-- 类型：一次性临时取数，不新增 ADS 表，不修改或回填生产数据。
-- 时间：2026-06-01 ~ 2026-06-07，按 America/Toronto 切天。
-- 粒度：曝光日期 × 模块 × 内容 ID。
-- 排名：每天在 star / magazine / brand 内分别按登录用户曝光 UV 取前 30。
DECLARE toronto_tz STRING DEFAULT 'America/Toronto';

DECLARE start_dt DATE DEFAULT DATE '2026-06-01';

DECLARE end_dt DATE DEFAULT DATE '2026-06-07';

DECLARE top_n INT64 DEFAULT 30;

WITH
  -- 事件基础层：按物理分区粗筛后，还原事件真实时间并映射模块及对应 ID 原文。
  event_base AS (
    SELECT
      DATE(
        TIMESTAMP(
          DATETIME(logAt_timestamp, 'UTC'),
          COALESCE(NULLIF(prop_timezone, ''), 'UTC')
        ),
        toronto_tz
      ) AS exposure_dt,
      CASE event_name
        WHEN 'v_home_star' THEN 'star'
        WHEN 'v_home_magazine' THEN 'magazine'
        WHEN 'v_home_brand' THEN 'brand'
      END AS module,
      CASE event_name
        WHEN 'v_home_star' THEN args_star
        WHEN 'v_home_magazine' THEN args_magazine
        WHEN 'v_home_brand' THEN args_brand
      END AS raw_column_ids,
      CAST(prop_user_id AS STRING) AS user_id
    FROM
      `my-project-8584-jetonai.decom.dwd_event_log`
    WHERE
      DATE(logAt_timestamp) BETWEEN DATE_SUB(start_dt, INTERVAL 1 DAY) AND DATE_ADD(end_dt, INTERVAL 1 DAY)
      AND event_name IN ('v_home_star', 'v_home_magazine', 'v_home_brand')
      AND NULLIF(CAST(prop_user_id AS STRING), '') IS NOT NULL
  ),
  -- 目标日期层：按转换后的美东业务日期精确过滤。
  target_events AS (
    SELECT
      exposure_dt,
      module,
      raw_column_ids,
      user_id
    FROM
      event_base
    WHERE
      exposure_dt BETWEEN start_dt AND end_dt
  ),
  -- 内容 ID 拆分层：兼容 JSON 数组、JSON 字符串和普通单值，并过滤空值。
  content_exposure_events AS (
    SELECT
      e.exposure_dt,
      e.module,
      column_id,
      e.user_id
    FROM
      target_events e
      CROSS JOIN UNNEST (
        CASE
          WHEN e.raw_column_ids IS NULL
          OR TRIM(e.raw_column_ids) IN ('', 'null', '[]', '""') THEN []
          WHEN STARTS_WITH(TRIM(e.raw_column_ids), '[') THEN ARRAY(
            SELECT
              cleaned_column_id
            FROM
              (
                SELECT
                  NULLIF(
                    NULLIF(TRIM(TRIM(column_id_json), '"'), ''),
                    'null'
                  ) AS cleaned_column_id
                FROM
                  UNNEST (
                    IFNULL(JSON_EXTRACT_ARRAY(e.raw_column_ids, '$'), [])
                  ) AS column_id_json
              )
            WHERE
              cleaned_column_id IS NOT NULL
          )
          ELSE [
            NULLIF(
              NULLIF(
                COALESCE(
                  JSON_EXTRACT_SCALAR(e.raw_column_ids, '$'),
                  TRIM(TRIM(e.raw_column_ids), '"')
                ),
                ''
              ),
              'null'
            )
          ]
        END
      ) AS column_id
    WHERE
      column_id IS NOT NULL
  ),
  -- 内容曝光 UV 层：同一用户当天重复曝光同一内容只计一次。
  content_exposure_uv AS (
    SELECT
      exposure_dt,
      module,
      column_id,
      COUNT(DISTINCT user_id) AS exposure_uv
    FROM
      content_exposure_events
    GROUP BY
      exposure_dt,
      module,
      column_id
  ),
  -- 每日模块排名层：UV 相同时按内容 ID 升序，保证排名结果稳定。
  ranked_content AS (
    SELECT
      exposure_dt,
      module,
      column_id,
      exposure_uv,
      ROW_NUMBER() OVER (
        PARTITION BY
          exposure_dt,
          module
        ORDER BY
          exposure_uv DESC,
          column_id
      ) AS uv_rank
    FROM
      content_exposure_uv
  )
  -- 最终结果：每天每个模块保留曝光 UV 前 30 的内容。
SELECT
  r.exposure_dt,
  r.module,
  r.column_id,
  r.exposure_uv,
  r.uv_rank
FROM
  ranked_content r
WHERE
  r.uv_rank <= top_n
ORDER BY
  r.exposure_dt,
  r.module,
  r.uv_rank;
