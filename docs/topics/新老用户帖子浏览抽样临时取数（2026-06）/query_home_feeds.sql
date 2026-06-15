-- 新老用户首页 Feeds 帖子曝光抽样临时取数（2026-06）
-- 时间：2026-06-01 ~ 2026-06-07，按 America/Toronto 切天。
-- 抽样：每天分别稳定抽取 10 个新用户和 10 个老用户。
-- 粒度：曝光日期 × 用户分群 × 浏览用户 ID × 帖子 ID。
DECLARE toronto_tz STRING DEFAULT 'America/Toronto';

DECLARE shanghai_tz STRING DEFAULT 'Asia/Shanghai';

DECLARE start_dt DATE DEFAULT DATE '2026-06-01';

DECLARE end_dt DATE DEFAULT DATE '2026-06-07';

DECLARE sample_size INT64 DEFAULT 10;

DECLARE sample_salt STRING DEFAULT 'home-feeds-post-sample-20260601-20260607-v1';

WITH
  -- 帖子主数据层：同一 post_code 如有多条记录，保留最近更新且能映射到目标内容类型的一条。
  post_detail AS (
    SELECT
      CAST(post_code AS STRING) AS post_code,
      CAST(poster_user_id AS STRING) AS poster_user_id,
      CASE
        WHEN post_type IN (2, 4) THEN '明星'
        WHEN post_type = 5 THEN '杂志'
        WHEN post_type IN (6, 7) THEN '品牌'
      END AS content_type,
      CAST(creator AS STRING) AS creator,
      DATETIME(TIMESTAMP(gmt_create, 'UTC'), shanghai_tz) AS content_create_time_cn,
      DATETIME(TIMESTAMP(publish_time, 'UTC'), shanghai_tz) AS content_publish_time_cn,
      ARRAY(
        SELECT
          JSON_VALUE(asset, '$.assetCode')
        FROM
          UNNEST (
            IFNULL(
              JSON_QUERY_ARRAY(post_ext, '$.mainAssets'),
              ARRAY<JSON >[]
            )
          ) AS asset
        WITH
        OFFSET
          AS asset_offset
        WHERE
          NULLIF(JSON_VALUE(asset, '$.assetCode'), '') IS NOT NULL
        ORDER BY
          asset_offset
      ) AS asset_codes,
      ARRAY(
        SELECT
          COALESCE(JSON_VALUE(asset, '$.image.url'), '')
        FROM
          UNNEST (
            IFNULL(
              JSON_QUERY_ARRAY(post_ext, '$.mainAssets'),
              ARRAY<JSON >[]
            )
          ) AS asset
        WITH
        OFFSET
          AS asset_offset
        WHERE
          NULLIF(JSON_VALUE(asset, '$.assetCode'), '') IS NOT NULL
        ORDER BY
          asset_offset
      ) AS post_image_urls
    FROM
      `my-project-8584-jetonai.v3_decom.community_post`
    WHERE
      post_code IS NOT NULL
      AND post_type IN (2, 4, 5, 6, 7)
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY
          CAST(post_code AS STRING)
        ORDER BY
          TIMESTAMP(COALESCE(gmt_update, gmt_create), 'UTC') DESC,
          id DESC
      ) = 1
  ),
  -- 事件基础层：利用物理分区缩小扫描范围，再按埋点上报时区还原真实时间并切美东业务日。
  event_base AS (
    SELECT
      DATE(
        TIMESTAMP(
          DATETIME(logAt_timestamp, 'UTC'),
          COALESCE(NULLIF(prop_timezone, ''), 'UTC')
        ),
        toronto_tz
      ) AS browse_dt,
      TIMESTAMP(
        DATETIME(logAt_timestamp, 'UTC'),
        COALESCE(NULLIF(prop_timezone, ''), 'UTC')
      ) AS browse_ts,
      CAST(prop_user_id AS STRING) AS browse_user_id,
      CAST(post_code AS STRING) AS post_code,
      COALESCE(NULLIF(raw_event_id, ''), NULLIF(hash_id, '')) AS browse_event_id
    FROM
      `my-project-8584-jetonai.decom.dwd_event_log`
    WHERE
      DATE(logAt_timestamp) BETWEEN DATE_SUB(start_dt, INTERVAL 1 DAY) AND DATE_ADD(end_dt, INTERVAL 1 DAY)
      AND event_name = 'v_home_feeds'
      AND NULLIF(CAST(prop_user_id AS STRING), '') IS NOT NULL
      AND NULLIF(CAST(post_code AS STRING), '') IS NOT NULL
  ),
  -- 目标事件层：只保留目标美东日期和能映射到明星、杂志或品牌的帖子曝光。
  target_events AS (
    SELECT
      e.browse_dt,
      e.browse_ts,
      e.browse_user_id,
      e.post_code,
      e.browse_event_id
    FROM
      event_base e
      INNER JOIN post_detail p ON p.post_code = e.post_code
    WHERE
      e.browse_dt BETWEEN start_dt AND end_dt
  ),
  -- 用户分群层：新用户为首活日等于曝光日，老用户为首活日早于曝光日。
  grouped_users AS (
    SELECT DISTINCT
      e.browse_dt,
      e.browse_user_id,
      CASE
        WHEN f.first_active_date = e.browse_dt THEN '新用户'
        WHEN f.first_active_date < e.browse_dt THEN '老用户'
      END AS user_group
    FROM
      target_events e
      INNER JOIN `my-project-8584-jetonai.decom.dim_user_first_active` f ON CAST(f.prop_user_id AS STRING) = e.browse_user_id
    WHERE
      f.first_active_date <= e.browse_dt
  ),
  -- 用户抽样排序层：每天在新用户和老用户内分别生成稳定的抽样顺序。
  ranked_users AS (
    SELECT
      browse_dt,
      user_group,
      browse_user_id,
      ROW_NUMBER() OVER (
        PARTITION BY
          browse_dt,
          user_group
        ORDER BY
          FARM_FINGERPRINT(
            CONCAT(
              CAST(browse_dt AS STRING),
              '|',
              user_group,
              '|',
              browse_user_id,
              '|',
              sample_salt
            )
          ),
          browse_user_id
      ) AS sample_rank
    FROM
      grouped_users
    WHERE
      user_group IS NOT NULL
  ),
  -- 用户抽样结果层：每天分别保留排序前 10 位的新用户和老用户。
  sampled_users AS (
    SELECT
      browse_dt,
      user_group,
      browse_user_id
    FROM
      ranked_users
    WHERE
      sample_rank <= sample_size
  ),
  -- 样本曝光明细：同一天同一用户同一帖子聚合，保留首次曝光时间和去重曝光次数。
  sampled_browse_detail AS (
    SELECT
      e.browse_dt,
      s.user_group,
      e.browse_user_id,
      e.post_code,
      MIN(e.browse_ts) AS first_browse_ts,
      COUNT(DISTINCT e.browse_event_id) AS browse_count
    FROM
      target_events e
      INNER JOIN sampled_users s ON s.browse_dt = e.browse_dt
      AND s.browse_user_id = e.browse_user_id
    GROUP BY
      e.browse_dt,
      s.user_group,
      e.browse_user_id,
      e.post_code
  )
  -- 最终结果：字段名与原帖子详情浏览抽样结果保持一致。
SELECT
  b.browse_dt,
  b.user_group,
  b.browse_user_id,
  b.post_code,
  p.poster_user_id,
  p.asset_codes,
  p.content_type,
  p.creator,
  p.content_create_time_cn,
  p.content_publish_time_cn,
  DATETIME(b.first_browse_ts, toronto_tz) AS first_browse_time_et,
  b.browse_count,
  p.post_image_urls
FROM
  sampled_browse_detail b
  INNER JOIN post_detail p ON p.post_code = b.post_code
ORDER BY
  b.browse_dt,
  b.user_group,
  b.browse_user_id,
  b.first_browse_ts,
  b.post_code;
