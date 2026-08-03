-- 品牌帖子点击率与新用户首次点击内容占比临时取数（2026-08）
-- 需求 1：每日 × 帖子 × 品牌 的曝光 UV / 点击 UV / CTR / 点赞 UV
-- 口径：
--   - 帖子归属品牌：v3_decom.community_post.post_type IN (6, 7)（FASHION_LOOK / FASHION_PRODUCT，见 docs/rules/post-mapping.md）
--   - 品牌名称：community_post.poster_user_id -> v3_decom.kol_rel.nickname
--   - 曝光：event_name 以 v_ 开头，且 args_post（dwd_event_log.post_code）关联到该品牌帖子
--   - 点击：event_name 以 c_ 开头，且 args_post 关联到该品牌帖子；排除 c_email、c_push 这类被动通知类事件
--   - 点赞：服务端表 v3_decom.community_post_like，按 gmt_create 切 America/Toronto 天
--   - 曝光 UV / 点击 UV 仅统计已登录用户（prop_user_id 非空），按 COUNT(DISTINCT prop_user_id) 统计

DECLARE toronto_tz STRING DEFAULT 'America/Toronto';
DECLARE bucket_min_dt DATE DEFAULT DATE_SUB(CURRENT_DATE(toronto_tz), INTERVAL 30 DAY);
DECLARE bucket_max_dt DATE DEFAULT DATE_SUB(CURRENT_DATE(toronto_tz), INTERVAL 1 DAY);

WITH
  -- 品牌帖子：post_type 直接判定为品牌内容的帖子。
  brand_post AS (
    SELECT
      id AS post_id,
      post_code,
      poster_user_id
    FROM
      `my-project-8584-jetonai.v3_decom.community_post`
    WHERE
      post_type IN (6, 7)
      AND post_code IS NOT NULL
  ),
  -- 品牌名称：帖子发布者对应的 kol_rel 昵称。
  brand_name AS (
    SELECT
      user_id,
      nickname AS brand_name
    FROM
      `my-project-8584-jetonai.v3_decom.kol_rel`
    WHERE
      user_id IS NOT NULL
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY
          user_id
        ORDER BY
          IF(status = 0, 0, 1)
      ) = 1
  ),
  -- 品牌帖子基础信息：帖子 + 品牌名称一一对应。
  brand_post_dim AS (
    SELECT
      p.post_id,
      p.post_code,
      n.brand_name
    FROM
      brand_post p
      LEFT JOIN brand_name n ON n.user_id = p.poster_user_id
  ),
  -- 事件基础层：统一按 Toronto 切天，只保留携带 args_post 且命中品牌帖子的事件行。
  event_base AS (
    SELECT
      DATE(
        TIMESTAMP(
          DATETIME(e.logAt_timestamp, 'UTC'),
          COALESCE(NULLIF(e.prop_timezone, ''), 'UTC')
        ),
        toronto_tz
      ) AS dt,
      e.event_name,
      NULLIF(e.prop_user_id, '') AS user_id,
      b.post_code
    FROM
      `my-project-8584-jetonai.decom.dwd_event_log` e
      INNER JOIN brand_post_dim b ON b.post_code = e.post_code
    WHERE
      DATE(e.logAt_timestamp) BETWEEN DATE_SUB(bucket_min_dt, INTERVAL 1 DAY) AND DATE_ADD(bucket_max_dt, INTERVAL 1 DAY)
  ),
  -- 曝光明细：event_name 以 v_ 开头的已登录用户事件。
  exposure_daily AS (
    SELECT
      dt,
      post_code,
      COUNT(DISTINCT user_id) AS exposure_uv
    FROM
      event_base
    WHERE
      dt BETWEEN bucket_min_dt AND bucket_max_dt
      AND STARTS_WITH(event_name, 'v_')
      AND user_id IS NOT NULL
    GROUP BY
      dt,
      post_code
  ),
  -- 点击明细：event_name 以 c_ 开头的已登录用户事件，排除被动通知类事件。
  click_daily AS (
    SELECT
      dt,
      post_code,
      COUNT(DISTINCT user_id) AS click_uv
    FROM
      event_base
    WHERE
      dt BETWEEN bucket_min_dt AND bucket_max_dt
      AND STARTS_WITH(event_name, 'c_')
      AND event_name NOT IN ('c_email', 'c_push')
      AND user_id IS NOT NULL
    GROUP BY
      dt,
      post_code
  ),
  -- 服务端点赞明细：按创建时间切 Toronto 天，映射回品牌帖子。
  like_daily AS (
    SELECT
      DATE(TIMESTAMP(l.gmt_create, 'UTC'), toronto_tz) AS dt,
      b.post_code,
      COUNT(DISTINCT l.user_id) AS like_uv
    FROM
      `my-project-8584-jetonai.v3_decom.community_post_like` l
      INNER JOIN brand_post_dim b ON b.post_id = l.post_id
    WHERE
      DATE(TIMESTAMP(l.gmt_create, 'UTC'), toronto_tz) BETWEEN bucket_min_dt AND bucket_max_dt
      AND l.user_id IS NOT NULL
      AND l.user_id != ''
    GROUP BY
      dt,
      post_code
  ),
  -- 指标主键骨架：合并所有产生过曝光 / 点击 / 点赞的 日期 × 帖子。
  keys AS (
    SELECT
      dt,
      post_code
    FROM
      exposure_daily
    UNION DISTINCT
    SELECT
      dt,
      post_code
    FROM
      click_daily
    UNION DISTINCT
    SELECT
      dt,
      post_code
    FROM
      like_daily
  )
SELECT
  k.dt,
  b.post_code,
  b.brand_name,
  COALESCE(e.exposure_uv, 0) AS exposure_uv,
  COALESCE(c.click_uv, 0) AS click_uv,
  IFNULL(
    SAFE_DIVIDE(
      COALESCE(c.click_uv, 0),
      NULLIF(COALESCE(e.exposure_uv, 0), 0)
    ),
    0
  ) AS ctr,
  COALESCE(l.like_uv, 0) AS like_uv
FROM
  keys k
  JOIN brand_post_dim b ON b.post_code = k.post_code
  LEFT JOIN exposure_daily e ON e.dt = k.dt
  AND e.post_code = k.post_code
  LEFT JOIN click_daily c ON c.dt = k.dt
  AND c.post_code = k.post_code
  LEFT JOIN like_daily l ON l.dt = k.dt
  AND l.post_code = k.post_code
ORDER BY
  k.dt,
  b.brand_name,
  b.post_code;
