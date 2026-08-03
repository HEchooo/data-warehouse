-- 品牌帖子点击率与新用户首次点击内容占比临时取数（2026-08）
-- 需求 2：每日新用户「首活当天」第一条 c_home_feeds 点击到的内容类型占比
-- 口径：
--   - 新用户：decom.dim_user_first_active.first_active_date
--   - 首次点击：新用户首活当天（America/Toronto）产生的第一条 c_home_feeds 事件（按事件时间最早，
--     同一时刻多条按 raw_event_id/hash_id 兜底排序取一条），不看首活日之后的点击
--   - 内容类型：c_home_feeds 命中的 args_post 关联到 v3_decom.community_post.post_type，
--     按 docs/rules/post-mapping.md 映射：2/4=明星、5=杂志、6/7=品牌，其余（1/8/9/10 等未在规则文档定义的
--     post_type，以及帖子已被删除等无法关联的情况）归入「其他」
--   - 占比分母：当天「首活当天有 c_home_feeds 点击」的新用户数，不含当天完全没有点击的新用户

DECLARE toronto_tz STRING DEFAULT 'America/Toronto';
DECLARE bucket_min_dt DATE DEFAULT DATE_SUB(CURRENT_DATE(toronto_tz), INTERVAL 30 DAY);
DECLARE bucket_max_dt DATE DEFAULT DATE_SUB(CURRENT_DATE(toronto_tz), INTERVAL 1 DAY);

WITH
-- 新用户首活维表：限定在取数窗口内首活的用户。
new_users AS (
  SELECT
    CAST(prop_user_id AS STRING) AS user_id,
    first_active_date
  FROM `my-project-8584-jetonai.decom.dim_user_first_active`
  WHERE first_active_date BETWEEN bucket_min_dt AND bucket_max_dt
    AND prop_user_id IS NOT NULL
    AND CAST(prop_user_id AS STRING) != ''
),
-- c_home_feeds 点击明细：只保留带 args_post（post_code）的点击行。
home_feeds_click AS (
  SELECT
    NULLIF(prop_user_id, '') AS user_id,
    post_code,
    TIMESTAMP(
      DATETIME(logAt_timestamp, 'UTC'),
      COALESCE(NULLIF(prop_timezone, ''), 'UTC')
    ) AS event_ts,
    raw_event_id,
    hash_id
  FROM `my-project-8584-jetonai.decom.dwd_event_log`
  WHERE event_name = 'c_home_feeds'
    AND post_code IS NOT NULL
    AND prop_user_id IS NOT NULL
    AND prop_user_id != ''
    AND DATE(logAt_timestamp) BETWEEN DATE_SUB(bucket_min_dt, INTERVAL 1 DAY)
        AND DATE_ADD(bucket_max_dt, INTERVAL 1 DAY)
),
-- 只保留新用户首活当天（Toronto 切天）产生的点击。
new_user_first_active_day_click AS (
  SELECT
    u.user_id,
    u.first_active_date,
    c.post_code,
    c.event_ts,
    c.raw_event_id,
    c.hash_id
  FROM new_users u
  INNER JOIN home_feeds_click c
    ON c.user_id = u.user_id
    AND DATE(c.event_ts, toronto_tz) = u.first_active_date
),
-- 每个新用户首活当天的第一条 c_home_feeds：按事件时间最早，兜底按 raw_event_id/hash_id 排序取一条。
first_click_per_user AS (
  SELECT
    user_id,
    first_active_date,
    post_code
  FROM new_user_first_active_day_click
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY user_id, first_active_date
    ORDER BY event_ts ASC, raw_event_id ASC, hash_id ASC
  ) = 1
),
-- 首次点击帖子映射内容类型：post_type 2/4=明星、5=杂志、6/7=品牌，其余归为其他。
first_click_content AS (
  SELECT
    f.user_id,
    f.first_active_date,
    CASE
      WHEN p.post_type IN (2, 4) THEN '明星'
      WHEN p.post_type = 5 THEN '杂志'
      WHEN p.post_type IN (6, 7) THEN '品牌'
      ELSE '其他'
    END AS content_type
  FROM first_click_per_user f
  LEFT JOIN `my-project-8584-jetonai.v3_decom.community_post` p
    ON p.post_code = f.post_code
),
-- 每日汇总：首活当天有点击的新用户数（占比分母）、各内容类型点击用户数（占比分子）。
daily_first_click_total AS (
  SELECT
    first_active_date AS dt,
    COUNT(DISTINCT user_id) AS first_click_new_users
  FROM first_click_content
  GROUP BY dt
),
daily_content_type AS (
  SELECT
    first_active_date AS dt,
    content_type,
    COUNT(DISTINCT user_id) AS content_type_users
  FROM first_click_content
  GROUP BY dt, content_type
)
SELECT
  ft.dt,
  ct.content_type,
  ct.content_type_users,
  SAFE_DIVIDE(ct.content_type_users, NULLIF(ft.first_click_new_users, 0)) AS content_type_share
FROM daily_first_click_total ft
LEFT JOIN daily_content_type ct
  ON ct.dt = ft.dt
ORDER BY ft.dt, ct.content_type;
