-- 品牌帖子点击率与新用户首次点击内容占比临时取数（2026-08）
-- 需求 3：每日 x 品牌维度商品点击率，仅保留有商品点击数据的品牌 x 日期
-- 口径：
--   - 品牌范围：decom.dim_column.module = 'brand'（column_id -> column_name，即品牌名称）
--   - 商品点击 UV：事件 c_product，args.asset_code 关联 decom.dim_asset（module = 'brand'）得到 column_id，
--     直接复用已下沉的 decom.dws_asset_user_daily（粒度 dt x asset_code x user_id，product_click_pv > 0），
--     不重新解析 dwd_event_log 的 args.asset_code
--   - 帖子曝光 UV：event_name 以 v_ 开头，args_post（dwd_event_log.post_code）关联到该品牌（poster_user_id）名下的帖子
--     （与 query_brand_post_ctr.sql 的曝光口径一致），仅统计已登录用户
--   - 商品点击率 = 商品点击 UV / 帖子曝光 UV，按 dt x 品牌计算，不再对整段时间汇总
--   - 仅保留当天商品点击 UV > 0 的 dt x 品牌组合；当天商品点击 UV = 0 的组合不出现在结果中

DECLARE toronto_tz STRING DEFAULT 'America/Toronto';
DECLARE bucket_min_dt DATE DEFAULT DATE '2026-03-01';
DECLARE bucket_max_dt DATE DEFAULT CURRENT_DATE(toronto_tz);

WITH
-- 品牌范围：dim_column 里 module = 'brand' 的专栏。
brand_column AS (
  SELECT
    column_id,
    column_name AS brand_name
  FROM `my-project-8584-jetonai.decom.dim_column`
  WHERE module = 'brand'
    AND column_id IS NOT NULL
),
-- 品牌帖子：仅取属于品牌专栏的帖子，用于曝光归因。
brand_post AS (
  SELECT
    post_code,
    poster_user_id AS column_id
  FROM `my-project-8584-jetonai.v3_decom.community_post`
  WHERE post_code IS NOT NULL
    AND poster_user_id IN (SELECT column_id FROM brand_column)
),
-- 帖子曝光 UV：event_name 以 v_ 开头，按 dt x 品牌去重用户，口径与 query_brand_post_ctr.sql 一致。
exposure_by_brand_daily AS (
  SELECT
    DATE(
      TIMESTAMP(
        DATETIME(e.logAt_timestamp, 'UTC'),
        COALESCE(NULLIF(e.prop_timezone, ''), 'UTC')
      ),
      toronto_tz
    ) AS dt,
    p.column_id,
    COUNT(DISTINCT NULLIF(e.prop_user_id, '')) AS exposure_uv
  FROM `my-project-8584-jetonai.decom.dwd_event_log` e
  INNER JOIN brand_post p
    ON p.post_code = e.post_code
  WHERE DATE(e.logAt_timestamp) BETWEEN bucket_min_dt AND DATE_ADD(bucket_max_dt, INTERVAL 1 DAY)
    AND STARTS_WITH(e.event_name, 'v_')
    AND e.prop_user_id IS NOT NULL
    AND e.prop_user_id != ''
  GROUP BY dt, p.column_id
  HAVING dt BETWEEN bucket_min_dt AND bucket_max_dt
),
-- 品牌素材：dim_asset 里 module = 'brand' 的素材，用于商品点击归因。
brand_asset AS (
  SELECT
    asset_code,
    column_id
  FROM `my-project-8584-jetonai.decom.dim_asset`
  WHERE module = 'brand'
    AND column_id IS NOT NULL
),
-- 商品点击 UV：复用 dws_asset_user_daily 的 product_click_pv，按 dt x 品牌去重用户。
click_by_brand_daily AS (
  SELECT
    d.dt,
    a.column_id,
    COUNT(DISTINCT d.user_id) AS click_uv
  FROM `my-project-8584-jetonai.decom.dws_asset_user_daily` d
  INNER JOIN brand_asset a
    ON a.asset_code = d.asset_code
  WHERE d.dt BETWEEN bucket_min_dt AND bucket_max_dt
    AND d.product_click_pv > 0
  GROUP BY d.dt, a.column_id
)
SELECT
  c.dt,
  b.brand_name,
  SAFE_DIVIDE(c.click_uv, NULLIF(COALESCE(e.exposure_uv, 0), 0)) AS product_click_rate
FROM click_by_brand_daily c
JOIN brand_column b
  ON b.column_id = c.column_id
LEFT JOIN exposure_by_brand_daily e
  ON e.dt = c.dt
  AND e.column_id = c.column_id
ORDER BY c.dt, product_click_rate DESC;
