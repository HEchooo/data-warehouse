-- 试穿与商品行为临时取数（2026-05）
-- 结果包含 4 个结果集：
-- 1) 主指标（1~18）
-- 2) 指标 10：人均试穿次数分布（试穿总次数 / 试穿UV）
-- 3) 指标 19：人均点击次数分布
-- 4) 指标 11/12 事件形状探查（c*/v* + product_code 非空）

DECLARE toronto_tz STRING DEFAULT 'America/Toronto';
DECLARE current_start DATE DEFAULT DATE '2026-05-01';
DECLARE current_end DATE DEFAULT DATE '2026-05-25';
DECLARE last_month_start DATE DEFAULT DATE '2026-04-01';
DECLARE last_month_end DATE DEFAULT DATE '2026-04-30';

-- 日期骨架：固定区间天数，保证“日均 = SUM(day_value) / 固定天数”。
CREATE TEMP TABLE period_calendar AS
SELECT 'current' AS period_tag, dt
FROM UNNEST(GENERATE_DATE_ARRAY(current_start, current_end)) AS dt
UNION ALL
SELECT 'last_month' AS period_tag, dt
FROM UNNEST(GENERATE_DATE_ARRAY(last_month_start, last_month_end)) AS dt;

-- 新老用户首活维表：后续用于指标 1/2 分群。
CREATE TEMP TABLE user_first_active AS
SELECT
  CAST(prop_user_id AS STRING) AS user_id,
  first_active_date
FROM `my-project-8584-jetonai.decom.dim_user_first_active`
WHERE prop_user_id IS NOT NULL
  AND CAST(prop_user_id AS STRING) != '';

-- 事件基础层：统一按 Toronto 切天，统一 user_id，保留计算所需字段。
CREATE TEMP TABLE dwd_base AS
SELECT
  DATE(
    TIMESTAMP(
      DATETIME(logAt_timestamp, 'UTC'),
      COALESCE(NULLIF(prop_timezone, ''), 'UTC')
    ),
    toronto_tz
  ) AS dt,
  event_name,
  CAST(prop_user_id AS STRING) AS user_id,
  args_page_key,
  NULLIF(product_code, '') AS product_code,
  raw_event_id,
  hash_id
FROM `my-project-8584-jetonai.decom.dwd_event_log`
WHERE DATE(logAt_timestamp) BETWEEN DATE_SUB(last_month_start, INTERVAL 1 DAY)
    AND DATE_ADD(current_end, INTERVAL 1 DAY)
  AND prop_user_id IS NOT NULL
  AND CAST(prop_user_id AS STRING) != '';

-- styleme 服务端行为：按 gmt_create 切 Toronto 日。
CREATE TEMP TABLE styleme_daily AS
WITH styleme_raw AS (
  SELECT
    CASE
      WHEN DATE(TIMESTAMP(gmt_create, 'UTC'), toronto_tz) BETWEEN current_start AND current_end
        THEN 'current'
      WHEN DATE(TIMESTAMP(gmt_create, 'UTC'), toronto_tz) BETWEEN last_month_start AND last_month_end
        THEN 'last_month'
      ELSE NULL
    END AS period_tag,
    DATE(TIMESTAMP(gmt_create, 'UTC'), toronto_tz) AS dt,
    CAST(user_id AS STRING) AS user_id
  FROM `my-project-8584-jetonai.v3_decom.common_ext_ai_task`
  WHERE DATE(TIMESTAMP(gmt_create, 'UTC'), toronto_tz) BETWEEN last_month_start AND current_end
    AND task_scene = 'STYLE_ME'
    AND user_id IS NOT NULL
    AND CAST(user_id AS STRING) != ''
)
SELECT
  period_tag,
  dt,
  COUNT(DISTINCT user_id) AS styleme_uv
FROM styleme_raw
WHERE period_tag IS NOT NULL
GROUP BY period_tag, dt;

-- 每日主事实：先按日聚合，再补全日期骨架。
CREATE TEMP TABLE period_daily_metrics AS
WITH
-- 每日活跃用户：任意事件 UV（DAU）。
active_users_daily AS (
  SELECT
    dt,
    COUNT(DISTINCT user_id) AS dau_uv_total
  FROM dwd_base
  GROUP BY dt
),
-- 每日试穿用户：c_tryon UV（给 1/2 分群复用）。
tryon_users_daily AS (
  SELECT
    dt,
    user_id
  FROM dwd_base
  WHERE event_name = 'c_tryon'
  GROUP BY dt, user_id
),
-- 新老用户分群日事实：dau 与 tryon_uv 同时输出。
user_group_daily AS (
  SELECT
    a.dt,
    COUNT(DISTINCT IF(f.first_active_date = a.dt, a.user_id, NULL)) AS dau_new_uv,
    COUNT(DISTINCT IF(f.first_active_date < a.dt, a.user_id, NULL)) AS dau_old_uv,
    COUNT(DISTINCT IF(f.first_active_date = a.dt AND t.user_id IS NOT NULL, a.user_id, NULL)) AS tryon_new_uv,
    COUNT(DISTINCT IF(f.first_active_date < a.dt AND t.user_id IS NOT NULL, a.user_id, NULL)) AS tryon_old_uv
  FROM (
    SELECT DISTINCT dt, user_id
    FROM dwd_base
  ) a
  LEFT JOIN user_first_active f
    ON f.user_id = a.user_id
  LEFT JOIN tryon_users_daily t
    ON t.dt = a.dt
    AND t.user_id = a.user_id
  GROUP BY a.dt
),
-- 每日事件指标：主指标分子分母、PV 分母一次性输出。
event_metric_daily AS (
  SELECT
    dt,
    COUNT(DISTINCT IF(event_name = 'c_tryon' AND args_page_key = 'home_feeds', user_id, NULL)) AS home_tryon_uv,
    COUNT(DISTINCT IF(event_name = 'v_home_feeds', user_id, NULL)) AS home_feeds_uv,
    COUNT(DISTINCT IF(event_name = 'c_tryon' AND args_page_key NOT IN ('star', 'magazine', 'brand', 'kol'), user_id, NULL)) AS post_tryon_uv,
    COUNT(DISTINCT IF(event_name IN (
      'v_star_post_feeds',
      'v_star_post_detail',
      'v_magazine_post_detail',
      'v_magazine_post_feeds',
      'v_brand_post_feeds',
      'v_brand_post_detail',
      'v_kol_post_feeds',
      'v_kol_post_detail'
    ), user_id, NULL)) AS post_scene_exposure_uv,
    COUNT(DISTINCT IF(event_name = 'c_tryon' AND args_page_key = 'product_detail', user_id, NULL)) AS product_detail_tryon_uv,
    COUNT(DISTINCT IF(event_name = 'v_product_detail', user_id, NULL)) AS product_detail_uv,
    COUNT(DISTINCT IF(STARTS_WITH(event_name, 'c') AND product_code IS NOT NULL, user_id, NULL)) AS product_click_uv,
    COUNT(DISTINCT IF(STARTS_WITH(event_name, 'v') AND product_code IS NOT NULL, user_id, NULL)) AS product_exposure_uv,
    COUNT(DISTINCT IF(event_name = 'c_home_feeds' AND product_code IS NOT NULL, user_id, NULL)) AS home_product_click_uv,
    COUNT(DISTINCT IF(event_name = 'v_home_feeds' AND product_code IS NOT NULL, user_id, NULL)) AS home_product_exposure_uv,
    COUNT(DISTINCT IF(event_name = 'c_tryon_result_list', user_id, NULL)) AS tryon_result_click_uv,
    COUNT(DISTINCT IF(event_name IN ('v_tryon_result_detail', 'v_tryon_result_list'), user_id, NULL)) AS tryon_result_exposure_uv,
    COUNT(DISTINCT IF(event_name = 'c_shop_feeds', user_id, NULL)) AS shop_click_uv,
    COUNT(DISTINCT IF(event_name = 'v_shop_feeds', user_id, NULL)) AS shop_exposure_uv,
    COUNT(DISTINCT IF(event_name = 'c_tryon', user_id, NULL)) AS tryon_uv_total,
    COUNT(DISTINCT IF(event_name = 'c_tryon', COALESCE(raw_event_id, hash_id), NULL)) AS tryon_pv,
    COUNT(DISTINCT IF(STARTS_WITH(event_name, 'c') AND product_code IS NOT NULL, raw_event_id, NULL)) AS product_click_pv_raw
  FROM dwd_base
  GROUP BY dt
)
SELECT
  c.period_tag,
  c.dt,
  COALESCE(a.dau_uv_total, 0) AS dau_uv_total,
  COALESCE(g.dau_new_uv, 0) AS dau_new_uv,
  COALESCE(g.dau_old_uv, 0) AS dau_old_uv,
  COALESCE(g.tryon_new_uv, 0) AS tryon_new_uv,
  COALESCE(g.tryon_old_uv, 0) AS tryon_old_uv,
  COALESCE(e.home_tryon_uv, 0) AS home_tryon_uv,
  COALESCE(e.home_feeds_uv, 0) AS home_feeds_uv,
  COALESCE(e.post_tryon_uv, 0) AS post_tryon_uv,
  COALESCE(e.post_scene_exposure_uv, 0) AS post_scene_exposure_uv,
  COALESCE(e.product_detail_tryon_uv, 0) AS product_detail_tryon_uv,
  COALESCE(e.product_detail_uv, 0) AS product_detail_uv,
  COALESCE(s.styleme_uv, 0) AS styleme_uv,
  COALESCE(e.product_click_uv, 0) AS product_click_uv,
  COALESCE(e.product_exposure_uv, 0) AS product_exposure_uv,
  COALESCE(e.home_product_click_uv, 0) AS home_product_click_uv,
  COALESCE(e.home_product_exposure_uv, 0) AS home_product_exposure_uv,
  COALESCE(e.tryon_result_click_uv, 0) AS tryon_result_click_uv,
  COALESCE(e.tryon_result_exposure_uv, 0) AS tryon_result_exposure_uv,
  COALESCE(e.shop_click_uv, 0) AS shop_click_uv,
  COALESCE(e.shop_exposure_uv, 0) AS shop_exposure_uv,
  COALESCE(e.tryon_uv_total, 0) AS tryon_uv_total,
  COALESCE(e.tryon_pv, 0) AS tryon_pv,
  COALESCE(e.product_click_pv_raw, 0) AS product_click_pv_raw,
  SAFE_DIVIDE(COALESCE(e.tryon_pv, 0), NULLIF(COALESCE(e.tryon_uv_total, 0), 0)) AS daily_tryon_per_tryon_uv,
  SAFE_DIVIDE(COALESCE(e.product_click_pv_raw, 0), NULLIF(COALESCE(e.product_click_uv, 0), 0)) AS daily_click_per_uv
FROM period_calendar c
LEFT JOIN active_users_daily a
  ON a.dt = c.dt
LEFT JOIN user_group_daily g
  ON g.dt = c.dt
LEFT JOIN event_metric_daily e
  ON e.dt = c.dt
LEFT JOIN styleme_daily s
  ON s.period_tag = c.period_tag
  AND s.dt = c.dt;

-- -- 结果集 1：主指标（1~18），统一转成长表，便于直接喂给看板配置。
-- WITH period_agg AS (
--   SELECT
--     period_tag,
--     COUNT(*) AS day_cnt,
--     SUM(dau_new_uv) AS sum_dau_new_uv,
--     SUM(dau_old_uv) AS sum_dau_old_uv,
--     SUM(tryon_new_uv) AS sum_tryon_new_uv,
--     SUM(tryon_old_uv) AS sum_tryon_old_uv,
--     SUM(home_tryon_uv) AS sum_home_tryon_uv,
--     SUM(home_feeds_uv) AS sum_home_feeds_uv,
--     SUM(post_tryon_uv) AS sum_post_tryon_uv,
--     SUM(post_scene_exposure_uv) AS sum_post_scene_exposure_uv,
--     SUM(product_detail_tryon_uv) AS sum_product_detail_tryon_uv,
--     SUM(product_detail_uv) AS sum_product_detail_uv,
--     SUM(styleme_uv) AS sum_styleme_uv,
--     SUM(product_click_uv) AS sum_product_click_uv,
--     SUM(product_exposure_uv) AS sum_product_exposure_uv,
--     SUM(home_product_click_uv) AS sum_home_product_click_uv,
--     SUM(home_product_exposure_uv) AS sum_home_product_exposure_uv,
--     SUM(tryon_result_click_uv) AS sum_tryon_result_click_uv,
--     SUM(tryon_result_exposure_uv) AS sum_tryon_result_exposure_uv,
--     SUM(shop_click_uv) AS sum_shop_click_uv,
--     SUM(shop_exposure_uv) AS sum_shop_exposure_uv
--   FROM period_daily_metrics
--   GROUP BY period_tag
-- )
-- SELECT period_tag, metric_name, metric_value
-- FROM (
--   SELECT period_tag, '1. 新用户试穿率' AS metric_name, SAFE_DIVIDE(sum_tryon_new_uv, NULLIF(sum_dau_new_uv, 0)) AS metric_value FROM period_agg
--   UNION ALL
--   SELECT period_tag, '2. 老用户试穿率', SAFE_DIVIDE(sum_tryon_old_uv, NULLIF(sum_dau_old_uv, 0)) FROM period_agg
--   UNION ALL
--   SELECT period_tag, '3. 首页信息流试穿 UV（日均）', SAFE_DIVIDE(sum_home_tryon_uv, day_cnt) FROM period_agg
--   UNION ALL
--   SELECT period_tag, '4. 首页试穿渗透率', SAFE_DIVIDE(sum_home_tryon_uv, NULLIF(sum_home_feeds_uv, 0)) FROM period_agg
--   UNION ALL
--   SELECT period_tag, '5. 帖子试穿 UV（日均）', SAFE_DIVIDE(sum_post_tryon_uv, day_cnt) FROM period_agg
--   UNION ALL
--   SELECT period_tag, '6. 帖子试穿渗透率', SAFE_DIVIDE(sum_post_tryon_uv, NULLIF(sum_post_scene_exposure_uv, 0)) FROM period_agg
--   UNION ALL
--   SELECT period_tag, '7. 商品详情页试穿 UV（日均）', SAFE_DIVIDE(sum_product_detail_tryon_uv, day_cnt) FROM period_agg
--   UNION ALL
--   SELECT period_tag, '8. 商品详情页试穿渗透率', SAFE_DIVIDE(sum_product_detail_tryon_uv, NULLIF(sum_product_detail_uv, 0)) FROM period_agg
--   UNION ALL
--   SELECT period_tag, '9. styleme 试穿 UV（日均）', SAFE_DIVIDE(sum_styleme_uv, day_cnt) FROM period_agg
--   UNION ALL
--   SELECT period_tag, '11. 商品点击 UV（日均）', SAFE_DIVIDE(sum_product_click_uv, day_cnt) FROM period_agg
--   UNION ALL
--   SELECT period_tag, '12. 商品曝光 UV（日均）', SAFE_DIVIDE(sum_product_exposure_uv, day_cnt) FROM period_agg
--   UNION ALL
--   SELECT period_tag, '13. 首页商品点击 UV（日均）', SAFE_DIVIDE(sum_home_product_click_uv, day_cnt) FROM period_agg
--   UNION ALL
--   SELECT period_tag, '14. 首页商品曝光点击率', SAFE_DIVIDE(sum_home_product_click_uv, NULLIF(sum_home_product_exposure_uv, 0)) FROM period_agg
--   UNION ALL
--   SELECT period_tag, '15. 试穿结果页点击 UV（日均）', SAFE_DIVIDE(sum_tryon_result_click_uv, day_cnt) FROM period_agg
--   UNION ALL
--   SELECT period_tag, '16. 试穿结果页曝光点击率', SAFE_DIVIDE(sum_tryon_result_click_uv, NULLIF(sum_tryon_result_exposure_uv, 0)) FROM period_agg
--   UNION ALL
--   SELECT period_tag, '17. 商城商品点击 UV（日均）', SAFE_DIVIDE(sum_shop_click_uv, day_cnt) FROM period_agg
--   UNION ALL
--   SELECT period_tag, '18. 商城商品曝光点击率', SAFE_DIVIDE(sum_shop_click_uv, NULLIF(sum_shop_exposure_uv, 0)) FROM period_agg
-- )
-- ORDER BY period_tag, metric_name;

-- -- 结果集 2：指标 10，人均试穿次数分布（每日 tryon_pv / tryon_uv 分桶）。
-- WITH bucketed AS (
--   SELECT
--     period_tag,
--     CASE
--       WHEN daily_tryon_per_tryon_uv > 0 AND daily_tryon_per_tryon_uv <= 3 THEN '(0~3]'
--       WHEN daily_tryon_per_tryon_uv > 3 AND daily_tryon_per_tryon_uv <= 6 THEN '(3~6]'
--       WHEN daily_tryon_per_tryon_uv > 6 AND daily_tryon_per_tryon_uv <= 10 THEN '(6~10]'
--       WHEN daily_tryon_per_tryon_uv > 10 AND daily_tryon_per_tryon_uv <= 15 THEN '(10~15]'
--       WHEN daily_tryon_per_tryon_uv > 15 THEN '15次以上'
--       ELSE '0或无试穿UV'
--     END AS tryon_per_tryon_uv_bucket
--   FROM period_daily_metrics
-- )
-- SELECT
--   period_tag,
--   tryon_per_tryon_uv_bucket,
--   COUNT(*) AS day_count,
--   SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER (PARTITION BY period_tag)) AS day_ratio
-- FROM bucketed
-- GROUP BY period_tag, tryon_per_tryon_uv_bucket
-- ORDER BY
--   period_tag,
--   CASE tryon_per_tryon_uv_bucket
--     WHEN '(0~3]' THEN 1
--     WHEN '(3~6]' THEN 2
--     WHEN '(6~10]' THEN 3
--     WHEN '(10~15]' THEN 4
--     WHEN '15次以上' THEN 5
--     ELSE 9
--   END;

-- -- 结果集 3：指标 19，人均点击次数分布（每日 商品点击PV/raw_event_id 去重 / 商品点击UV 分桶）。
-- WITH bucketed AS (
--   SELECT
--     period_tag,
--     CASE
--       WHEN daily_click_per_uv > 0 AND daily_click_per_uv <= 3 THEN '(0~3]'
--       WHEN daily_click_per_uv > 3 AND daily_click_per_uv <= 5 THEN '(3~5]'
--       WHEN daily_click_per_uv > 5 AND daily_click_per_uv <= 10 THEN '(5~10]'
--       WHEN daily_click_per_uv > 10 AND daily_click_per_uv <= 15 THEN '(10~15]'
--       WHEN daily_click_per_uv > 15 THEN '15次以上'
--       ELSE '0或无点击UV'
--     END AS click_per_uv_bucket
--   FROM period_daily_metrics
-- )
-- SELECT
--   period_tag,
--   click_per_uv_bucket,
--   COUNT(*) AS day_count,
--   SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER (PARTITION BY period_tag)) AS day_ratio
-- FROM bucketed
-- GROUP BY period_tag, click_per_uv_bucket
-- ORDER BY
--   period_tag,
--   CASE click_per_uv_bucket
--     WHEN '(0~3]' THEN 1
--     WHEN '(3~5]' THEN 2
--     WHEN '(5~10]' THEN 3
--     WHEN '(10~15]' THEN 4
--     WHEN '15次以上' THEN 5
--     ELSE 9
--   END;

-- 结果集 4：指标 11/12 事件形状探查（c*/v* + product_code 非空）。
SELECT
  p.period_tag,
  b.event_name,
  COUNT(1) AS row_cnt,
  COUNT(DISTINCT b.user_id) AS uv_user,
  COUNT(DISTINCT b.raw_event_id) AS pv_raw_event
FROM dwd_base b
INNER JOIN period_calendar p
  ON p.dt = b.dt
WHERE b.product_code IS NOT NULL
  AND (STARTS_WITH(b.event_name, 'c') OR STARTS_WITH(b.event_name, 'v'))
GROUP BY p.period_tag, b.event_name
ORDER BY p.period_tag, row_cnt DESC;
