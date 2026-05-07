-- 投资人报表（核心业务报表）
-- 2026-05-07 指标改造后验证 SQL（回填区间：2026-04-01 ~ 2026-05-06）
-- 说明：旧字段 avg_video_view_count 已下线，此处用同公式重算代理值做对照。

-- 1) 区间汇总（新旧口径对照）
WITH base AS (
  SELECT
    dt,
    new_video_count,
    active_video_count,
    new_video_view_count,
    avg_new_video_view_count,
    new_video_7d_avg_view_count,
    video_production_cost_usd,
    cost_per_thousand_views_usd,
    CASE
      WHEN active_video_count = 0 THEN 0
      ELSE ROUND(CAST(new_video_view_count AS NUMERIC) / CAST(active_video_count AS NUMERIC), 4)
    END AS legacy_avg_video_view_count_proxy
  FROM `my-project-8584-jetonai.decom.ads_daily_investor`
  WHERE dt BETWEEN DATE '2026-04-01' AND DATE '2026-05-06'
)
SELECT
  COUNT(*) AS day_cnt,
  MIN(dt) AS min_dt,
  MAX(dt) AS max_dt,
  ROUND(AVG(legacy_avg_video_view_count_proxy), 4) AS avg_legacy_proxy,
  ROUND(AVG(new_video_7d_avg_view_count), 4) AS avg_new_video_7d,
  ROUND(AVG(video_production_cost_usd), 4) AS avg_production_cost_usd,
  ROUND(AVG(cost_per_thousand_views_usd), 4) AS avg_cpm_usd,
  MIN(cost_per_thousand_views_usd) AS min_cpm_usd,
  MAX(cost_per_thousand_views_usd) AS max_cpm_usd
FROM base;

-- 2) 明细抽样（最近 10 天）
SELECT
  dt,
  new_video_view_count,
  active_video_count,
  CASE
    WHEN active_video_count = 0 THEN 0
    ELSE ROUND(CAST(new_video_view_count AS NUMERIC) / CAST(active_video_count AS NUMERIC), 4)
  END AS legacy_avg_video_view_count_proxy,
  new_video_7d_avg_view_count,
  video_production_cost_usd,
  cost_per_thousand_views_usd
FROM `my-project-8584-jetonai.decom.ads_daily_investor`
WHERE dt BETWEEN DATE '2026-04-01' AND DATE '2026-05-06'
ORDER BY dt DESC
LIMIT 10;

-- 3) 完整性检查（新指标是否有空值）
SELECT
  COUNT(*) AS total_days,
  COUNTIF(new_video_7d_avg_view_count IS NULL) AS null_new_video_7d_avg_view_count,
  COUNTIF(video_production_cost_usd IS NULL) AS null_video_production_cost_usd,
  COUNTIF(cost_per_thousand_views_usd IS NULL) AS null_cost_per_thousand_views_usd
FROM `my-project-8584-jetonai.decom.ads_daily_investor`
WHERE dt BETWEEN DATE '2026-04-01' AND DATE '2026-05-06';
