# ECN 数据看板

## 正式号累计口径不要使用 tag 创建时间

- 现象：`videos.platform_video_tags.created_at` 只是正式号 tag 写入服务端的时间，会导致 `creator_count` / `formal_video_count_to_date` 在打标当天集中跳数。
- 正确口径：正式号集合仍由 `env=prod` tag 判定，但累计账号数和累计视频数的进入日期要复用“首次 collect 且 `views > 0`”的日期，即 `ads_daily_formal_video_increment` 中的 `video_first_positive_view.first_positive_view_dt`。

## tag 新增后必须先刷新 dim_video

- 现象：直接查 `videos.yt_video_analytics_log` join `videos.platform_video_tags` 有播放数据，但 `ads_daily_formal_video_increment` 仍为 0。
- 原因：ADS 不直接读 tag 表和原始 analytics，而是读取 `dim_video + dws_social_video_state_daily`；如果 tag 新增后只刷 DWS/ADS，新的正式号视频不会进入 `dim_video.is_formal_video`，下游仍按旧正式号集合计算。
- 正确刷新顺序：`dim_video -> dws_social_video_snapshot_daily -> dws_social_video_state_daily -> ads_daily_formal_video_increment -> ads_weekly_formal_video_increment_growth`。

## 周增长类图表不要在 chart 里写死常量 metric

- 现象：周增长倍数、月度增长倍数、季度增长倍数卡片在 Superset chart 里直接把 metric 写成常量，例如 `1.79`、`12.64`、`2019.07`，看板会长期显示“好看但不真实”的结果。
- 正确做法：这 3 张卡片都要直接读取 `decom.ads_weekly_formal_video_increment_growth` 的聚合字段，分别使用 `COALESCE(MAX(avg_week_growth_multiple), 0)`、`COALESCE(MAX(monthly_growth_multiple), 0)`、`COALESCE(MAX(quarterly_growth_multiple), 0)`。
- 修改方式：通过 Superset API 更新 chart 时，要同时改 `params.metric.sqlExpression`、`query_context.queries[*].metrics[*].sqlExpression` 和 `query_context.form_data.metric.sqlExpression`，不能只改一处。

## 周表增长口径必须排除未满 7 天窗口

- 现象：如果 `ads_weekly_formal_video_increment_growth` 直接按 `week_index` 聚合，不校验窗口天数，最后一个未满 7 天的周也会进入周均值和增长倍数，导致图 6-9 一起失真。
- 正确做法：周窗口从 `2026-03-30` 起按 7 天切分，只有 `COUNT(*) = 7` 的完整周才能入表；`week_end_dt` 应固定为 `week_start_dt + 6 day`，不要用当前窗口里的 `MAX(dt)` 兜底。
