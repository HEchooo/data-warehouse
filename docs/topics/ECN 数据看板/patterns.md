# ECN 数据看板

## 正式号累计口径不要使用 tag 创建时间

- 现象：`videos.platform_video_tags.created_at` 只是正式号 tag 写入服务端的时间，会导致 `creator_count` / `formal_video_count_to_date` 在打标当天集中跳数。
- 正确口径：正式号集合仍由 `env=prod` tag 判定，但累计账号数和累计视频数的进入日期要复用“首次 collect 且 `views > 0`”的日期，即 `ads_daily_formal_video_increment` 中的 `video_first_positive_view.first_positive_view_dt`。

## tag 新增后必须先刷新 dim_video

- 现象：直接查 `videos.yt_video_analytics_log` join `videos.platform_video_tags` 有播放数据，但 `ads_daily_formal_video_increment` 仍为 0。
- 原因：ADS 不直接读 tag 表和原始 analytics，而是读取 `dim_video + dws_social_video_state_daily`；如果 tag 新增后只刷 DWS/ADS，新的正式号视频不会进入 `dim_video.is_formal_video`，下游仍按旧正式号集合计算。
- 正确刷新顺序：`dim_video -> dws_social_video_snapshot_daily -> dws_social_video_state_daily -> ads_daily_formal_video_increment -> ads_weekly_formal_video_increment_growth`。
