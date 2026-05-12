# 【AI-Fashion】账号发布数据增量看板踩坑记录

## 正式号累计口径不要使用 tag 创建时间

- 现象：`videos.platform_video_tags.created_at` 只是正式号 tag 写入服务端的时间，会导致 `creator_count` / `formal_video_count_to_date` 在打标当天集中跳数。
- 正确口径：正式号集合仍由 `env=prod` tag 判定，但累计账号数和累计视频数的进入日期要复用“首次 collect 且 `views > 0`”的日期，即 `ads_daily_formal_video_increment` 中的 `video_first_positive_view.first_positive_view_dt`。
