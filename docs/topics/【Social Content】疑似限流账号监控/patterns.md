# 【Social Content】疑似限流账号监控踩坑记录

## dense T+1 状态不能直接代表真实 T+1 观测

- `dws_social_video_state_daily_v2` 会在某天没有真实采集时沿用最近一次累计值，适合日末状态查询，但不等于 T+1 当天一定采到了平台数据
- 实际抽查发现部分 YouTube 发布日期的 T+1 真实快照覆盖率只有约 67%～80%；如果直接使用 dense state，会把采集缺失误判为播放低迷
- 疑似限流算法必须同时连接 `dws_social_video_snapshot_daily_v2`，只有账号当日全部发布视频都存在 T+1 真实快照时才计算篇均播放并进入后续排名

## tag-only 视频不能默认判为非活跃账号

- `platform_video_tags` 可能先于平台视频主表出现；这类正式号视频在 `yt_videos` / `videos` / `ig_media` 中暂时没有主记录
- 如果只从视频主表连接账号表获取 `is_active`，tag-only 视频会被默认判成非活跃，即使对应账号当前 `is_active = 1`
- `dim_video` 应先统一三端启用账号名单，再按 `channel + creator_id` 给所有视频（包括 tag-only 视频）计算 `is_active_account`
