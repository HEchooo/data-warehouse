# 服务端视频映射关系

## dataset: videos

## youtube

### 发布时间
`yt_videos.published_at`
### 每日每个视频观看次数
`yt_video_analytics_log.collected_at`
`yt_video_analytics_log.video_id`
`yt_video_analytics_log.views`
### 连接条件
`yt_videos.video_id = yt_video_analytics_log.video_id`

## tiktok

### 发布时间
`videos.create_time`(这个是时间戳，要转为时间)
### 每日每个视频观看次数
`video_stats_log.collected_at`
`video_stats_log.video_id`
`video_stats_log.view_count`
### 连接条件
`videos.id = video_stats_log.video_id`

## ins

### 发布时间
`ig_media.published_at`
### 每日每个视频观看次数
`ig_media_stats_log.collected_at`
`ig_media_stats_log.media_id`
`ig_media_stats_log.view_count`
### 连接条件
`ig_media.media_id = ig_media_stats_log.media_id`