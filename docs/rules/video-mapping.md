# 服务端视频映射关系

## dataset: videos

## youtube

### 发布时间
`videos.yt_videos.published_at`
### 每日每个视频观看次数
`videos.yt_video_analytics_log.collected_at`
`videos.yt_video_analytics_log.video_id`
`videos.yt_video_analytics_log.views`
### 连接条件
`videos.yt_videos.video_id = videos.yt_video_analytics_log.video_id`

## tiktok

### 发布时间
`videos.videos.create_time`(这个是时间戳，要转为时间)
### 每日每个视频观看次数
`videos.video_stats_log.collected_at`
`videos.video_stats_log.video_id`
`videos.video_stats_log.view_count`
### 连接条件
`videos.id = videos.video_stats_log.video_id`

## ins

### 发布时间
`videos.ig_media.published_at`
### 每日每个视频观看次数
`videos.ig_media_stats_log.collected_at`
`videos.ig_media_stats_log.media_id`
`videos.ig_media_stats_log.view_count`
### 连接条件
`videos.ig_media.media_id = videos.ig_media_stats_log.media_id`
