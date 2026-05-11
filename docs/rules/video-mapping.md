# 服务端视频映射关系

## dataset: videos

## youtube

### 发布时间
`videos.yt_videos.published_at`
### 每日每个视频观看次数和点赞次数
`videos.yt_video_analytics_log.collected_at`
`videos.yt_video_analytics_log.video_id`
`videos.yt_video_analytics_log.views`
`videos.yt_video_analytics_log.likes`
### 连接条件
`videos.yt_videos.video_id = videos.yt_video_analytics_log.video_id`
### 完播比
`videos.yt_video_analytics_log.avg_view_percentage`
### 渠道 
`channel_id`
#### 渠道分为video部门渠道和jhonson部门渠道
#### bob 团队测试渠道
`channel_id IN ('UCynAg7WHdiy8SuQpESO__yw','UCgBJozWjiE0cqwv7UNalyog')`
#### bob 团队内部渠道
`channel_id IN ('UC4rj_IlO3V-d5ebVhgagh1w','UC70Y2-yeuFOoTx6yNkqioKA','UCiX5DzRDNIb9fH_MQvoIN7w','UCLs3oc5eyptqVTEnXaIwHvQ','UCPAQ09Nusb_OZzwfWGAE_Aw','UCV5FBW_l8cayS8L_zAIvlkQ','UCVsLt_8hbY5nG5da8sslD_w','UCzIQMkLCHa63JekjxaA133w')`
#### 除了以上渠道，其余都是 jhonson 部门渠道


## tiktok

### 发布时间
`videos.videos.create_time`(这个是时间戳，要转为时间)
### 每日每个视频观看次数和点赞次数
`videos.video_stats_log.collected_at`
`videos.video_stats_log.video_id`
`videos.video_stats_log.view_count`
`videos.video_stats_log.like_count`
### 连接条件
`videos.id = videos.video_stats_log.video_id`
### 完播比
`视频平均观看时长（服务端还没提供字段）/ videos.videos.duration`
#### 博主
`user_id`


## ins

### 发布时间
`videos.ig_media.published_at`
### 每日每个视频观看次数
`videos.ig_media_stats_log.collected_at`
`videos.ig_media_stats_log.media_id`
`videos.ig_media_stats_log.view_count`
`videos.ig_media_stats_log.like_count`
### 连接条件
`videos.ig_media.media_id = videos.ig_media_stats_log.media_id`
### 完播比
`ig_media_stats_log.avg_watch_time / 视频时长（服务端还没提供字段）`
### 博主
`account_id`


## 自动发布统一视频到不同平台的去重逻辑
1. youtube
`yt_uploads.task_id + yt_uploads.status = "COMPLETED"`
`yt_uploads.channel_id + yt_uploads.youtube_video_id -> yt_video_analytics_log.channel_id + yt_video_analytics_log.video_id`

2. ins
`ig_uploads.task_id + ig_uploads.status = "COMPLETED"`
`ig_uploads.channel_id + ig_uploads.instagram_media_id -> ig_media_stats_log.account_id + ig_media_stats_log.media_id`

3. tiktok
`video_uploads.task_id + video_uploads.status = "COMPLETED"`
`video_uploads.user_id + video_uploads.tiktok_video_id -> video_stats_log.user_id + video_stats_log.video_id`


## 自动发布视频对应 8 位数字编码
`JSON_VALUE(video_upload_tasks.ext_info, '$.promotion_code')`


## 正式号视频
`videos.platform_video_tags.tag_type = 'env' AND videos.platform_video_tags.tag_value = 'prod'`
平台: `videos.platform_video_tags.platform`
视频 id：`videos.platform_video_tags.video_id`