# 服务端视频映射关系

## dataset: videos

## 视频与账号展示字段

以下映射是三端视频与账号展示属性的统一来源。主题报表需要视频链接、视频标题、账号名称或账号 URL 时，直接引用本节，不在各主题重复维护字段映射。

| 平台 | 视频 ID | 视频标题 | 账号 ID | 账号展示名称 | 账号用户名 / Handle | 视频 URL | 账号 URL | 主表连接条件 |
|------|------|------|------|------|------|------|------|------|
| YouTube | `videos.yt_videos.video_id` | `videos.yt_videos.title` | `videos.yt_videos.channel_id` | `videos.yt_channels.channel_name` | `videos.yt_channels.custom_url` | `CONCAT('https://www.youtube.com/watch?v=', videos.yt_videos.video_id)` | `CONCAT('https://www.youtube.com/', videos.yt_channels.custom_url)` | `videos.yt_videos.channel_id = videos.yt_channels.channel_id` |
| TikTok | `videos.videos.id` | `videos.videos.title` | `videos.videos.user_id` | `videos.users.display_name` | `videos.users.username` | `videos.videos.embed_link` | `videos.users.profile_deep_link` | `videos.videos.user_id = videos.users.id` |
| Instagram | `videos.ig_media.media_id` | `videos.ig_media.caption`；Instagram 当前没有独立标题字段 | `videos.ig_media.account_id` | `videos.ig_channels.account_name` | `videos.ig_channels.username` | `videos.ig_media.permalink` | `CONCAT('https://www.instagram.com/', videos.ig_channels.account_name, '/reels/')` | `videos.ig_media.account_id = videos.ig_channels.account_id` |

补充规则：

- 三端视频标题统一下沉为 `decom.dim_video.video_title`；YouTube / TikTok 取 `title`，Instagram 取 `caption`
- URL 必须使用表中字段或上表固定表达式，不自行猜测其他平台路径
- `TikTok embed_link`、`profile_deep_link` 或其他源字段为空时保留空值；除非业务明确确认，不使用其他 URL 字段兜底
- `Instagram caption` 是视频文案，不等同于平台提供的独立视频标题

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
视频 id：`videos.platform_video_tags.platform_video_id`
账号 id：`videos.platform_video_tags.platform_channel_id`
