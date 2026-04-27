# 【Social Content】社媒表现数据看板

## 报表

### 中间表：`dws_social_video_daily`

- 表：`decom.dws_social_video_daily`
- 粒度：`dt + channel + creator_id + video_key`
- 作用：承接社媒视频从发布时间起的逐日累计值与日增量，供 `ads_daily_social_content` 聚合
- 调度：独立表级任务 `jobs/dws/dws_social_video_daily.py`

### 中间表：`dws_social_channel_daily`

- 表：`decom.dws_social_channel_daily`
- 粒度：`dt + channel`
- 作用：承接社媒渠道级指标聚合与“全部渠道”统一视频去重，供 `ads_daily_social_content` 只做展示层派生
- 调度：独立表级任务 `jobs/dws/dws_social_channel_daily.py`

### 最终报表：`ads_daily_social_content`

- 表：`decom.ads_daily_social_content`
- 粒度：`dt + channel`
- Dashboard：[【Social Content】社媒表现数据看板](https://bi.alvinclub.ca/superset/dashboard/102/)（id=102）
- 调度：独立表级任务 `jobs/ads/ads_daily_social_content.py`，不挂共享 `ADS` 旧入口，也不接入共享 `DWS` 旧入口

## 字段

| 需求字段 | 定义 |
|------|------|
| 日期 | 报表日期，按 `America/Toronto` 切天 |
| 渠道 | 主表直接产出 `全部渠道 / youtube / tiktok / ins` 4 个展示渠道值 |
| 每日新增 views | 每个视频每天取 `max(collected_at)` 对应的累计播放值，与前一日日末累计播放值做差后按渠道汇总 |
| 每日新增 likes | 每个视频每天取 `max(collected_at)` 对应的累计点赞值，与前一日日末累计点赞值做差后按渠道汇总 |
| 每天新增视频数 | 某视频第一次被采集到且播放量大于 0 的那一天计为新增，后续日期不重复计数；单渠道按原始视频汇总，`全部渠道` 在自动发布场景下按统一任务去重 |
| 新增视频平均播放数 | 当天新增视频首次被采到正播放量时的累计播放数之和 / 当天新增视频数 |
| 前一天新增 views / 博主总数 | `dt` 当天新增 views / 截至 `dt-1` 已发布过视频的历史去重博主总数；YouTube 博主为 `channel_id`，TikTok 为 `user_id`，Ins 为 `account_id`；`全部渠道` 按 `channel + creator_id` 去重 |
| 每个博主最新 3 条的平均赞播率 | 先限定“截至 `dt` 已经有采集数据”的视频，再对每个博主按发布时间倒序取其中最新 3 条，样本池整体按 `SUM(likes) / SUM(views)` 计算 |
| 完播比 | 需求已确认但业务口径未定，暂不落地 |
| 第一秒留存率 | 需求已确认但业务口径未定，暂不落地 |

## 已落地字段

### 中间表：`decom.dws_social_video_daily`

| 需求字段 | 落地表 | 落地字段 | 口径 |
|------|------|------|------|
| 日期 | `decom.dws_social_video_daily` | `dt` | 按 `America/Toronto` 切天；每个视频从 `published_dt` 起到当前日期每天都有一行 |
| 渠道 | `decom.dws_social_video_daily` | `channel` | 固定枚举 `youtube / tiktok / ins` |
| 博主 | `decom.dws_social_video_daily` | `creator_id` | YouTube 用 `channel_id`，TikTok 用 `user_id`，Ins 用 `account_id` |
| 视频主键 | `decom.dws_social_video_daily` | `video_key` | 渠道前缀 + 平台视频主键，如 `youtube:{video_id}` |
| 日末累计 views | `decom.dws_social_video_daily` | `day_end_views` | 当天若有采集则取该日最后一次采集的累计值；否则沿用最近一次已知累计值 |
| 日末累计 likes | `decom.dws_social_video_daily` | `day_end_likes` | 当天若有采集则取该日最后一次采集的累计值；否则沿用最近一次已知累计值 |
| 每日新增 views | `decom.dws_social_video_daily` | `daily_view_increment` | `day_end_views - 前一天 day_end_views` |
| 每日新增 likes | `decom.dws_social_video_daily` | `daily_like_increment` | `day_end_likes - 前一天 day_end_likes` |
| 每天新增视频数 | `decom.dws_social_video_daily` | `is_new_video` | 按 `video_key` 全历史实际采集日中最早满足 `raw_day_end_views > 0` 的 `dt` 记为新增日；当日 `is_new_video = TRUE`，后续日期不重复计数 |
| 新增视频贡献播放数 | `decom.dws_social_video_daily` | `new_video_contributed_view_count` | 仅在 `is_new_video = TRUE` 当天取 `raw_day_end_views`，表示该视频首次被采到正播放量时已经累计拿到的播放数；其余日期为 `0` |
| 当天真实采集 views | `decom.dws_social_video_daily` | `raw_day_end_views` | 仅当 `dt` 当天真的有采集快照时写入；否则为 `NULL`，仅保留作审计 |
| 当天真实采集 likes | `decom.dws_social_video_daily` | `raw_day_end_likes` | 仅当 `dt` 当天真的有采集快照时写入；否则为 `NULL`，仅保留作审计 |
| 截至当天是否已有快照 | `decom.dws_social_video_daily` | `has_snapshot_to_dt` | 从发布时间到 `dt` 为止，是否已经至少出现过一次真实采集快照 |

### 最终报表：`decom.ads_daily_social_content`

| 需求字段 | 落地表 | 落地字段 | 口径 |
|------|------|------|------|
| 渠道 | `decom.ads_daily_social_content` | `channel` | 固定枚举 `全部渠道 / youtube / tiktok / ins` |
| 每日新增 views | `decom.ads_daily_social_content` | `daily_view_increment` | 各渠道视频按 `America/Toronto` 切天，先取每日 `max(collected_at)` 对应累计播放值，再减前一日日末累计值后汇总 |
| 每日新增 likes | `decom.ads_daily_social_content` | `daily_like_increment` | 各渠道视频按 `America/Toronto` 切天，先取每日 `max(collected_at)` 对应累计点赞值，再减前一日日末累计值后汇总 |
| 每天新增视频数 | `decom.ads_daily_social_content` | `new_video_count` | 来自 `decom.dws_social_channel_daily`；`youtube / tiktok / ins` 仍按 `COUNT(DISTINCT IF(is_new_video, video_key, NULL))` 汇总；`全部渠道` 通过 `decom.dim_auto_publish_video_task.unified_video_key` 做自动发布统一视频去重 |
| 新增视频平均播放数 | `decom.ads_daily_social_content` | `avg_new_video_view_count` | `new_video_contributed_view_count / new_video_count`；其中 `new_video_contributed_view_count` 是内部辅助字段，仅用于计算该指标，不在展示层单独强调 |
| 前一天新增 views / 博主总数 | `decom.ads_daily_social_content` | `daily_view_increment_per_creator` | 分子为 `daily_view_increment`；分母为 `creator_count_to_prev_day`，即截至 `dt-1` 已发布过视频的历史去重博主数；`全部渠道` 用 `channel + creator_id` 去重 |
| 每个博主最新 3 条的平均赞播率 | `decom.ads_daily_social_content` | `latest_3_avg_like_view_rate` | 先限定截至 `dt` 已有采集数据的视频，再按发布时间倒序取每个博主其中最新 3 条；汇总样本池的 `latest_3_sample_like_count / latest_3_sample_view_count`；`全部渠道` 仍按 `channel + creator_id` 分组取各博主最新 3 条 |

补充说明：
- YouTube 统一按 `docs/rules/video-mapping.md` 排除 AI 团队渠道，即 `video 部门测试渠道 + video 部门内部渠道`
- `views / likes` 源字段都是累计值，不是日增值
- 若某视频在 `dt` 没有新采集记录，则其 `dt` 日末累计值沿用最近一次已采集值，因此 `dt` 的日增量记为 `0`
- `decom.dws_social_video_daily` 对下游承诺“从 `published_dt` 起每天一行”的 dense daily 语义；其中 `day_end_views / day_end_likes` 是主口径字段，`raw_day_end_views / raw_day_end_likes` 只保留作审计
- “每天新增视频数”沿用投资人报表的视频新增口径：按 `video_key` 全历史最早满足 `raw_day_end_views > 0` 的日期记为新增日，不依赖 `published_dt = dt`
- 因为 dashboard 支持 `channel` filter，自动发布跨平台去重只作用于 `channel = 全部渠道`；筛到单渠道时仍保留各平台原始视频数量口径
- 自动发布统一视频映射当前由 `decom.dim_auto_publish_video_task` 统一维护，不再由 Social Content 自己重复拼三端上传表
- “新增视频平均播放数”的分子不是当天全量视频播放增量，而是新增视频在首次被采到正播放量当天的 `raw_day_end_views` 之和
- `jobs/dws/dws_social_video_daily.py` 的实现采用“真实采集日快照 -> 生效区间 -> 一次性展开逐日明细”的批量方式，不再逐天启动查询任务
- “博主总数”按发布时间判断，不依赖是否已经采集到播放快照
- “最新 3 条”不是先取发布时间最新 3 条再判断有没有数据，而是先限定截至 `dt` 已有采集数据的视频，再从这些视频里按发布时间倒序取最新 3 条
- `全部渠道` 不是额外视图字段，而是直接写入主表的展示渠道值
- 看板当前落地 6 个指标；`完播比`、`第一秒留存率` 先保留在需求范围内，不进入当前 ADS 字段

## 依赖

- 上游：`videos.yt_videos`, `videos.yt_video_analytics_log`, `videos.videos`, `videos.video_stats_log`, `videos.ig_media`, `videos.ig_media_stats_log`
- 中间表：`decom.dim_auto_publish_video_task`, `decom.dws_social_video_daily`, `decom.dws_social_channel_daily`
- 报表表：`decom.ads_daily_social_content`
- Superset：主物理 dataset `ads_daily_social_content`（id=230），chart `Social Content（日表）-每日新增 Views 趋势`（id=575）、`Social Content（日表）-每日新增 Likes 趋势`（id=576）、`Social Content（日表）-前一天新增 Views / 博主总数趋势`（id=577）、`Social Content（日表）-每个博主最新 3 条平均赞播率趋势`（id=578），dashboard `【Social Content】社媒表现数据看板`（id=102）
- 脚本：`jobs/dim/dim_auto_publish_video_task.py`, `jobs/dws/dws_social_video_daily.py`, `jobs/dws/dws_social_channel_daily.py`, `jobs/ads/ads_daily_social_content.py`
- DDL：`ddl/dim/dim_auto_publish_video_task.sql`, `ddl/dws/dws_social_video_daily.sql`, `ddl/dws/dws_social_channel_daily.sql`, `ddl/ads/ads_daily_social_content.sql`
- 编排：`env/etl_config.json`, `etl_run.py`
