# 【Social Content】社媒表现数据看板

## 报表

### 中间表：`dws_social_video_snapshot_daily`

- 表：`decom.dws_social_video_snapshot_daily`
- 粒度：`dt + channel + creator_id + video_key`（仅真实采集日有记录）
- 作用：消费 `decom.dim_video` 与三端原始快照日志，按 `dt+video_key` 取每天最后一次采集的累计 `views/likes`，作为社媒视频口径的唯一快照源
- 调度：独立表级任务 `jobs/dws/dws_social_video_snapshot_daily.py`

### 中间表：`dws_social_video_state_daily`

- 表：`decom.dws_social_video_state_daily`
- 粒度：`dt + channel + creator_id + video_key`
- 作用：只消费 `dws_social_video_snapshot_daily`，承接社媒视频从发布时间起的逐日累计状态，主口径只保留 `day_end_views / day_end_likes`
- 调度：独立表级任务 `jobs/dws/dws_social_video_state_daily.py`

### 中间表：`dws_social_channel_daily`

- 表：`decom.dws_social_channel_daily`
- 粒度：`dt + channel`
- 作用：直接消费 `dws_social_video_state_daily`，承接社媒渠道级指标聚合与“全部渠道”统一视频去重，供 `ads_daily_social_content` 只做展示层派生
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
| 新视频7天平均播放数 | 统计日 `dt` 回看 `dt-6` 发布的视频 cohort，取这些视频在 `dt-6 ~ dt` 的累计播放数总和 / cohort 视频数；单渠道按原始视频统计，`全部渠道` 按统一视频首发日对齐 cohort |
| 前一天新增 views / 博主总数 | `dt` 当天新增 views / 截至 `dt-1` 已发布过视频的历史去重博主总数；YouTube 博主为 `channel_id`，TikTok 为 `user_id`，Ins 为 `account_id`；`全部渠道` 按 `channel + creator_id` 去重 |
| 每个博主最新 3 条的平均赞播率 | 先限定“截至 `dt` 已经有采集数据”的视频，再对每个博主按发布时间倒序取其中最新 3 条，样本池整体按 `SUM(likes) / SUM(views)` 计算 |
| 完播比 | 需求已确认但业务口径未定，暂不落地 |
| 第一秒留存率 | 需求已确认但业务口径未定，暂不落地 |

## 已落地字段

### 中间表：`decom.dws_social_video_snapshot_daily`

| 需求字段 | 落地表 | 落地字段 | 口径 |
|------|------|------|------|
| 日期 | `decom.dws_social_video_snapshot_daily` | `dt` | 按 `America/Toronto` 切天；只在有真实采集的日期产出 |
| 渠道 | `decom.dws_social_video_snapshot_daily` | `channel` | 固定枚举 `youtube / tiktok / ins` |
| 博主 | `decom.dws_social_video_snapshot_daily` | `creator_id` | YouTube 用 `channel_id`，TikTok 用 `user_id`，Ins 用 `account_id` |
| 视频主键 | `decom.dws_social_video_snapshot_daily` | `video_key` | 渠道前缀 + 平台视频主键，如 `youtube:{video_id}` |
| 日末累计 views | `decom.dws_social_video_snapshot_daily` | `day_end_views` | 每个视频每天取 `max(collected_at)` 对应的累计播放值 |
| 日末累计 likes | `decom.dws_social_video_snapshot_daily` | `day_end_likes` | 每个视频每天取 `max(collected_at)` 对应的累计点赞值 |

### 中间表：`decom.dws_social_video_state_daily`

| 需求字段 | 落地表 | 落地字段 | 口径 |
|------|------|------|------|
| 日期 | `decom.dws_social_video_state_daily` | `dt` | 按 `America/Toronto` 切天；每个视频从 `published_dt` 起到当前日期每天都有一行 |
| 渠道 | `decom.dws_social_video_state_daily` | `channel` | 固定枚举 `youtube / tiktok / ins` |
| 博主 | `decom.dws_social_video_state_daily` | `creator_id` | YouTube 用 `channel_id`，TikTok 用 `user_id`，Ins 用 `account_id` |
| 视频主键 | `decom.dws_social_video_state_daily` | `video_key` | 渠道前缀 + 平台视频主键，如 `youtube:{video_id}` |
| 日末累计 views | `decom.dws_social_video_state_daily` | `day_end_views` | 当天若 `snapshot_daily` 有采集则取该值；否则沿用前一天状态或窗口期初承接值 |
| 日末累计 likes | `decom.dws_social_video_state_daily` | `day_end_likes` | 当天若 `snapshot_daily` 有采集则取该值；否则沿用前一天状态或窗口期初承接值 |

### 最终报表：`decom.ads_daily_social_content`

| 需求字段 | 落地表 | 落地字段 | 口径 |
|------|------|------|------|
| 渠道 | `decom.ads_daily_social_content` | `channel` | 固定枚举 `全部渠道 / youtube / tiktok / ins` |
| 每日新增 views | `decom.ads_daily_social_content` | `daily_view_increment` | 各渠道视频按 `America/Toronto` 切天，先取每日 `max(collected_at)` 对应累计播放值，再减前一日日末累计值后汇总 |
| 每日新增 likes | `decom.ads_daily_social_content` | `daily_like_increment` | 各渠道视频按 `America/Toronto` 切天，先取每日 `max(collected_at)` 对应累计点赞值，再减前一日日末累计值后汇总 |
| 每天新增视频数 | `decom.ads_daily_social_content` | `new_video_count` | 来自 `decom.dws_social_channel_daily`；`youtube / tiktok / ins` 仍按 `COUNT(DISTINCT IF(is_new_video, video_key, NULL))` 汇总；`全部渠道` 通过 `decom.dim_auto_publish_video_task.unified_video_key` 做自动发布统一视频去重 |
| 新增视频平均播放数 | `decom.ads_daily_social_content` | `avg_new_video_view_count` | `new_video_contributed_view_count / new_video_count`；其中 `new_video_contributed_view_count` 是内部辅助字段，仅用于计算该指标，不在展示层单独强调 |
| 新视频7天平均播放数 | `decom.ads_daily_social_content` | `published_7d_cohort_avg_view_count` | `published_7d_cohort_view_count / published_7d_cohort_video_count`；其中 `published_7d_cohort_video_count` / `published_7d_cohort_view_count` 来自 `decom.dws_social_channel_daily`，表示统计日 `dt` 回看 `dt-6` 发布 cohort 的样本数与 7 天累计播放数 |
| 前一天新增 views / 博主总数 | `decom.ads_daily_social_content` | `daily_view_increment_per_creator` | 分子为 `daily_view_increment`；分母为 `creator_count_to_prev_day`，即截至 `dt-1` 已发布过视频的历史去重博主数；`全部渠道` 用 `channel + creator_id` 去重 |
| 每个博主最新 3 条的平均赞播率 | `decom.ads_daily_social_content` | `latest_3_avg_like_view_rate` | 先限定截至 `dt` 已有采集数据的视频，再按发布时间倒序取每个博主其中最新 3 条；汇总样本池的 `latest_3_sample_like_count / latest_3_sample_view_count`；`全部渠道` 仍按 `channel + creator_id` 分组取各博主最新 3 条 |

补充说明：
- YouTube 统一按 `docs/rules/video-mapping.md` 排除 AI 团队渠道，即 `video 部门测试渠道 + video 部门内部渠道`
- `views / likes` 源字段都是累计值，不是日增值
- 若某视频在 `dt` 没有新采集记录，则其 `dt` 日末累计值沿用最近一次已采集值，因此 `dt` 的日增量记为 `0`
- 原始快照排序（`dt+video` 取最后采集）只在 `dws_social_video_snapshot_daily` 做一次，`state/channel/ads` 不再直接扫描 raw log
- `decom.dws_social_video_state_daily` 对下游承诺“从 `published_dt` 起每天一行”的 dense daily 语义，主口径只保留 `day_end_views / day_end_likes`
- “每天新增视频数”在渠道层直接按状态表派生：`day_end_views > 0 AND LAG(day_end_views, 1, 0) = 0`
- 因为 dashboard 支持 `channel` filter，自动发布跨平台去重只作用于 `channel = 全部渠道`；筛到单渠道时仍保留各平台原始视频数量口径
- 自动发布统一视频映射当前由 `decom.dim_auto_publish_video_task` 统一维护，不再由 Social Content 自己重复拼三端上传表
- “新增视频平均播放数”的分子不是当天全量视频播放增量，而是新增视频在首次变成正播放当天的 `day_end_views` 之和
- “新视频7天平均播放数”按发布 cohort 统计，不复用 `is_new_video` 首次正播放日口径；单渠道按 `published_dt = dt-6` 取样，`全部渠道` 则按统一视频键全历史最早 `published_dt` 对齐 cohort 日期
- “新视频7天平均播放数”的分子使用 `dt` 当天该 cohort 视频的 `day_end_views` 汇总，等价于这些视频从发布日到第 7 天的累计播放数；若中间某天无新采集，则仍沿用最近一次已知累计值
- `jobs/dws/dws_social_video_state_daily.py` 直接维护视频日状态层，再由渠道层按需做 `LAG` 和新增视频派生
- “博主总数”按发布时间判断，不依赖是否已经采集到播放快照
- “最新 3 条”不是先取发布时间最新 3 条再判断有没有数据，而是先限定截至 `dt` 已有采集数据的视频，再从这些视频里按发布时间倒序取最新 3 条
- `全部渠道` 不是额外视图字段，而是直接写入主表的展示渠道值
- 看板当前落地 7 个指标；`完播比`、`第一秒留存率` 先保留在需求范围内，不进入当前 ADS 字段

## 依赖

- 上游：`videos.yt_videos`, `videos.yt_video_analytics_log`, `videos.videos`, `videos.video_stats_log`, `videos.ig_media`, `videos.ig_media_stats_log`
- 中间表：`decom.dim_video`, `decom.dim_auto_publish_video_task`, `decom.dws_social_video_snapshot_daily`, `decom.dws_social_video_state_daily`, `decom.dws_social_channel_daily`
- 报表表：`decom.ads_daily_social_content`
- Superset：主物理 dataset `ads_daily_social_content`（id=230），chart `Social Content（日表）-每日新增 Views 趋势`（id=575）、`Social Content（日表）-每日新增 Likes 趋势`（id=576）、`Social Content（日表）-前一天新增 Views / 博主总数趋势`（id=577）、`Social Content（日表）-每个博主最新 3 条平均赞播率趋势`（id=578）、`Social Content（日表）-每天新增视频数趋势`（id=579）、`Social Content（日表）-新增视频平均播放数趋势`（id=580）、`Social Content（日表）-新视频7天平均播放数趋势`（id=586），dashboard `【Social Content】社媒表现数据看板`（id=102）
- 脚本：`jobs/dim/dim_video.py`, `jobs/dim/dim_auto_publish_video_task.py`, `jobs/dws/dws_social_video_snapshot_daily.py`, `jobs/dws/dws_social_video_state_daily.py`, `jobs/dws/dws_social_channel_daily.py`, `jobs/ads/ads_daily_social_content.py`
- DDL：`ddl/dim/dim_video.sql`, `ddl/dim/dim_auto_publish_video_task.sql`, `ddl/dws/dws_social_video_snapshot_daily.sql`, `ddl/dws/dws_social_video_state_daily.sql`, `ddl/dws/dws_social_channel_daily.sql`, `ddl/ads/ads_daily_social_content.sql`
- 编排：`env/etl_config.json`, `etl_run.py`
