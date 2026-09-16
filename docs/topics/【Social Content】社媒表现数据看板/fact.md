# 【Social Content】社媒表现数据看板v2

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | Social Content 社媒表现数据看板v2 | 社媒数据看板 |
| 指标 | 每日新增 views | 每日新增播放量、播放增量 |
| 指标 | 每日新增 likes | 每日新增点赞数、点赞增量 |
| 指标 | 日末累计 views | 日末累计播放量 |

## 适用范围

- 业务线：`ECN 视频`, `TikTok 橱窗号`
- 适合：查询社媒各渠道的视频发布、播放、点赞、关注与新视频 cohort 日趋势。
- 不适合：属性拆分应使用[属性维度数据](../【Social Content】属性维度数据/fact.md)；单视频排行应使用[每日排行榜](../【Social Content】每日排行榜/fact.md)。
- 需要澄清：需要确认渠道、日期范围，以及增量、累计或 cohort 指标。

## 报表

### 已废弃的旧口径

- 表：`my-project-8584-jetonai.decom.ads_daily_social_content`
- 状态：`deprecated`
- 粒度：历史社媒日汇总
- 时区：`America/Toronto`
- 数据范围：仅供历史旧口径参考。
- 必要过滤：不得用于新查询。
- 不包含：v2 当前口径。
- 替代表：`my-project-8584-jetonai.decom.ads_daily_social_content_v2`
- 旧 Dashboard：[【Social Content】社媒表现数据看板](https://bi.alvinclub.ca/superset/dashboard/social-content/)（已归档为 Draft）
- 该表仅保留作历史旧口径参考；新取数、报表及趋势分析一律使用 `decom.ads_daily_social_content_v2`，不得将两表数据直接拼接或比较。

### 中间表：`dws_social_video_snapshot_daily_v2`

- 中间表：`decom.dws_social_video_snapshot_daily_v2`
- 粒度：`dt + channel + creator_id + video_key`（仅真实采集日有记录）
- 作用：消费 `decom.dim_video` 与三端原始快照日志，按 `dt+video_key` 取每天最后一次采集的累计 `views/likes`，作为社媒视频口径的唯一快照源
- 调度：独立表级任务 `jobs/dws/dws_social_video_snapshot_daily_v2.py`

### 中间表：`dws_social_video_state_daily_v2`

- 中间表：`decom.dws_social_video_state_daily_v2`
- 粒度：`dt + channel + creator_id + video_key`
- 作用：只消费 `dws_social_video_snapshot_daily_v2`，承接社媒视频从发布时间起的逐日累计状态，主口径只保留 `day_end_views / day_end_likes`
- 调度：独立表级任务 `jobs/dws/dws_social_video_state_daily_v2.py`

### 中间表：`dws_social_channel_daily_v2`

- 中间表：`decom.dws_social_channel_daily_v2`
- 粒度：`dt + channel`
- 作用：直接消费 `dws_social_video_state_daily_v2`，承接社媒渠道级指标聚合与“全部渠道”统一视频去重，供 `ads_daily_social_content_v2` 只做展示层派生
- 调度：独立表级任务 `jobs/dws/dws_social_channel_daily_v2.py`

### 最终报表：`ads_daily_social_content_v2`

- 表：`my-project-8584-jetonai.decom.ads_daily_social_content_v2`
- 状态：`active`
- 粒度：`dt + channel`
- 时区：`America/Toronto`
- 数据范围：社媒渠道视频发布、播放、点赞、关注和 cohort 日汇总。
- 必要过滤：全渠道结果使用文档定义的全部渠道行。
- 不包含：属性、单视频排行和账号限流明细。
- Dashboard：[【Social Content】社媒表现数据看板v2](https://bi.alvinclub.ca/superset/dashboard/social-content-v2/)（id=116）
- 分布表：`decom.ads_daily_social_content_published_day3_view_bucket_v2`
- 分布表粒度：`dt + channel + published_day3_view_bucket`
- 展示起点：`2026-05-01`
- 复用 `decom.dim_video` 与 `decom.dim_auto_publish_video_task`
- YouTube 累计快照使用 `videos.yt_video_stats_log.view_count / like_count`
- 渠道聚合层排除与投资人报表一致的 10 个 YouTube 测试 / 内部频道；该报表不按 `decom.dim_video.is_formal_video` 做例外保留
- 内部历史从 `2026-03-13` 开始构建，以支持 `2026-05-01` 起的日增量、最新3条和7天 cohort 计算
- 首次生产回填范围：`2026-03-13 ~ 2026-06-12`
- 首次回填完成后，独立编排任务已启用

### ads_daily_social_content_published_day3_view_bucket_v2

- 表：`my-project-8584-jetonai.decom.ads_daily_social_content_published_day3_view_bucket_v2`
- 状态：`active`
- 粒度：一行代表一个 `dt × channel × published_day3_view_bucket`
- 时区：`America/Toronto`
- 数据范围：社媒视频发布后第 3 日播放量分布。
- 必要过滤：按目标 `channel` 和播放分桶过滤。
- 不包含：渠道总体指标和其他 cohort 周期。

### Lark ECN 数据发布推送

- 脚本：`jobs/ads/ads_daily_ecn_lark_push.py`
- 推送日期：默认查询 `DATE_SUB(CURRENT_DATE('America/Toronto'), INTERVAL 1 DAY)`，仅发送单个 `dt` 对应的当日 / 昨日对比
- 运行模式：线上 `remote` 模式读取 `env/lark_webhook.json` 的 `remote_ecn`；本地测试必须设置 `PATH_MODE_OVERRIDE=local`
- 发布统计：来自 `videos.video_publish_daily_stats`，按 `stat_date` 聚合三端 platform 后展示活跃运营账号数、API / RPA 发布数、API / RPA 发布成功数
- 社媒读数：`new_video_count` 来自 `decom.ads_daily_investor`，在推送中展示为“新增视频数（社媒读到的）”
- 同卡片还展示发布时间 cohort 平均播放和第三方渠道外链跳转数据，分别复用 `decom.ads_daily_social_content_v2` 与 `decom.ads_daily_thirdapp_channel_uv`

## 字段

日期按 `America/Toronto` 切天。源字段 `views / likes` 都是累计值，不是日增值。YouTube 按 `docs/rules/video-mapping.md` 排除 AI 团队渠道（video 部门测试渠道 + 内部渠道），累计快照用 `yt_video_stats_log`。

### ads_daily_social_content_v2（主报表）

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 渠道 | `channel` | 视频 | 主表直接产出 `全部渠道 / youtube / tiktok / ins` 四个展示值，`全部渠道` 不是视图字段；自动发布跨平台去重只作用于 `全部渠道`，筛到单渠道时保留各平台原始视频数量口径 | 按目标渠道过滤 | `全部渠道` 与单渠道行不可一起聚合 |
| 每日新增 views | `daily_view_increment` | 视频 | 每个视频每天取 `max(collected_at)` 对应的累计播放，减前一日日末累计后按渠道汇总；当天无新采集时沿用最近一次已采值，日增记为 `0` | 无 | 可跨天求和；不可混入 `全部渠道` 行 |
| 每日新增 likes | `daily_like_increment` | 视频 | 同上口径，基于累计点赞 | 无 | 可跨天求和；不可混入 `全部渠道` 行 |
| 每天新增视频数 | `new_video_count` | 视频 | 视频第一次被采集到且播放量大于 0 的那天计为新增，后续不重复计数；渠道层按 `day_end_views > 0 AND LAG(day_end_views, 1, 0) = 0` 派生；`全部渠道` 通过 `decom.dim_auto_publish_video_task.unified_video_key` 做自动发布统一视频去重 | 无 | 可跨天求和；不可混入 `全部渠道` 行 |
| 新增视频贡献播放 | `new_video_contributed_view_count` | 视频 | 新增视频首次变成正播放当天的 `day_end_views` 之和；是平均播放数的分子，不是当天全量播放增量 | 无 | 可跨天求和；不可混入 `全部渠道` 行 |
| 新增视频平均播放数 | `avg_new_video_view_count` | 视频 | `new_video_contributed_view_count / new_video_count` | 无 | 不可 `AVG`；跨天或跨渠道须先汇总分子分母再重算；无有效分母时物理 ADS 落 `0` |
| 新视频 7 天 cohort 样本 | `published_7d_cohort_video_count` / `published_7d_cohort_view_count` | 视频 | 统计日 `dt` 回看 `dt-6` 首采正播放的 cohort 样本数，及这些视频在 `dt` 当天的 `day_end_views` 汇总（等价于首采正播放后第 7 天的累计播放）；单渠道按 `first_positive_view_dt = dt-6` 取样，`全部渠道` 按统一视频键全历史最早 `first_positive_view_dt` 对齐 cohort | 无 | 可跨天求和；不可混入 `全部渠道` 行 |
| 新视频 7 天平均播放数 | `published_7d_cohort_avg_view_count` | 视频 | `published_7d_cohort_view_count / published_7d_cohort_video_count` | 无 | 不可 `AVG`；跨天或跨渠道须先汇总分子分母再重算；无有效分母时物理 ADS 落 `0` |
| 发布时间 cohort 累计播放 | `published_day0_view_count` / `published_day1_view_count` / `published_day3_view_count` / `published_day7_view_count` / `published_day14_view_count` | 视频 | 以 `published_dt = dt` 的视频为 cohort，取其在 `T / T+1 / T+3 / T+7 / T+14` 观察日的**日末累计播放总和**，不是这几天内逐日增量求和；`全部渠道` 按统一视频全历史最早发布时间对齐 cohort | 无 | cohort 观察值，未成熟返回 `NULL`，不可当 0；不可跨 cohort 日期求和 |
| 截至前一日博主总数 | `creator_count_to_prev_day` | 博主 | 截至 `dt-1` 已发布过视频的历史去重博主数，按发布时间判断，不依赖是否已采到播放快照；YouTube 用 `channel_id`、TikTok 用 `user_id`、Ins 用 `account_id`，`全部渠道` 按 `channel + creator_id` 去重 | 无 | 日末累计 / 去重计数，不可跨天或跨渠道求和 |
| 前一天新增 views / 博主总数 | `daily_view_increment_per_creator` | 博主 | `daily_view_increment / creator_count_to_prev_day` | 无 | 不可 `AVG`；跨天或跨渠道须先汇总分子分母再重算；无有效分母时物理 ADS 落 `0` |
| 最新 3 条样本 | `latest_3_sample_creator_count` / `latest_3_sample_video_count` / `latest_3_sample_like_count` / `latest_3_sample_view_count` | 博主 | 先限定截至 `dt` 已有采集数据的视频，再按发布时间倒序取每个博主其中最新 3 条；不是先取发布时间最新 3 条再判断有无数据。`全部渠道` 仍按 `channel + creator_id` 分组 | 无 | 可跨天求和；不可混入 `全部渠道` 行 |
| 每个博主最新 3 条平均赞播率 | `latest_3_avg_like_view_rate` | 博主 | 样本池整体 `latest_3_sample_like_count / latest_3_sample_view_count`，不是各博主赞播率再平均 | 无 | 不可 `AVG`；跨天或跨渠道须先汇总分子分母再重算；无有效分母时物理 ADS 落 `0` |

### ads_daily_social_content_published_day3_view_bucket_v2（T+3 播放分桶）

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 渠道 | `channel` | 视频 | `全部渠道 / youtube / tiktok / ins` | 按目标渠道过滤 | `全部渠道` 与单渠道行不可一起聚合 |
| 分桶 | `published_day3_view_bucket` / `published_day3_view_bucket_order` | 视频 | `T+3` 日末累计播放的严格开闭区间分桶，`order` 固定 `1-7`，对应 `0`、`(0,100]`、`(100,500]`、`(500,1000]`、`(1000,3000]`、`(3000,10000]`、`(10000,+∞)` | 无 | 不聚合 |
| 分桶视频数 | `video_count` | 视频 | `published_dt = dt` 的成熟 cohort 在对应分桶中的视频数；单渠道按 `video_key`，`全部渠道` 先按 `unified_video_key` 聚合到单个逻辑视频再落桶，避免同一自动发布视频重复进多个桶 | 只产出已成熟 cohort，最新 3 个 cohort 日期不提前补零或用当前累计播放近似 | cohort 观察值，未成熟返回 `NULL`，不可当 0；不可跨 cohort 日期求和 |

`T+3` 分桶有两张图：`视频占比` 用 `video_count / SUM(video_count)`，`Views 贡献占比` 用虚拟 dataset `virtual_social_content_v2_published_day3_view_bucket_sorted` 额外输出的 `view_count`（对应 `dt + channel + bucket` 的 `T+3` 累计播放总和）计算 `view_count / SUM(view_count)`。

### 中间层 dws_social_video_snapshot_daily_v2 / dws_social_video_state_daily_v2

两张表字段相同，差别在于 `snapshot` 只在有真实采集的日期产出，`state` 对下游承诺「从 `published_dt` 起每天一行」的 dense daily 语义——当天没有采集时沿用前一天状态或窗口期初承接值。原始快照排序（`dt + video` 取最后一次采集）只在 `snapshot` 做一次，下游不再直接扫描 raw log。

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 日期 | `dt` | 自然日 | 按 `America/Toronto` 切天 | 无 | 不聚合 |
| 渠道 | `channel` | 视频 | 固定枚举 `youtube / tiktok / ins`（无 `全部渠道`） | 按目标渠道过滤 | 不聚合 |
| 博主 | `creator_id` | 博主 | YouTube 用 `channel_id`，TikTok 用 `user_id`，Ins 用 `account_id` | 无 | 不聚合 |
| 视频主键 | `video_key` | 视频 | 渠道前缀 + 平台视频主键，如 `youtube:{video_id}` | 无 | 不聚合 |
| 日末累计 views | `day_end_views` | 视频 | 每个视频每天取 `max(collected_at)` 对应的累计播放值 | 无 | 累计值，不可跨天求和 |
| 日末累计 likes | `day_end_likes` | 视频 | 每个视频每天取 `max(collected_at)` 对应的累计点赞值 | 无 | 累计值，不可跨天求和 |

### Superset 虚拟 dataset

不新增 BigQuery 物理表，只在 Superset 层派生：

| 虚拟 dataset | 粒度 | 关键字段与口径 |
|---|---|---|
| `virtual_social_content_v2_published_horizon_views`（id=264） | `dt × channel × horizon_label` | `horizon_label` 用不可见控制字符前缀稳定 `T / T+1 / T+3 / T+7 / T+14` 排序；`view_count` 为对应观察日的累计播放总和，`video_count` 为 cohort 视频数（真实渠道按 `video_key` 去重，`全部渠道` 按 `unified_video_key` 对齐全历史最早发布时间后去重）；未成熟 cohort 返回 `NULL`，展示时还会按 `America/Toronto` 当前自然日截断未走完的观察窗。平均播放由 chart 用 `SUM(view_count) / SUM(video_count)` 现算 |
| `virtual_social_content_v2_published_day3_view_bucket_sorted`（id=266） | `dt × channel × bucket` | 在物理分桶表基础上额外输出 `view_count`，供「Views 贡献占比」图使用 |
| `virtual_social_content_v2_weekly_creator_day3_view_bucket_sorted`（id=267） | `week_start_dt × channel × bucket` | 账号周均 `T+3 Views` 分桶。先取每条视频在 `T+3` 观察日的累计播放，再按「账号-自然周」汇总并除以该账号该周发布视频数（`COUNT(DISTINCT video_key)`，不是固定除以 7）得到账号周均值；`creator_week_count` 为桶内账号周样本数，`avg_daily_view_count` 为桶内账号周均值之和（字段名保持不变，语义已是篇均），`view_count` 为除以视频数之前的周内 `T+3` 播放总和（调试校验用）。只产出整周都满足 `T+3` 观察窗的成熟自然周，图表硬过滤 `week_start_dt >= '2026-05-01'`，实际首周为 `2026-05-04 ~ 2026-05-10`。`全部渠道` 下账号仍按 `channel + creator_id` 区分，不走 `unified_video_key`。两张图：`账号占比` 用 `creator_week_count / SUM(creator_week_count)`，`贡献占比` 用 `avg_daily_view_count / SUM(avg_daily_view_count)` |

`T+3` 视频分桶图与周维度账号分桶图的展示顺序统一从高桶到低桶：`(10000,+∞)`、`(3000,10000]`、`(1000,3000]`、`(500,1000]`、`(100,500]`、`(0,100]`、`0`。

### 未落地需求

`完播比` 与 `第一秒留存率` 需求已确认但业务口径未定，暂不落地，不进入当前 ADS 字段。

补充说明：

- 发布时间 cohort 播放图与「每天新增视频数」不是同一口径：前者按 `published_dt` 分 cohort，后者按首个 `views > 0` 的采集日统计新增视频
- 自动发布统一视频映射由 `decom.dim_auto_publish_video_task` 统一维护，不再由 Social Content 自己拼三端上传表
- Lark ECN 数据发布推送中的总量比例为：账号发视频率 = 发布总视频数 / 活跃运营账号数，视频发布成功率 = 发布成功总视频数 / 发布总视频数，社媒平台发布视频数读数成功率 = 新增视频数 / 发布总视频数
- Lark ECN 数据发布推送的主指标表使用原生 table 组件分页展示：第 1 页展示核心总量和比例，第 2 页展示 API / RPA 明细；比例不保留小数，当日比例用黄色斜体突出

## 依赖

- 上游：`videos.yt_videos`, `videos.yt_video_stats_log`, `videos.videos`, `videos.video_stats_log`, `videos.ig_media`, `videos.ig_media_stats_log`, `videos.video_publish_daily_stats`
- 中间表：`decom.dim_video`, `decom.dim_auto_publish_video_task`, `decom.dws_social_video_snapshot_daily_v2`, `decom.dws_social_video_state_daily_v2`, `decom.dws_social_channel_daily_v2`
- 报表表：`decom.ads_daily_social_content_v2`, `decom.ads_daily_social_content_published_day3_view_bucket_v2`, `decom.ads_daily_thirdapp_channel_uv`, `decom.ads_daily_investor`
- 编排：`env/etl_config.json`, `etl_run.py`
- 脚本：`jobs/dws/dws_social_video_snapshot_daily_v2.py`, `jobs/dws/dws_social_video_state_daily_v2.py`, `jobs/dws/dws_social_channel_daily_v2.py`, `jobs/ads/ads_daily_social_content_v2.py`, `jobs/ads/ads_daily_social_content_published_day3_view_bucket_v2.py`, `jobs/ads/ads_daily_ecn_lark_push.py`
- DDL：`ddl/dws/dws_social_video_snapshot_daily_v2.sql`, `ddl/dws/dws_social_video_state_daily_v2.sql`, `ddl/dws/dws_social_channel_daily_v2.sql`, `ddl/ads/ads_daily_social_content_v2.sql`, `ddl/ads/ads_daily_social_content_published_day3_view_bucket_v2.sql`
- Superset：物理 dataset `ads_daily_social_content_v2`（id=262），virtual dataset `virtual_social_content_v2_published_horizon_views`（id=264）、`virtual_social_content_v2_published_day3_view_bucket_sorted`（id=266）、`virtual_social_content_v2_weekly_creator_day3_view_bucket_sorted`（id=267），dashboard `【Social Content】社媒表现数据看板v2`（id=116）；chart id `637 / 638 / 639 / 640 / 641 / 642 / 643 / 664 / 665 / 666 / 667 / 668`
