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

### v2 并行链路

- 目标看板：`【Social Content】社媒表现数据看板v2`
- 最终表：`decom.ads_daily_social_content_v2`
- 粒度：`dt + channel`
- 分布表：`decom.ads_daily_social_content_published_day3_view_bucket_v2`
- 分布表粒度：`dt + channel + published_day3_view_bucket`
- 展示起点：`2026-05-01`
- Dashboard：[【Social Content】社媒表现数据看板v2](https://bi.alvinclub.ca/superset/dashboard/116/)（id=116）
- v1 链路保持不变；v2 从快照层起使用独立表：
  - `decom.dws_social_video_snapshot_daily_v2`
  - `decom.dws_social_video_state_daily_v2`
  - `decom.dws_social_channel_daily_v2`
  - `decom.ads_daily_social_content_v2`
  - `decom.ads_daily_social_content_published_day3_view_bucket_v2`
- v2 复用 `decom.dim_video` 与 `decom.dim_auto_publish_video_task`
- v2 的 YouTube 累计快照改用 `videos.yt_video_stats_log.view_count / like_count`
- v2 渠道聚合层排除与投资人报表一致的 10 个 YouTube 测试 / 内部频道；该报表不按 `decom.dim_video.is_formal_video` 做例外保留
- TikTok、Instagram 数据源及7项指标口径与 v1 保持一致
- v2 内部历史从 `2026-03-13` 开始构建，以支持 `2026-05-01` 起的日增量、最新3条和7天 cohort 计算
- 首次生产回填范围：`2026-03-13 ~ 2026-06-12`
- 首次回填完成后，v2 独立编排任务已启用

## 字段

| 需求字段 | 定义 |
|------|------|
| 日期 | 报表日期，按 `America/Toronto` 切天 |
| 渠道 | 主表直接产出 `全部渠道 / youtube / tiktok / ins` 4 个展示渠道值 |
| 每日新增 views | 每个视频每天取 `max(collected_at)` 对应的累计播放值，与前一日日末累计播放值做差后按渠道汇总 |
| 每日新增 likes | 每个视频每天取 `max(collected_at)` 对应的累计点赞值，与前一日日末累计点赞值做差后按渠道汇总 |
| 每天新增视频数 | 某视频第一次被采集到且播放量大于 0 的那一天计为新增，后续日期不重复计数；单渠道按原始视频汇总，`全部渠道` 在自动发布场景下按统一任务去重 |
| 新增视频平均播放数 | 当天新增视频首次被采到正播放量时的累计播放数之和 / 当天新增视频数 |
| 新视频7天平均播放数 | 统计日 `dt` 回看 `dt-6` 首次采到正播放（`view>0`）的视频 cohort，取这些视频在 7 天后的累计播放数均值；单渠道按原始视频统计，`全部渠道` 按统一视频首个正播放日对齐 cohort |
| 当天发布新视频在发布当天（T）的总播放数据 | 以 `published_dt = dt` 的视频作为 cohort，统计这些视频在发布当天日末的累计播放总和；单渠道按原始视频统计，`全部渠道` 按统一视频全历史最早发布时间对齐 cohort |
| 当天发布新视频在发布后1天（T+1）的总播放数据 | 以 `published_dt = dt` 的视频作为 cohort，统计这些视频在发布后第 1 天日末的累计播放总和；未满 `T+1` 的近期 cohort 返回空值 |
| 当天发布新视频在发布后3天（T+3）的总播放数据 | 以 `published_dt = dt` 的视频作为 cohort，统计这些视频在发布后第 3 天日末的累计播放总和；未满 `T+3` 的近期 cohort 返回空值 |
| 3天全量 view count 分桶视频数量占比 | 以 `published_dt = dt` 的视频作为 cohort，取这些视频在发布后第 3 天日末的累计播放值做严格开闭区间分桶：`0`、`(0,100]`、`(100,500]`、`(500,1000]`、`(1000,3000]`、`(3000,10000]`、`(10000,+∞)`；单渠道按原始视频统计，`全部渠道` 按统一视频全历史最早发布时间对齐 cohort，并把同一统一视频在 `T+3` 的各平台累计播放汇总后落到唯一分桶 |
| 账号周均 T+3 Views 分桶账号占比 | 以自然周内每天 `published_dt` 的视频为样本，先取每条视频在 `T+3` 观察日的累计播放值，再按 `账号-自然周` 汇总该周发布视频的 `T+3` 播放总和，并固定除以 `7` 得到账号周均 `T+3 Views`；然后按同一套分桶：`0`、`(0,100]`、`(100,500]`、`(500,1000]`、`(1000,3000]`、`(3000,10000]`、`(10000,+∞)` 统计账号样本占比；`全部渠道` 按 `channel + creator_id` 区分账号，不做跨平台账号合并 |
| 账号周均 T+3 Views 分桶贡献占比 | 与上一条使用同一批 `账号-自然周` 样本和同一分桶；贡献分子为各 bucket 内账号周均 `T+3 Views` 之和，分母为当周全量账号周均 `T+3 Views` 之和 |
| 当天发布新视频在发布后7天（T+7）的总播放数据 | 以 `published_dt = dt` 的视频作为 cohort，统计这些视频在发布后第 7 天日末的累计播放总和；未满 `T+7` 的近期 cohort 返回空值 |
| 当天发布新视频在发布后14天（T+14）的总播放数据 | 以 `published_dt = dt` 的视频作为 cohort，统计这些视频在发布后第 14 天日末的累计播放总和；未满 `T+14` 的近期 cohort 返回空值 |
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
| 新视频7天平均播放数 | `decom.ads_daily_social_content` | `published_7d_cohort_avg_view_count` | `published_7d_cohort_view_count / published_7d_cohort_video_count`；其中 `published_7d_cohort_video_count` / `published_7d_cohort_view_count` 来自 `decom.dws_social_channel_daily`，表示统计日 `dt` 回看 `dt-6` 首采正播放 cohort 的样本数与 7 天累计播放数 |
| 前一天新增 views / 博主总数 | `decom.ads_daily_social_content` | `daily_view_increment_per_creator` | 分子为 `daily_view_increment`；分母为 `creator_count_to_prev_day`，即截至 `dt-1` 已发布过视频的历史去重博主数；`全部渠道` 用 `channel + creator_id` 去重 |
| 每个博主最新 3 条的平均赞播率 | `decom.ads_daily_social_content` | `latest_3_avg_like_view_rate` | 先限定截至 `dt` 已有采集数据的视频，再按发布时间倒序取每个博主其中最新 3 条；汇总样本池的 `latest_3_sample_like_count / latest_3_sample_view_count`；`全部渠道` 仍按 `channel + creator_id` 分组取各博主最新 3 条 |

### Superset 虚拟 dataset：`virtual_social_content_v2_published_horizon_views`

| 需求字段 | 落地位置 | 落地字段 | 口径 |
|------|------|------|------|
| 日期 | Superset 虚拟 dataset `virtual_social_content_v2_published_horizon_views` | `dt` | 发布时间 cohort 日期，直接复用 `decom.ads_daily_social_content_v2.dt` |
| 渠道 | Superset 虚拟 dataset `virtual_social_content_v2_published_horizon_views` | `channel` | 固定枚举 `全部渠道 / youtube / tiktok / ins` |
| 观察窗标签 | Superset 虚拟 dataset `virtual_social_content_v2_published_horizon_views` | `horizon_label` | 使用不可见控制字符前缀稳定 `T / T+1 / T+3 / T+7 / T+14` 的排序 |
| 累计播放总和 | Superset 虚拟 dataset `virtual_social_content_v2_published_horizon_views` | `view_count` | 对应发布时间 cohort 在 `T / T+1 / T+3 / T+7 / T+14` 观察日的累计播放总和；未成熟 cohort 返回 `NULL` |
| 发布时间 cohort 视频数 | Superset 虚拟 dataset `virtual_social_content_v2_published_horizon_views` | `video_count` | 对应发布时间 cohort 的发布视频数；真实渠道按 `video_key` 去重，`全部渠道` 按 `unified_video_key` 对齐全历史最早发布时间后去重；未成熟 cohort 返回 `NULL` |

### 分布报表：`decom.ads_daily_social_content_published_day3_view_bucket_v2`

| 需求字段 | 落地表 | 落地字段 | 口径 |
|------|------|------|------|
| 渠道 | `decom.ads_daily_social_content_published_day3_view_bucket_v2` | `channel` | 固定枚举 `全部渠道 / youtube / tiktok / ins` |
| 3天全量 view count 分桶排序 | `decom.ads_daily_social_content_published_day3_view_bucket_v2` | `published_day3_view_bucket_order` | 固定为 `1-7`，对应 `0`、`(0,100]`、`(100,500]`、`(500,1000]`、`(1000,3000]`、`(3000,10000]`、`(10000,+∞)` |
| 3天全量 view count 分桶名称 | `decom.ads_daily_social_content_published_day3_view_bucket_v2` | `published_day3_view_bucket` | 展示层直接使用的 `T+3` 累计播放分桶标签 |
| 分桶视频数量 | `decom.ads_daily_social_content_published_day3_view_bucket_v2` | `video_count` | `published_dt = dt` 的成熟 cohort 在对应 `T+3` 累计播放分桶中的视频数；单渠道按 `video_key` 统计，`全部渠道` 按 `unified_video_key` 统计 |

补充说明：
- Superset 虚拟 dataset `virtual_social_content_v2_published_day3_view_bucket_sorted` 额外输出 `view_count`，表示对应 `dt + channel + bucket` 下视频在 `T+3` 观察日的累计播放总和，供“发布后3天全量 Views 分桶贡献占比”图使用

### Superset 周分布虚拟 dataset：`virtual_social_content_v2_weekly_creator_day3_view_bucket_sorted`

| 需求字段 | 落地位置 | 落地字段 | 口径 |
|------|------|------|------|
| 自然周起始日 | Superset 虚拟 dataset `virtual_social_content_v2_weekly_creator_day3_view_bucket_sorted` | `week_start_dt` | 按 `WEEK(MONDAY)` 对 `published_dt` 截断后的周起始日；只产出“整周都已满足 T+3 观察窗”的成熟自然周 |
| 自然周结束日 | Superset 虚拟 dataset `virtual_social_content_v2_weekly_creator_day3_view_bucket_sorted` | `week_end_dt` | `week_start_dt + 6 day` |
| 自然周标签 | Superset 虚拟 dataset `virtual_social_content_v2_weekly_creator_day3_view_bucket_sorted` | `week_label` | `YYYY.MM.DD - YYYY.MM.DD` |
| 渠道 | Superset 虚拟 dataset `virtual_social_content_v2_weekly_creator_day3_view_bucket_sorted` | `channel` | 固定枚举 `全部渠道 / youtube / tiktok / ins` |
| 账号周均 T+3 Views 分桶排序 | Superset 虚拟 dataset `virtual_social_content_v2_weekly_creator_day3_view_bucket_sorted` | `published_day3_view_bucket_order` | 固定为 `1-7`，对应 `0`、`(0,100]`、`(100,500]`、`(500,1000]`、`(1000,3000]`、`(3000,10000]`、`(10000,+∞)` |
| 账号周均 T+3 Views 分桶名称 | Superset 虚拟 dataset `virtual_social_content_v2_weekly_creator_day3_view_bucket_sorted` | `published_day3_view_bucket` | 展示层直接使用的账号周均 `T+3 Views` 分桶标签 |
| 账号周均 T+3 Views 分桶排序标签 | Superset 虚拟 dataset `virtual_social_content_v2_weekly_creator_day3_view_bucket_sorted` | `published_day3_view_bucket_sort_label` | 用不可见控制字符前缀保证 Superset legend / tooltip / 堆积顺序稳定 |
| 分桶账号样本数 | Superset 虚拟 dataset `virtual_social_content_v2_weekly_creator_day3_view_bucket_sorted` | `creator_week_count` | 每个 `week_start_dt + channel + bucket` 下的 `账号-自然周` 样本数 |
| 分桶账号周均 T+3 Views 总和 | Superset 虚拟 dataset `virtual_social_content_v2_weekly_creator_day3_view_bucket_sorted` | `avg_daily_view_count` | 每个账号先按“该周发布视频的 T+3 播放总和 / 7”得到账号周均值，再按 bucket 汇总求和 |
| 分桶账号周内 T+3 Views 总和 | Superset 虚拟 dataset `virtual_social_content_v2_weekly_creator_day3_view_bucket_sorted` | `view_count` | 每个账号周样本在除以 `7` 之前的周内 `T+3 Views` 总和；当前主要用于调试与校验 |

补充说明：
- 当前周维度账号分桶只落在 Superset 虚拟 dataset，不新增 BigQuery 物理 ADS 表；原因是本次需求未要求生产回填
- 周维度图表仍复用现有 dashboard `channel` filter；`全部渠道` 下按 `channel + creator_id` 区分账号，不走 `unified_video_key`
- 图表硬过滤 `week_start_dt >= '2026-05-01'`，实际首周为 `2026-05-04 ~ 2026-05-10`，对应 v2 展示起点后的第一个完整自然周
- YouTube 统一按 `docs/rules/video-mapping.md` 排除 AI 团队渠道，即 `video 部门测试渠道 + video 部门内部渠道`
- `views / likes` 源字段都是累计值，不是日增值
- 若某视频在 `dt` 没有新采集记录，则其 `dt` 日末累计值沿用最近一次已采集值，因此 `dt` 的日增量记为 `0`
- 原始快照排序（`dt+video` 取最后采集）只在 `dws_social_video_snapshot_daily` 做一次，`state/channel/ads` 不再直接扫描 raw log
- `decom.dws_social_video_state_daily` 对下游承诺“从 `published_dt` 起每天一行”的 dense daily 语义，主口径只保留 `day_end_views / day_end_likes`
- “每天新增视频数”在渠道层直接按状态表派生：`day_end_views > 0 AND LAG(day_end_views, 1, 0) = 0`
- 因为 dashboard 支持 `channel` filter，自动发布跨平台去重只作用于 `channel = 全部渠道`；筛到单渠道时仍保留各平台原始视频数量口径
- 自动发布统一视频映射当前由 `decom.dim_auto_publish_video_task` 统一维护，不再由 Social Content 自己重复拼三端上传表
- “新增视频平均播放数”的分子不是当天全量视频播放增量，而是新增视频在首次变成正播放当天的 `day_end_views` 之和
- “新视频7天平均播放数”当前按首采正播放 cohort 统计：单渠道按 `first_positive_view_dt = dt-6` 取样；`全部渠道` 则按统一视频键全历史最早 `first_positive_view_dt` 对齐 cohort 日期
- “新视频7天平均播放数”的分子使用 `dt` 当天该 cohort 视频的 `day_end_views` 汇总，等价于这些视频“首采正播放后第 7 天”的累计播放数；若中间某天无新采集，则仍沿用最近一次已知累计值
- 发布时间 cohort 播放图与“每天新增视频数”不是同一口径：前者按 `published_dt` 分 cohort，后者按首个 `views > 0` 的采集日统计新增视频
- 发布时间 cohort 播放图的 `T / T+1 / T+3 / T+7 / T+14` 都表示对应观察日的日末累计播放，不是这几天内逐日播放增量求和
- dashboard 图表 `Social Content v2（日表）-发布时间 cohort 平均播放折线图` 在 Superset 层使用 `SUM(view_count) / SUM(video_count)` 展示平均播放量；底层 `view_count` 仍保留 cohort 总播放，供其他图表复用
- 发布时间 cohort 播放图里未成熟的近期 cohort 直接返回空值，不用当前累计播放去近似 `T+1 / T+3 / T+7 / T+14`
- 发布时间 cohort 播放图展示时还会按 `America/Toronto` 当前自然日动态截断未走完整天的观察窗；例如在 `2026-06-25` 查看 `2026-06-24` cohort 时，只展示 `T`，不展示 `T+1`
- `T+3` 播放分桶图只产出已成熟 cohort；最新 3 个 cohort 日期不会提前补零或用当前累计播放近似
- `T+3` 播放分桶图在 `全部渠道` 下先按统一视频键聚合到单个逻辑视频，再按聚合后的累计播放值落桶，避免同一自动发布视频重复进多个桶
- `T+3` 播放分桶图当前有两张：`视频占比` 以 `video_count / SUM(video_count)` 计算；`Views 贡献占比` 以 `view_count / SUM(view_count)` 计算
- 周维度账号分桶图当前也有两张：`账号占比` 以 `creator_week_count / SUM(creator_week_count)` 计算；`贡献占比` 以 `avg_daily_view_count / SUM(avg_daily_view_count)` 计算
- `T+3` 视频分桶图与周维度账号分桶图的展示顺序统一按高桶到低桶排列：`(10000,+∞)`、`(3000,10000]`、`(1000,3000]`、`(500,1000]`、`(100,500]`、`(0,100]`、`0`
- 周维度账号分桶图的分母固定为 `7`，不是“该账号该周有发视频的天数”；因此只展示完整成熟自然周，不展示半周样本
- `jobs/dws/dws_social_video_state_daily.py` 直接维护视频日状态层，再由渠道层按需做 `LAG` 和新增视频派生
- “博主总数”按发布时间判断，不依赖是否已经采集到播放快照
- “最新 3 条”不是先取发布时间最新 3 条再判断有没有数据，而是先限定截至 `dt` 已有采集数据的视频，再从这些视频里按发布时间倒序取最新 3 条
- `全部渠道` 不是额外视图字段，而是直接写入主表的展示渠道值
- 看板当前落地 7 个指标；`完播比`、`第一秒留存率` 先保留在需求范围内，不进入当前 ADS 字段
- v2 看板沿用上述7个指标；相对 v1，YouTube 累计快照使用 `yt_video_stats_log`，且渠道聚合层按投资人报表同款 10 个 YouTube 排除频道收敛视频范围

## 依赖

- 上游：`videos.yt_videos`, `videos.yt_video_analytics_log`, `videos.videos`, `videos.video_stats_log`, `videos.ig_media`, `videos.ig_media_stats_log`
- 中间表：`decom.dim_video`, `decom.dim_auto_publish_video_task`, `decom.dws_social_video_snapshot_daily`, `decom.dws_social_video_state_daily`, `decom.dws_social_channel_daily`
- 报表表：`decom.ads_daily_social_content`
- Superset：主物理 dataset `ads_daily_social_content`（id=230），chart `Social Content（日表）-每日新增 Views 趋势`（id=575）、`Social Content（日表）-每日新增 Likes 趋势`（id=576）、`Social Content（日表）-前一天新增 Views / 博主总数趋势`（id=577）、`Social Content（日表）-每个博主最新 3 条平均赞播率趋势`（id=578）、`Social Content（日表）-每天新增视频数趋势`（id=579）、`Social Content（日表）-新增视频平均播放数趋势`（id=580）、`Social Content（日表）-新视频7天平均播放数趋势`（id=586），dashboard `【Social Content】社媒表现数据看板`（id=102）
- 脚本：`jobs/dim/dim_video.py`, `jobs/dim/dim_auto_publish_video_task.py`, `jobs/dws/dws_social_video_snapshot_daily.py`, `jobs/dws/dws_social_video_state_daily.py`, `jobs/dws/dws_social_channel_daily.py`, `jobs/ads/ads_daily_social_content.py`
- DDL：`ddl/dim/dim_video.sql`, `ddl/dim/dim_auto_publish_video_task.sql`, `ddl/dws/dws_social_video_snapshot_daily.sql`, `ddl/dws/dws_social_video_state_daily.sql`, `ddl/dws/dws_social_channel_daily.sql`, `ddl/ads/ads_daily_social_content.sql`
- 编排：`env/etl_config.json`, `etl_run.py`
- v2 脚本：`jobs/dws/dws_social_video_snapshot_daily_v2.py`, `jobs/dws/dws_social_video_state_daily_v2.py`, `jobs/dws/dws_social_channel_daily_v2.py`, `jobs/ads/ads_daily_social_content_v2.py`, `jobs/ads/ads_daily_social_content_published_day3_view_bucket_v2.py`
- v2 DDL：`ddl/dws/dws_social_video_snapshot_daily_v2.sql`, `ddl/dws/dws_social_video_state_daily_v2.sql`, `ddl/dws/dws_social_channel_daily_v2.sql`, `ddl/ads/ads_daily_social_content_v2.sql`, `ddl/ads/ads_daily_social_content_published_day3_view_bucket_v2.sql`
- v2 Superset：物理 dataset `ads_daily_social_content_v2`（id=262），virtual dataset `virtual_social_content_v2_published_horizon_views`（id=264）、`virtual_social_content_v2_published_day3_view_bucket_sorted`（id=266）、`virtual_social_content_v2_weekly_creator_day3_view_bucket_sorted`（id=267），dashboard `【Social Content】社媒表现数据看板v2`（id=116），copy dashboard `【Social Content】社媒表现数据看板v2 copy`（id=119）；chart id `637 / 638 / 639 / 640 / 641 / 642 / 643 / 664 / 665 / 666 / 667 / 668`
