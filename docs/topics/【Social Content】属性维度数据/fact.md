# 【Social Content】属性维度数据

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 指标 | 日增 Views / Likes | 日增播放量、日增点赞数 |
| 指标 | 新视频 7 天平均播放数 | 新视频 7 日平均 Views、T+7 平均播放量 |
| 维度 | 博主属性 | 账号属性 |

## 适用范围

- 业务线：`ECN 视频`, `TikTok 橱窗号`
- 适合：按视频任务属性和博主属性分析社媒视频新增播放、点赞、新视频及 cohort 表现。
- 不适合：不拆属性的渠道总览应使用[Social Content 社媒表现](../【Social Content】社媒表现数据看板/fact.md)。
- 需要澄清：需要确认视频或博主视角、属性维度、日期范围和 cohort 周期。

## 报表

### ads_daily_social_content_attribute_v2

- 表：`my-project-8584-jetonai.decom.ads_daily_social_content_attribute_v2`
- 状态：`active`
- 粒度：一行代表一个 `dt × 视频属性组合`
- 时区：`America/Toronto`
- 数据范围：能映射到 `dim_video` 且包含 `video_task` 属性的视频和博主表现。
- 必要过滤：按目标属性维度过滤。
- 不包含：无法映射或缺少 `video_task` 属性的视频。

- Dashboard：[【Social Content】属性维度数据](https://bi.alvinclub.ca/superset/dashboard/social-content-attribute-data/)（id=124）
- Dashboard 主图表使用物理 dataset `ads_daily_social_content_attribute_v2`（id=277）；cohort 图使用只对物理 ADS 做轻量展开的 virtual dataset（id=276）
- 统计范围：只统计 `videos.video_upload_tasks.ext_info` 顶层存在 `video_task` key，且能通过 `video_upload_task_channels.platform_video_id` 映射到 `decom.dim_video.video_id` 的视频
- 数据起点：由数据源动态决定（`dws_social_video_state_daily_v2` 与 `dim_social_video_attribute` join 后的 `MIN(dt)`），不在脚本里硬编码日期；上游回补历史 `video_task` 后起点会自动向前扩展
- 首次生产回填范围：`2026-07-15 ~ 2026-07-16`
- 视频类图表：每日新增 Views、每日新增 Likes、每天新增视频数、新增视频平均播放数、新视频 7 天平均播放数、发布时间 cohort 平均播放折线图
- 博主类图表：前一天新增 Views / 博主总数、每个博主最新 3 条平均赞播率

## 字段

日期按 `America/Toronto` 切天；图表不展示当前未走完的自然日。属性映射落在 `decom.dim_social_video_attribute`（`video_key` / `task_id` / `video_attribute` / `video_brand` / `blogger_attribute`），分子分母落在 `decom.dws_social_attribute_daily_v2`。

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 渠道 | `channel` | 视频 | `全部渠道 / youtube / tiktok / ins`；`全部渠道` 的自动发布视频按 `task_id` 统一去重 | 按目标渠道过滤 | `全部渠道` 与单渠道行不可一起聚合 |
| 指标范围 | `metric_scope` | 指标 | 区分该行属于视频类还是博主类指标；视频属性 / 视频品牌只影响视频类，博主属性只影响博主类 | 按目标范围过滤 | 不同 `metric_scope` 行不可一起聚合 |
| 视频属性 | `video_attribute` | 视频 | `ext_info.production_type` 映射：`normal -> 常规`、`brand -> CPM`、`brand_cps -> CPS` | 仅对视频类指标有效 | 不聚合 |
| 视频品牌 | `video_brand` | 视频 | `ext_info.video_task.video_brand_group.name`，空值展示为 `未设置` | 仅对视频类指标有效 | 不聚合 |
| 博主属性 | `blogger_attribute` | 博主 | `ext_info.video_task.blogger_type` 映射：`normal -> 常规`、`cpm -> CPM`、`cps -> CPS` | 仅对博主类指标有效 | 不聚合 |
| 日增 Views | `daily_view_increment` | 视频 | 视频日末累计播放减前一日日末累计，再按日期、渠道与属性汇总 | 无 | 可跨天求和（同一渠道与属性组合内）；不可混入 `全部渠道` 行 |
| 日增 Likes | `daily_like_increment` | 视频 | 视频日末累计点赞减前一日日末累计，再按日期、渠道与属性汇总 | 无 | 可跨天求和（同一渠道与属性组合内）；不可混入 `全部渠道` 行 |
| 每天新增视频数 | `new_video_count` | 视频 | `day_end_views > 0` 且前一日 `day_end_views = 0` 的视频数 | 无 | 可跨天求和（同一渠道与属性组合内）；不可混入 `全部渠道` 行 |
| 新增视频贡献播放 | `new_video_contributed_view_count` | 视频 | 新增视频首次变成正播放当天的 `day_end_views` 之和，是平均播放数的分子 | 无 | 可跨天求和（同一渠道与属性组合内）；不可混入 `全部渠道` 行 |
| 新增视频平均播放数 | `avg_new_video_view_count` | 视频 | `SUM(new_video_contributed_view_count) / SUM(new_video_count)`；无有效分母时返回 `NULL` | 无 | 不可 `AVG`；跨天、跨渠道或跨属性须先汇总分子分母再重算 |
| 新视频 7 天 cohort 样本 | `published_7d_cohort_video_count` / `published_7d_cohort_view_count` | 视频 | 统计日 `dt` 回看 `dt-6` 首采正播放 cohort 的样本数与 7 天累计播放数 | 无 | 可跨天求和（同一渠道与属性组合内）；不可混入 `全部渠道` 行 |
| 新视频 7 天平均播放数 | `published_7d_cohort_avg_view_count` | 视频 | `SUM(published_7d_cohort_view_count) / SUM(published_7d_cohort_video_count)` | 无 | 不可 `AVG`；跨天、跨渠道或跨属性须先汇总分子分母再重算 |
| 发布时间 cohort 样本 | `published_day0_*` / `published_day1_*` / `published_day3_*` / `published_day7_*` / `published_day14_*`（各含 `_video_count` 与 `_view_count`） | 视频 | 以 `published_dt = dt` 的视频为 cohort，取其在 `T / T+1 / T+3 / T+7 / T+14` 观察日的日末累计播放总和与视频数；未成熟 cohort 返回 `NULL` | 无 | 可跨天求和（同一渠道与属性组合内）；不可混入 `全部渠道` 行；未成熟 cohort 的 `NULL` 不可当 0 |
| 发布时间 cohort 平均播放 | 未落地，按 `SUM(view_count) / SUM(video_count)` 现算 | 视频 | 对应观察日的平均播放，由 Superset 虚拟 dataset `virtual_social_content_attribute_published_horizon_views_v2`（id=276）按 `dt / channel / video_attribute / video_brand / horizon_label` 输出 | 无 | 不可 `AVG`；跨天、跨渠道或跨属性须先汇总分子分母再重算 |
| 截至前一日博主总数 | `creator_count_to_prev_day` | 博主 | 截至 `dt-1` 已发布过视频的历史去重博主数 | 无 | 累计值，不可跨天求和 |
| 前一天新增 Views / 博主总数 | `daily_view_increment_per_creator` | 博主 | `SUM(daily_view_increment) / SUM(creator_count_to_prev_day)` | 无 | 不可 `AVG`；跨天、跨渠道或跨属性须先汇总分子分母再重算 |
| 最新 3 条样本 | `latest_3_sample_creator_count` / `latest_3_sample_video_count` / `latest_3_sample_like_count` / `latest_3_sample_view_count` | 博主 | 在博主属性范围内，取每个博主截至 `dt` 有正播放的最新 3 条视频，汇总样本博主数、视频数、点赞与播放 | 无 | 可跨天求和（同一渠道与属性组合内）；不可混入 `全部渠道` 行 |
| 每个博主最新 3 条平均赞播率 | `latest_3_avg_like_view_rate` | 博主 | `SUM(latest_3_sample_like_count) / SUM(latest_3_sample_view_count)` | 无 | 不可 `AVG`；跨天、跨渠道或跨属性须先汇总分子分母再重算 |

平均值与比率在分母为 0 时返回 `NULL`；`NULL` 表示无有效样本，不表示指标为 0。

## 依赖

- 上游任务：`videos.video_upload_task_channels`、`videos.video_upload_tasks`
- 视频维表：`decom.dim_video`
- 视频日状态：`decom.dws_social_video_state_daily_v2`
- 参考口径：`docs/topics/【Social Content】社媒表现数据看板/fact.md`
- 脚本：`jobs/dim/dim_social_video_attribute.py`、`jobs/dws/dws_social_attribute_daily_v2.py`、`jobs/ads/ads_daily_social_content_attribute_v2.py`
- DDL：`ddl/dim/dim_social_video_attribute.sql`、`ddl/dws/dws_social_attribute_daily_v2.sql`、`ddl/ads/ads_daily_social_content_attribute_v2.sql`
- 编排：`env/etl_config.json` 中的独立表级任务 `dim_social_video_attribute -> dws_social_attribute_daily_v2 -> ads_daily_social_content_attribute_v2`
- Superset：物理 dataset id `277`、cohort virtual dataset id `276`，chart id `687 / 688 / 689 / 690 / 691 / 692 / 693 / 694`，dashboard id `124`
- 性能：DIM 每次只解析一次 `video_task` 任务 JSON；DWS 只读属性映射后的视频日分区；Superset 主图表只读物理 ADS，dataset / chart 缓存时间为 1 小时
