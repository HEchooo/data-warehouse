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

| 需求字段 | 定义 |
|------|------|
| 日期 | 按 `America/Toronto` 切天；图表不展示当前未走完的自然日 |
| 渠道 | `全部渠道 / youtube / tiktok / ins`；`全部渠道` 的自动发布视频按 `task_id` 统一去重 |
| 视频属性 | 仅影响视频类图表；`ext_info.production_type` 映射为 `normal -> 常规`、`brand -> CPM`、`brand_cps -> CPS` |
| 视频品牌 | 仅影响视频类图表；取 `ext_info.video_task.video_brand_group.name`，空值展示为 `未设置` |
| 博主属性 | 仅影响博主类图表；`ext_info.video_task.blogger_type` 映射为 `normal -> 常规`、`cpm -> CPM`、`cps -> CPS` |
| 日增 Views / Likes | 视频日末累计值减前一日日末累计值，再按日期、渠道与属性汇总 |
| 每天新增视频数 | `day_end_views > 0` 且前一日 `day_end_views = 0` 的视频数 |
| 新增视频平均播放数 | `SUM(new_video_contributed_view_count) / SUM(new_video_count)` |
| 新视频 7 天平均播放数 | `SUM(published_7d_cohort_view_count) / SUM(published_7d_cohort_video_count)` |
| 发布时间 cohort 平均播放 | 按 `T / T+1 / T+3 / T+7 / T+14` 分别使用 `SUM(view_count) / SUM(video_count)` |
| 前一天新增 Views / 博主总数 | `SUM(daily_view_increment) / SUM(creator_count_to_prev_day)` |
| 每个博主最新 3 条平均赞播率 | 在博主属性范围内取每个博主截至 `dt` 有正播放的最新 3 条，按 `SUM(likes) / SUM(views)` 计算 |

## 已落地字段

| 落地位置 | 主要字段 |
|------|------|
| Superset virtual dataset `virtual_social_content_attribute_published_horizon_views_v2`（id=276） | `dt`、`channel`、`video_attribute`、`video_brand`、`horizon_label`、`view_count`、`video_count` |
| `decom.dim_social_video_attribute` | `video_key`、`task_id`、`video_attribute`、`video_brand`、`blogger_attribute` |
| `decom.dws_social_attribute_daily_v2` | `metric_scope`、属性维度与各指标分子分母 |
| `decom.ads_daily_social_content_attribute_v2` | 报表粒度字段与平均值 / 比率轻派生 |

补充说明：

- 视频平均值和赞播率都必须用表内对应的总播放、点赞和样本数分子分母重算，不能跨日期、渠道或属性直接平均
- 分母为 0 时平均值与比率返回 `NULL`；`NULL` 表示无有效样本，不表示指标为 0
- `全部渠道` 已按统一视频键去重，不能与单渠道行一起聚合

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
