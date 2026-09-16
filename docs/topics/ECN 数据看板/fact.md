# ECN 数据看板

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | ECN 数据看板 | AI-Fashion 账号发布数据增量看板 |
| 指标 | 全部已发布视频新增总播放量 | 新增 Views、播放增量、日增播放数 |
| 指标 | 全部已发布视频新增总点赞数 | 新增 Likes、点赞增量 |
| 指标 | 当日新增发布视频数 | 新增视频数、新发布视频数 |
| 指标 | 平均 LTV | 7 日 LTV、LTV 7 日播放量 |

## 适用范围

- 业务线：`ECN 视频`
- 适合：查询正式号视频的日增播放、点赞、发布量及固定周窗口增长。
- 不适合：全量社媒渠道表现应使用[Social Content 社媒表现](../【Social Content】社媒表现数据看板/fact.md)；单视频排行应使用[每日排行榜](../【Social Content】每日排行榜/fact.md)。
- 需要澄清：需要确认日指标或周指标、日期范围及指标是增量还是累计值。

## 报表

### 最终报表：`ads_daily_formal_video_increment`

- 表：`my-project-8584-jetonai.decom.ads_daily_formal_video_increment`
- 状态：`active`
- 粒度：`dt`
- 时区：`America/Toronto`
- 数据范围：正式号视频日增播放、点赞和发布指标。
- 必要过滤：无。
- 不包含：非正式号和视频明细行。
- 时间范围：美东日期 `2026-03-30` 至当天
- 作用：承接正式号视频日增量指标，服务图 1-5
- 调度：独立表级任务 `jobs/ads/ads_daily_formal_video_increment.py`
- Superset：物理 dataset `ads_daily_formal_video_increment`（id=240）

### 最终报表：`ads_weekly_formal_video_increment_growth`

- 表：`my-project-8584-jetonai.decom.ads_weekly_formal_video_increment_growth`
- 状态：`active`
- 粒度：`week_index`
- 时区：`America/Toronto`
- 数据范围：正式号视频固定 7 日窗口的周均新增播放和增长倍数。
- 必要过滤：无。
- 不包含：自然周之外的任意滚动窗口。
- 周窗口：从美东日期 `2026-04-13` 起，每 7 天一个窗口
- 作用：承接正式号视频周均新增 views 与增长倍数，服务图 6-9
- 调度：独立表级任务 `jobs/ads/ads_weekly_formal_video_increment_growth.py`
- Superset：物理 dataset `ads_weekly_formal_video_increment_growth`（id=241）

### Dashboard

- Dashboard：[ECN 数据看板](https://bi.alvinclub.ca/superset/dashboard/ai-fashion-formal-video-increment/)（id=109）

## 字段

统计对象统一为正式号视频：`videos.platform_video_tags.tag_type = 'env' AND tag_value = 'prod'`，视频键为 `platform + platform_video_id`；`decom.dim_video.is_formal_video` 承接该标记，`formal_tag_created_ts_utc` 为同一 `video_key` 的最早正式号 tag 创建时间。日期按 `America/New_York` 展示，与 `America/Toronto` 同属美东日期边界。

### ads_daily_formal_video_increment

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 新增总播放量 | `daily_view_increment` | 视频 | 截至当日已发布的全部正式号视频，当日 `day_end_views` 与前一日的差分汇总 | 仅正式号视频 | 可跨天求和 |
| 新增总点赞数 | `daily_like_increment` | 视频 | 同上，基于 `day_end_likes` 差分 | 仅正式号视频 | 可跨天求和 |
| 当日新增发布视频数 | `new_video_count` | 视频 | 首次采集到 `day_end_views > 0` 的日期落在当天的正式号视频数 | 仅正式号视频 | 可跨天求和 |
| 平均 LTV | `avg_ltv_7d_views` | 视频 | `ltv_7d_view_count / ltv_7d_video_count`；样本为首次采集正播放日期为 `dt-6` 的正式号视频，取其在 `dt` 当天的累计 views 均值；无样本时返回 `0` | 仅正式号视频 | 不可求和或平均；跨天须用 `SUM(ltv_7d_view_count) / SUM(ltv_7d_video_count)` 重算 |
| LTV 样本视频数 | `ltv_7d_video_count` | 视频 | 首次采集正播放日期为 `dt-6` 的正式号视频数 | 仅正式号视频 | 可跨天求和（作为分母） |
| LTV 样本累计 views | `ltv_7d_view_count` | 视频 | LTV 样本视频在 `dt` 当天的累计 views 总和 | 仅正式号视频 | 可跨天求和（作为分子） |
| 内容账号总数 | `creator_count` | 内容账号（`channel + creator_id`） | 按账号下第一个正式号视频首次采集到 `views > 0` 的日期起每日累计去重；不按视频发布时间或 tag 创建时间累计 | 仅正式号视频 | 累计值，取目标日当天一行，不可跨天求和 |
| 正式号视频累计数 | `formal_video_count_to_date` | 视频 | 按视频 `first_positive_view_dt` 起每日累计去重视频数 | 仅正式号视频 | 累计值，取目标日当天一行，不可跨天求和 |

### ads_weekly_formal_video_increment_growth

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 周序号与窗口 | `week_index` / `week_start_dt` / `week_end_dt` / `week_label` | 固定 7 日窗口 | `week_index = DIV(DATE_DIFF(dt, DATE '2026-04-13', DAY), 7)`；从 `2026-04-13` 起每 7 天一个窗口，只保留满 7 天的周 | 无 | 不聚合 |
| 周内总新增 views | `weekly_view_increment` | 视频 | 完整 7 天窗口内 `daily_view_increment` 之和 | 无 | 可跨周求和 |
| 每日新增总 views 周均值 | `weekly_avg_daily_view_increment` | 视频 | 完整 7 天窗口内 `daily_view_increment` 的算术平均值 | 只统计完整 7 天窗口 | 不可跨周平均；须用 `SUM(weekly_view_increment) / SUM(day_count)` 重算 |
| 上周周均值 | `previous_week_avg_daily_view_increment` | 视频 | 上一个 7 天窗口的周均值，作为增长倍数分母 | 无 | 不聚合 |
| 周增长倍数 | `week_growth_multiple` | 视频 | 当前周均值 / 上一周周均值；首周固定为 `1`；非首周上周均值为 0 时返回 `NULL` | 无 | 不可求和或平均 |
| 周增长倍数卡片 | `avg_week_growth_multiple` | 视频 | 排除首周基准后的 `week_growth_multiple` 算术平均值 | 排除首周 | 已是全周期汇总值，不可再聚合 |
| 月度增长倍数 | `monthly_growth_multiple` | 视频 | `avg_week_growth_multiple ^ (30.5 / 7)` | 无 | 已是全周期汇总值，不可再聚合 |
| 季度增长倍数 | `quarterly_growth_multiple` | 视频 | `avg_week_growth_multiple ^ (91.5 / 7)` | 无 | 已是全周期汇总值，不可再聚合 |

补充说明：

- 该主题只看正式号视频，不再额外维护 AI-Fashion 账号名单
- YouTube 累计播放与点赞复用 Social Content v2 链路，来源为 `videos.yt_video_stats_log.view_count / like_count`；TikTok、Instagram 数据源保持不变
- ECN 日表从 `2026-03-30` 开始，而 v2 状态链路从 `2026-03-13` 开始构建，能够承接日增量首日差分和 7 天 LTV 所需历史
- `dim_video` 保留 Social Content 原有 YouTube AI 团队排除规则，但正式号 tag 命中的视频会作为例外保留
- 如果正式号 tag 视频尚未出现在平台主视频表中，`dim_video` 使用 tag 创建时间作为 `published_dt` 兜底；新增视频和 LTV 仍以首次采集正播放日期为准

## 依赖

- 上游：`videos.platform_video_tags`, `videos.yt_video_stats_log`, `videos.video_stats_log`, `videos.ig_media_stats_log`
- 中间表：`decom.dim_video`, `decom.dws_social_video_snapshot_daily_v2`, `decom.dws_social_video_state_daily_v2`
- 报表表：`decom.ads_daily_formal_video_increment`, `decom.ads_weekly_formal_video_increment_growth`
- Superset：物理 dataset `ads_daily_formal_video_increment`（id=240）、`ads_weekly_formal_video_increment_growth`（id=241）；chart `正式号发布增量（日表）-全部已发布视频新增总播放量趋势`（id=593）、`正式号发布增量（日表）-当日新增发布视频数趋势`（id=594）、`正式号发布增量（日表）-平均 LTV 趋势`（id=595）、`正式号发布增量（日表）-内容账号总数趋势`（id=596）、`正式号发布增量（日表）-全部已发布视频新增总点赞数趋势`（id=597）、`正式号发布增量（周表）-每日新增总 views 周均值与增长倍数`（id=598）、`正式号发布增量（周表）-周增长倍数`（id=599）、`正式号发布增量（周表）-月度增长倍数`（id=600）、`正式号发布增量（周表）-季度增长倍数`（id=601）；dashboard `【AI-Fashion】账号发布数据增量看板`（id=109）
- 脚本：`jobs/dim/dim_video.py`, `jobs/dws/dws_social_video_snapshot_daily_v2.py`, `jobs/dws/dws_social_video_state_daily_v2.py`, `jobs/ads/ads_daily_formal_video_increment.py`, `jobs/ads/ads_weekly_formal_video_increment_growth.py`
- DDL：`ddl/dim/dim_video.sql`, `ddl/dws/dws_social_video_snapshot_daily_v2.sql`, `ddl/dws/dws_social_video_state_daily_v2.sql`, `ddl/ads/ads_daily_formal_video_increment.sql`, `ddl/ads/ads_weekly_formal_video_increment_growth.sql`
- 编排：`env/etl_config.json`, `etl_run.py`
