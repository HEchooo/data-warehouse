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

| 需求字段 | 定义 |
|---|---|
| 日期 | 报表日期，按 `America/New_York` 展示；底层社媒状态表与 `America/Toronto` 同属美东日期边界 |
| 正式号视频 | `videos.platform_video_tags.tag_type = 'env' AND videos.platform_video_tags.tag_value = 'prod'`；平台字段为 `platform`，视频字段为 `platform_video_id` |
| 全部已发布视频新增总播放量 | 当天统计截至当日已经发布过的全部正式号视频，当日 `day_end_views - 前一日 day_end_views` 的总和 |
| 当日新增发布视频数 | 首次采集到 `views > 0` 的日期落在当天的正式号视频数 |
| 平均 LTV | 首次采集正播放日期为 `dt-6` 的正式号视频，在 `dt` 当天的累计 `views` 均值 |
| 内容账号总数 | 截至当天，账号下至少有一个正式号视频首次采集到 `views > 0` 的去重内容账号数；账号键为 `channel + creator_id` |
| 全部已发布视频新增总点赞数 | 当天统计截至当日已经发布过的全部正式号视频，当日 `day_end_likes - 前一日 day_end_likes` 的总和 |
| 每日新增总 views 的周均值 | 从 `2026-04-13` 起每 7 天聚合，仅统计完整 7 天窗口内 `daily_view_increment` 的算术平均值 |
| 周增长倍数 | 当前 7 天窗口的每日新增 views 周均值 / 上一个 7 天窗口的每日新增 views 周均值；首周展示为 `1` |
| 周增长倍数卡片 | 排除首周基准后，对周增长倍数取算术平均值 |
| 月度增长倍数 | `周增长倍数卡片 ^ (30.5 / 7)` |
| 季度增长倍数 | `周增长倍数卡片 ^ (91.5 / 7)` |

## 已落地字段

### `decom.dim_video`

| 需求字段 | 落地表 | 落地字段 | 口径 |
|---|---|---|---|
| 正式号视频 | `decom.dim_video` | `is_formal_video` | 来自 `videos.platform_video_tags` 的 `env=prod` 标记 |
| 正式号 tag 创建时间 | `decom.dim_video` | `formal_tag_created_ts_utc` | 同一 `video_key` 的最早正式号 tag 创建时间 |

### `decom.ads_daily_formal_video_increment`

| 需求字段 | 落地表 | 落地字段 | 口径 |
|---|---|---|---|
| 日期 | `decom.ads_daily_formal_video_increment` | `dt` | 从 `2026-03-30` 到美东当天逐日补齐 |
| 全部已发布视频新增总播放量 | `decom.ads_daily_formal_video_increment` | `daily_view_increment` | 正式号视频当日累计 views 与前一日累计 views 的差分汇总 |
| 全部已发布视频新增总点赞数 | `decom.ads_daily_formal_video_increment` | `daily_like_increment` | 正式号视频当日累计 likes 与前一日累计 likes 的差分汇总 |
| 当日新增发布视频数 | `decom.ads_daily_formal_video_increment` | `new_video_count` | 首次采集到 `day_end_views > 0` 的正式号视频数 |
| 平均 LTV | `decom.ads_daily_formal_video_increment` | `avg_ltv_7d_views` | `ltv_7d_view_count / ltv_7d_video_count` |
| LTV 样本视频数 | `decom.ads_daily_formal_video_increment` | `ltv_7d_video_count` | 首次采集正播放日期为 `dt-6` 的正式号视频数 |
| LTV 样本累计 views | `decom.ads_daily_formal_video_increment` | `ltv_7d_view_count` | LTV 样本视频在 `dt` 当天的累计 views 总和 |
| 内容账号总数 | `decom.ads_daily_formal_video_increment` | `creator_count` | 先取账号下正式号视频最早 `first_positive_view_dt`，从该日期起每日累计去重 `channel + creator_id` |
| 正式号视频累计数 | `decom.ads_daily_formal_video_increment` | `formal_video_count_to_date` | 按视频 `first_positive_view_dt` 从该日期起每日累计去重视频数 |

### `decom.ads_weekly_formal_video_increment_growth`

| 需求字段 | 落地表 | 落地字段 | 口径 |
|---|---|---|---|
| 周序号 | `decom.ads_weekly_formal_video_increment_growth` | `week_index` | `DIV(DATE_DIFF(dt, DATE '2026-04-13', DAY), 7)` |
| 周窗口 | `decom.ads_weekly_formal_video_increment_growth` | `week_start_dt` / `week_end_dt` / `week_label` | 从 `2026-04-13` 起每 7 天一个完整窗口，仅保留满 7 天的周 |
| 每日新增总 views 的周均值 | `decom.ads_weekly_formal_video_increment_growth` | `weekly_avg_daily_view_increment` | 完整 7 天窗口内 `daily_view_increment` 的算术平均值 |
| 周增长倍数 | `decom.ads_weekly_formal_video_increment_growth` | `week_growth_multiple` | 当前周均值 / 前一周均值；首周为 `1` |
| 周增长倍数卡片 | `decom.ads_weekly_formal_video_increment_growth` | `avg_week_growth_multiple` | 排除首周后的 `week_growth_multiple` 平均值 |
| 月度增长倍数 | `decom.ads_weekly_formal_video_increment_growth` | `monthly_growth_multiple` | `avg_week_growth_multiple ^ (30.5 / 7)` |
| 季度增长倍数 | `decom.ads_weekly_formal_video_increment_growth` | `quarterly_growth_multiple` | `avg_week_growth_multiple ^ (91.5 / 7)` |

补充说明：

- 该主题只看正式号视频，不再额外维护 AI-Fashion 账号名单
- YouTube 累计播放与点赞复用 Social Content v2 链路，来源为 `videos.yt_video_stats_log.view_count / like_count`；TikTok、Instagram 数据源保持不变
- 日增播放、日增点赞和新增视频数可跨日求和；累计账号/视频数、平均 LTV 和所有增长倍数不可跨日或跨周求和、平均
- 平均 LTV 无样本时落 `0`；周增长倍数首周固定为 `1`，非首周的前周均值为 0 时返回 `NULL`
- ECN 日表从 `2026-03-30` 开始，而 v2 状态链路从 `2026-03-13` 开始构建，能够承接日增量首日差分和 7 天 LTV 所需历史
- `dim_video` 保留 Social Content 原有 YouTube AI 团队排除规则，但正式号 tag 命中的视频会作为例外保留
- 内容账号总数不按视频发布时间或正式号 tag `created_at` 累计，而是按账号下第一个正式号视频首次采集到 `views > 0` 的日期累计；账号进入正式号集合后，后续每日都计入
- 如果正式号 tag 视频尚未出现在平台主视频表中，`dim_video` 使用 tag 创建时间作为 `published_dt` 兜底；新增视频和 LTV 仍以首次采集正播放日期为准
- 截至 2026-05-12 本次上线校验，`env=prod` 正式号视频共 292 条、内容账号 289 个，正式号 tag 日期均为美东 `2026-05-11`；正式号累计账号/视频不使用该 tag 日期入表，而等待首次采集正播放日期
- 2026-05-12 首次上线时旧 analytics 源暂无正式号正播放快照；该结论仅是历史校验记录，切换到 stats 链路后不再作为当前数据状态判断依据

## 依赖

- 上游：`videos.platform_video_tags`, `videos.yt_video_stats_log`, `videos.video_stats_log`, `videos.ig_media_stats_log`
- 中间表：`decom.dim_video`, `decom.dws_social_video_snapshot_daily_v2`, `decom.dws_social_video_state_daily_v2`
- 报表表：`decom.ads_daily_formal_video_increment`, `decom.ads_weekly_formal_video_increment_growth`
- Superset：物理 dataset `ads_daily_formal_video_increment`（id=240）、`ads_weekly_formal_video_increment_growth`（id=241）；chart `正式号发布增量（日表）-全部已发布视频新增总播放量趋势`（id=593）、`正式号发布增量（日表）-当日新增发布视频数趋势`（id=594）、`正式号发布增量（日表）-平均 LTV 趋势`（id=595）、`正式号发布增量（日表）-内容账号总数趋势`（id=596）、`正式号发布增量（日表）-全部已发布视频新增总点赞数趋势`（id=597）、`正式号发布增量（周表）-每日新增总 views 周均值与增长倍数`（id=598）、`正式号发布增量（周表）-周增长倍数`（id=599）、`正式号发布增量（周表）-月度增长倍数`（id=600）、`正式号发布增量（周表）-季度增长倍数`（id=601）；dashboard `【AI-Fashion】账号发布数据增量看板`（id=109）
- 脚本：`jobs/dim/dim_video.py`, `jobs/dws/dws_social_video_snapshot_daily_v2.py`, `jobs/dws/dws_social_video_state_daily_v2.py`, `jobs/ads/ads_daily_formal_video_increment.py`, `jobs/ads/ads_weekly_formal_video_increment_growth.py`
- DDL：`ddl/dim/dim_video.sql`, `ddl/dws/dws_social_video_snapshot_daily_v2.sql`, `ddl/dws/dws_social_video_state_daily_v2.sql`, `ddl/ads/ads_daily_formal_video_increment.sql`, `ddl/ads/ads_weekly_formal_video_increment_growth.sql`
- 编排：`env/etl_config.json`, `etl_run.py`
