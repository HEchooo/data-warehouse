# 【Social Content】社媒表现数据看板踩坑记录

## YouTube 排除名单不能只过滤测试渠道

- 这条链路最初只排除了 `YOUTUBE_TEST_CHANNEL_IDS`，结果 AI 团队的内部渠道仍会落入 `dws_social_video_daily` 和 `ads_daily_social_content`
- 当前口径已经明确：Social Content 需要统一排除 `docs/rules/video-mapping.md` 中定义的全部 AI 团队渠道，也就是 `video 部门测试渠道 + video 部门内部渠道`
- 实现上不要在上下游脚本里各维护一份名单；当前以 `jobs/dws/dws_social_video_daily.py` 的共享常量作为代码侧单点来源，下游 ADS 脚本直接复用
- 排查这类问题时，先对照 `docs/rules/video-mapping.md` 检查规则 SSOT，再检查主题链路里是否存在“文档已更新、脚本仍保留旧名单”的漂移

## 历史博主总数不能直接从最近分区的 `dws_video_daily` 推

- `dws_video_daily` 当前按最近窗口重刷，近期分区里只保证“近期发布”或“近期仍有快照”的视频会出现
- 如果直接从这些近期分区反推“截至昨日的历史博主总数”，会漏掉很早发布且最近没有新快照的老博主
- 这个指标的分母应直接从视频维表按 `published_dt < dt` 统计历史去重博主数

## “最新 3 条有数据视频”必须带上窗口前种子快照

- 看板是按 `dt` 回看历史时点，不是只看 `dt` 当天是否有采集
- 如果只用窗口内当天快照，会把“窗口开始前已采集、窗口内没新采集”的视频错误排除出样本
- 正确做法是先取 `min_dt` 前最后一个日末快照作为 seed，再在窗口内按最近一次已知累计值向后沿用

## “全部渠道”不能直接按裸 `creator_id` 去重

- 本主题最终要求把 `全部渠道 / youtube / tiktok / ins` 4 条线直接写进同一张 ADS 表
- 如果 `全部渠道` 直接按裸 `creator_id` 去重，跨平台同字符串 ID 会被误判成同一博主
- 当前保守口径是 `channel + creator_id`，同时用于“前一天新增 views / 博主总数”的分母和“每个博主最新 3 条”的博主分组

## 自动发布跨平台视频只在“全部渠道”里按 `task_id` 去重

- `docs/rules/video-mapping.md` 已定义自动发布统一视频映射：YouTube 看 `yt_uploads.task_id + status = COMPLETED`，TikTok 看 `video_uploads.task_id + status = COMPLETED`，Ins 看 `ig_uploads.task_id + status = COMPLETED`
- 同一个自动发布任务会把同一条视频发到多个平台；这些平台视频在上传表里共享同一个 `task_id`
- 如果 `全部渠道` 继续按原始 `video_key` 统计“新增视频”，同一个自动发布任务会被重复算多次
- 因为 dashboard 支持 `channel` filter，去重只应作用于 `channel = 全部渠道` 这条展示线；单渠道筛选时必须保留平台自身原始视频数，不能把跨平台规则误带进去
- 正确做法是先在 `dim_auto_publish_video_task` 里把自动发布视频统一映射成 `unified_video_key`，再由 `dws_social_channel_daily` 回看该统一键全历史最早 `views > 0` 的日期，只在这一天记 `1`
- 为避免误并，只有当同一个 `video_key` 能稳定映射到唯一 `task_id` 时才参与去重；如果一个视频键出现多个不同 `task_id`，宁可退回原始 `video_key`

## 默认展示 4 条线时，不要额外绕一层 `channel_filter` 展示视图

- 这次为了同时满足“默认显示全部渠道 + 支持单渠道筛选”，一度做过 `ads_daily_social_content_filter` 视图和 Superset dataset `231`
- 技术上可用，但会额外引入：
  - BigQuery 对象
  - Superset dataset
  - 图表 groupby 字段和 filter target 的双份维护
- 当前更稳的做法是直接让主表 `ads_daily_social_content.channel` 产出 `全部渠道 / youtube / tiktok / ins`
- 这样 dashboard 默认不筛选时自然显示 4 条线，选中单个渠道时也能直接只剩对应折线

## `dws_social_video_daily` 不要按“逐日 1 天 1 个 job”全量回刷

- 这次重构 `dws_social_video_daily` 时，曾落过一版“逐日滚动”实现：每个 `dt` 单独跑一条 BigQuery 查询，并依赖前一天结果继续往后推
- 业务语义虽然直观，但全量回刷会退化成“历史多少天就启动多少个 job”
- 在 `2023-07-14 ~ 2026-04-03` 这次全量窗口里，一共 `995` 天；逐日滚动方案实际跑起来需要几十分钟，而且中断后只会留下中间 staging 表
- 这个方案只适合讲清楚口径，不适合作为最终的生产全量构建方式

## 更快的做法是“真实采集日快照 -> 生效区间 -> 一次性展开逐日明细”

- 社媒视频的 `views / likes` 天然是累计值，因此两次真实采集之间，累计值其实保持不变
- 所以不需要真的“今天抄昨天”地跑 `995` 次；先把真实采集日整理成日末快照，再把每个累计值转成生效区间，最后一次性展开成每天即可
- 这样生成的 `dws_social_video_daily` 结果仍然满足“每个视频从 `published_dt` 起每天一行、当天没采集就沿用最近一次累计值”的业务要求
- 这次按批量区间方案重写后，再执行同一窗口的生产全量回刷，整条链路总耗时约 `110.1` 秒，数量级上明显优于逐日滚动

## `snapshot_daily` 全量回刷不能强制每个日期都有行

- `dws_social_video_snapshot_daily` 的表语义是“只在真实采集日有记录”，不是 dense daily 状态表
- 全量回刷时，处理日期范围可能覆盖上千天，但原始日志真实有采集的日期远少于目标日期列表
- 因此 staging 校验只能要求有数据且 `dt + video_key` 唯一，不能要求 `COUNT(DISTINCT dt) = len(dates)`
- 如果需要“每天都有一行”的语义，应在 `dws_social_video_state_daily` 校验，而不是在 snapshot 层校验

## YouTube `analytics_log` 与 `stats_log` 不能视为同口径字段替换

- `yt_video_analytics_log.views / likes` 与 `yt_video_stats_log.view_count / like_count` 在相同 `video_id + collected_at` 下仍存在大量差异，迁移不是简单字段改名
- `yt_video_stats_log` 的视频覆盖更完整，但多伦多日期历史从 `2026-03-13` 开始，早于该日期不能只靠新表重建
- 为避免重写 v1 历史，当前采用独立 v2 链路；v2 内部从 `2026-03-13` 构建，看板固定从 `2026-05-01` 展示
- `yt_video_stats_log` 存在极少量相同 `video_id + collected_at` 的重复记录，日末去重必须增加 `id DESC` 作为稳定次序
- 首次回填验证中，`2026-05-01` 起 TikTok / Ins 与 v1 一致；YouTube v2 Views 约高 `12.02%`、Likes 约高 `9.97%`，符合更换统计源后的预期
