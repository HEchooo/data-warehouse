# 【Social Content】疑似限流账号监控

## 报表

- Dashboard：[【Social Content】疑似限流账号监控](https://bi.alvinclub.ca/superset/dashboard/social-content-suspected-throttle-monitor/)（id=123），独立看板，不接入现有每日排行榜 Dashboard
- 账号发布日期 T+1 中间表：`decom.dws_social_creator_published_t1_daily`
- 疑似限流账号日榜表：`decom.ads_daily_social_creator_suspected_throttle_rankings`
- Superset 物理 dataset：`ads_daily_social_creator_suspected_throttle_rankings`（id=273，主时间列 `dt`）
- Superset chart：疑似限流账号数（id=684）、疑似限流账号分布（按疑似程度）（id=685）、疑似限流账号 Ranking（id=686）、各平台疑似限流账号分布（id=695）
- 中间表粒度：`published_dt + channel + creator_id`
- 日榜粒度：`dt + channel + creator_id`；`dt` 表示已完整结束的美东自然日

## 字段

### 榜单口径

| 需求字段 | 定义 |
|------|------|
| 疑似限流账号 Ranking | `dt=D`；账号按 `channel + creator_id` 区分，样本视频首先必须在 T+1 当天存在真实播放快照，再仅保留当前启用账号：YouTube `videos.yt_channels.is_active = 1`、TikTok `videos.users.is_active = 1`、Instagram `videos.ig_channels.is_active = 1`。不使用 `dim_video.is_formal_video` 作为准入或排除条件。**判定以「发布日」为粒度**：过去 28 天有效样本视频数除以 4 得到周均发布量，只有周均发布量大于 3 的账号进入监控池。取账号在监控池内按发布日期倒序的最近 8 个发布日，最近 1 个发布日为观察对象、此前 7 个发布日为历史基准。历史基准 = 此前 7 个发布日**各自的当日 T+1 日均播放，再对 7 天求平均（各天等权，与当天视频条数无关）**；观察对象 = 最近发布日的当日 T+1 日均播放（同日多条取当日均值）。观察对象的发布日期必须恰好是 `D-1`——即**只在账号发布视频的次日榜单上评估一次，不发视频当天不进榜**；若观察对象当日日均低于历史基准的 30%，则进入疑似限流榜。2026-07-20 前的历史分区仍为旧口径（视频粒度、最近 3 个视频逐个低于基准 30%），未回刷 |

### 疑似限流账号日榜字段

| 需求字段 | 落地字段 | 口径 |
|------|------|------|
| 榜单日期 | `dt` | 已完整结束的美东自然日 `D`；观察对象为发布日期恰好等于 `D-1` 且已完成 T+1 观测的发布日 |
| 监控池日期 | `pool_window_start_dt` / `pool_window_end_dt` | `D-28 ~ D-1`，共 28 个完整自然日 |
| 监控池发布频率 | `pool_published_video_count` / `weekly_avg_published_video_count` | 过去 28 天具有真实 T+1 快照的有效样本视频数，及其除以 4 后的周均发布量；周均严格大于 3，即 28 天至少 13 个有效样本 |
| 历史基准日期 | `baseline_start_dt` / `baseline_end_dt` | 此前 7 个基准发布日中最早和最晚的发布日期，不表示固定自然日窗口 |
| 近期观察日期 | `recent_start_dt` / `recent_end_dt` | 观察发布日的发布日期，恒等于 `D-1`，两个字段取值相同 |
| 排名 | `rank_no` / `channel_rank_no` | 按限流指数升序生成全局排名和平台内排名；越小表示近期播放相对历史下降越明显 |
| 账号 | `channel + creator_id` | 不做跨平台账号合并；展示名称、Handle 和 URL 取观察发布日（`D-1`）对应账号属性 |
| 历史活跃发布日数 | `baseline_active_day_count` | 用作基准的发布日个数，取账号最近实际发过视频的 7 个发布日（可在日历上分散、非连续，允许中间断更）；是准入条件——监控池 28 天内不足 7 个发布日的账号不进榜，进榜账号该值恒为 7 |
| 历史样本 | `baseline_video_count` / `baseline_t1_view_count` | 此前 7 个基准发布日的视频总条数（可大于 7）及这些视频在 T+1 日末的累计播放总和 |
| 历史 T+1 篇均播放 | `baseline_t1_avg_view_count` | 此前 7 个发布日各自的当日 T+1 日均，再对 7 天求平均；各天等权，非 `baseline_t1_view_count / baseline_video_count` |
| 近期样本 | `recent_video_count` / `recent_t1_view_count` | 观察发布日（`D-1`）当天的视频条数及这些视频在 T+1 日末的累计播放总和 |
| 近期 T+1 篇均播放 | `recent_t1_avg_view_count` | 观察发布日（`D-1`）的当日 T+1 日均播放，即 `recent_t1_view_count / recent_video_count`；同日多条取均值 |
| 近期 T+1 播放明细 | `recent_video_1_t1_view_count` / `recent_video_2_t1_view_count` / `recent_video_3_t1_view_count` | 兼容旧口径历史分区保留的逐条视频字段，新口径按发布日聚合，三者固定为 NULL |
| 限流指数 | `throttle_ratio` | `recent_t1_avg_view_count / baseline_t1_avg_view_count`；按升序排名 |
| 下降比例 | `decline_rate` | `1 - throttle_ratio` |
| 疑似程度 | `severity` | 限流指数 `<=10%` 为“高度疑似”，其余命中账号为“疑似” |

### 账号发布日期 T+1 中间表字段

| 需求字段 | 落地字段 | 口径 |
|------|------|------|
| 发布日期 / T+1 日期 | `published_dt` / `t1_dt` | 按 `America/Toronto` 切日，且 `t1_dt = published_dt + 1` |
| 发布视频数 | `published_video_count` | 账号在该发布日期且 T+1 当天存在真实快照的有效样本视频数 |
| T+1 真实观测视频数 | `observed_t1_video_count` | 与 `published_video_count` 一致；中间表由真实快照直接驱动，不使用 dense state 的沿用值代替 |
| T+1 真实观测播放总和 | `observed_t1_view_count` | 仅汇总存在 T+1 真实快照的视频日末累计播放 |
| T+1 篇均播放 | `t1_avg_view_count` | 有效样本的 T+1 真实观测播放总和 / 有效样本数 |
| T+1 快照完整 | `is_t1_snapshot_complete` | 兼容字段；因中间表只保留真实 T+1 快照样本，固定为 `TRUE` |
| 视频 T+1 明细 | `video_samples` | 保存账号当日各视频的 `video_key`、发布时间、T+1 播放量和真实快照标记；不改变中间表粒度。日粒度榜单新口径已不再消费该数组，ADS 直接使用发布日级 `published_video_count` / `observed_t1_view_count` / `t1_avg_view_count` |

## 依赖

- 播放事实：`decom.dws_social_video_snapshot_daily_v2`
- 视频维表：`decom.dim_video`；承接三端视频、账号展示属性、Social Content 范围标记和三端账号当前启用状态 `is_active_account`；`is_formal_video` 仅供其他主题使用，本主题不作准入或排除判断
- 中间表：`decom.dws_social_creator_published_t1_daily`
- 报表表：`decom.ads_daily_social_creator_suspected_throttle_rankings`
- 展示字段映射：`docs/rules/video-mapping.md`
- 脚本：`jobs/dws/dws_social_creator_published_t1_daily.py`, `jobs/ads/ads_daily_social_creator_suspected_throttle_rankings.py`
- DDL：`ddl/dws/dws_social_creator_published_t1_daily.sql`, `ddl/ads/ads_daily_social_creator_suspected_throttle_rankings.sql`
- 编排：`env/etl_config.json` 中的独立表级入口 `dws_social_creator_published_t1_daily` 与 `ads_daily_social_creator_suspected_throttle_rankings`
- 性能：疑似限流账号榜只读取中间表的滚动 28 个发布日期分区；发布日级视频数、T+1 播放总和与日均播放在 DWS 一次算好，ADS 直接按发布日聚合排名，无需展开 `video_samples` 或回扫全量视频状态表
- Superset：物理 dataset `ads_daily_social_creator_suspected_throttle_rankings`（id=273），chart `684 / 685 / 686 / 695`，dashboard `【Social Content】疑似限流账号监控`（id=123）
