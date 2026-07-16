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
| 疑似限流账号 Ranking | `dt=D`；账号按 `channel + creator_id` 区分。过去 28 天发布视频数除以 4 得到周均发布量，只有周均发布量大于 3 的账号进入监控池。按发布时间取最近 10 个视频，最近 3 个为观察组、此前 7 个为历史基准，10 个视频都必须在 T+1 当天存在真实快照；若最近 3 个视频的 T+1 播放量逐个低于此前 7 个视频 T+1 平均播放量的 30%，则进入疑似限流榜 |

### 疑似限流账号日榜字段

| 需求字段 | 落地字段 | 口径 |
|------|------|------|
| 榜单日期 | `dt` | 已完整结束的美东自然日 `D`；只使用发布日期不晚于 `D-1` 且已完成 T+1 观测的视频 |
| 监控池日期 | `pool_window_start_dt` / `pool_window_end_dt` | `D-28 ~ D-1`，共 28 个完整自然日 |
| 监控池发布频率 | `pool_published_video_count` / `weekly_avg_published_video_count` | 过去 28 天发布视频数及其除以 4 后的周均发布量；周均严格大于 3，即 28 天至少发布 13 个视频 |
| 历史基准日期 | `baseline_start_dt` / `baseline_end_dt` | 此前 7 个基准视频中最早和最晚的发布日期，不再表示固定自然日窗口 |
| 近期观察日期 | `recent_start_dt` / `recent_end_dt` | 最近 3 个视频中最早和最晚的发布日期，不再要求每天都有发布 |
| 排名 | `rank_no` / `channel_rank_no` | 按限流指数升序生成全局排名和平台内排名；越小表示近期播放相对历史下降越明显 |
| 账号 | `channel + creator_id` | 不做跨平台账号合并；展示名称、Handle 和 URL 取窗口内最近发布视频对应账号属性 |
| 历史活跃发布日数 | `baseline_active_day_count` | 此前 7 个基准视频覆盖的发布日期数，仅用于展示，不再作为准入条件 |
| 历史样本 | `baseline_video_count` / `baseline_t1_view_count` | 固定为此前 7 个视频及这些视频在 T+1 日末的累计播放总和 |
| 历史 T+1 篇均播放 | `baseline_t1_avg_view_count` | `baseline_t1_view_count / 7` |
| 近期样本 | `recent_video_count` / `recent_t1_view_count` | 固定为最近 3 个视频及这些视频在 T+1 日末的累计播放总和 |
| 近期 T+1 篇均播放 | `recent_t1_avg_view_count` | `recent_t1_view_count / 3` |
| 近期三次 T+1 播放 | `recent_video_1_t1_view_count` / `recent_video_2_t1_view_count` / `recent_video_3_t1_view_count` | 按发布时间从早到晚对应最近第 3、第 2、第 1 个视频；三者都必须逐个低于历史基准的 30% |
| 限流指数 | `throttle_ratio` | `recent_t1_avg_view_count / baseline_t1_avg_view_count`；按升序排名 |
| 下降比例 | `decline_rate` | `1 - throttle_ratio` |
| 疑似程度 | `severity` | 限流指数 `<=10%` 为“高度疑似”，其余命中账号为“疑似” |

### 账号发布日期 T+1 中间表字段

| 需求字段 | 落地字段 | 口径 |
|------|------|------|
| 发布日期 / T+1 日期 | `published_dt` / `t1_dt` | 按 `America/Toronto` 切日，且 `t1_dt = published_dt + 1` |
| 发布视频数 | `published_video_count` | 账号在该发布日期发布的 Social Content 视频数 |
| T+1 真实观测视频数 | `observed_t1_video_count` | 在 T+1 当天真实快照表中存在记录的视频数，不使用 dense state 的沿用值代替 |
| T+1 真实观测播放总和 | `observed_t1_view_count` | 仅汇总存在 T+1 真实快照的视频日末累计播放 |
| T+1 篇均播放 | `t1_avg_view_count` | 只有真实观测视频数等于发布视频数时才计算，否则返回 `NULL` |
| T+1 快照完整 | `is_t1_snapshot_complete` | `observed_t1_video_count = published_video_count` |
| 视频 T+1 明细 | `video_samples` | 保存账号当日各视频的 `video_key`、发布时间、T+1 播放量和真实快照标记，供 ADS 按最近发布次数排序；不改变中间表粒度 |

## 依赖

- 播放事实：`decom.dws_social_video_snapshot_daily_v2`, `decom.dws_social_video_state_daily_v2`
- 视频维表：`decom.dim_video`；承接三端视频、账号展示属性和 Social Content 范围标记
- 中间表：`decom.dws_social_creator_published_t1_daily`
- 报表表：`decom.ads_daily_social_creator_suspected_throttle_rankings`
- 展示字段映射：`docs/rules/video-mapping.md`
- 脚本：`jobs/dws/dws_social_creator_published_t1_daily.py`, `jobs/ads/ads_daily_social_creator_suspected_throttle_rankings.py`
- DDL：`ddl/dws/dws_social_creator_published_t1_daily.sql`, `ddl/ads/ads_daily_social_creator_suspected_throttle_rankings.sql`
- 编排：`env/etl_config.json` 中的独立表级入口 `dws_social_creator_published_t1_daily` 与 `ads_daily_social_creator_suspected_throttle_rankings`
- 性能：疑似限流账号榜只读取中间表的滚动 28 个发布日期分区；视频级 T+1 播放和真实快照标记在 DWS 一次计算并以数组复用，避免 ADS 回扫全量视频状态表
- Superset：物理 dataset `ads_daily_social_creator_suspected_throttle_rankings`（id=273），chart `684 / 685 / 686`，dashboard `【Social Content】疑似限流账号监控`（id=123）
