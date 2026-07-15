# 【Social Content】疑似限流账号监控

## 报表

- Dashboard：[【Social Content】疑似限流账号监控](https://bi.alvinclub.ca/superset/dashboard/social-content-suspected-throttle-monitor/)（id=123），独立看板，不接入现有每日排行榜 Dashboard
- 账号发布日期 T+1 中间表：`decom.dws_social_creator_published_t1_daily`
- 疑似限流账号日榜表：`decom.ads_daily_social_creator_suspected_throttle_rankings`
- Superset 物理 dataset：`ads_daily_social_creator_suspected_throttle_rankings`（id=273，主时间列 `dt`）
- Superset chart：疑似限流账号数（id=684）、疑似限流账号趋势（按平台）（id=685）、疑似限流账号 Ranking（id=686）
- 中间表粒度：`published_dt + channel + creator_id`
- 日榜粒度：`dt + channel + creator_id`；`dt` 表示已完整结束的美东自然日

## 字段

### 榜单口径

| 需求字段 | 定义 |
|------|------|
| 疑似限流账号 Ranking | `dt=D`；历史基准窗口为 `D-10 ~ D-4`，近期窗口为 `D-3 ~ D-1`；账号按 `channel + creator_id` 区分。历史至少 5 条视频且覆盖至少 3 个活跃发布日，近期 3 天每天至少发布 1 条视频；两个窗口内所有发布视频都必须在 T+1 当天存在真实快照。若近期三天各自的 T+1 篇均播放量都低于历史 T+1 篇均播放量的 30%，则进入疑似限流榜 |

### 疑似限流账号日榜字段

| 需求字段 | 落地字段 | 口径 |
|------|------|------|
| 榜单日期 | `dt` | 已完整结束的美东自然日 `D`；例如 `D=2026-07-15` 时，近期发布日期为 `2026-07-12 ~ 2026-07-14` |
| 历史基准日期 | `baseline_start_dt` / `baseline_end_dt` | `D-10 ~ D-4`，共 7 个自然日 |
| 近期观察日期 | `recent_start_dt` / `recent_end_dt` | `D-3 ~ D-1`，共 3 个自然日 |
| 排名 | `rank_no` / `channel_rank_no` | 按限流指数升序生成全局排名和平台内排名；越小表示近期播放相对历史下降越明显 |
| 账号 | `channel + creator_id` | 不做跨平台账号合并；展示名称、Handle 和 URL 取窗口内最近发布视频对应账号属性 |
| 历史活跃发布日数 | `baseline_active_day_count` | 历史 7 天窗口内实际发布过视频的日期数，准入下限为 3 天 |
| 历史样本 | `baseline_video_count` / `baseline_t1_view_count` | 历史窗口发布视频数及这些视频在 T+1 日末的累计播放总和，视频数准入下限为 5 |
| 历史 T+1 篇均播放 | `baseline_t1_avg_view_count` | `baseline_t1_view_count / baseline_video_count`；不是 7 个日均值的简单平均 |
| 近期样本 | `recent_video_count` / `recent_t1_view_count` | 近期三天发布视频数及这些视频在 T+1 日末的累计播放总和；三天每天都必须有发布 |
| 近期 T+1 篇均播放 | `recent_t1_avg_view_count` | `recent_t1_view_count / recent_video_count` |
| 近期每日 T+1 篇均播放 | `recent_day_1_t1_avg_view_count` / `recent_day_2_t1_avg_view_count` / `recent_day_3_t1_avg_view_count` | 分别对应 `D-3 / D-2 / D-1` 发布 cohort 的 T+1 篇均播放，三者都必须低于历史基准的 30% |
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

## 依赖

- 播放事实：`decom.dws_social_video_snapshot_daily_v2`, `decom.dws_social_video_state_daily_v2`
- 视频维表：`decom.dim_video`；承接三端视频、账号展示属性和 Social Content 范围标记
- 中间表：`decom.dws_social_creator_published_t1_daily`
- 报表表：`decom.ads_daily_social_creator_suspected_throttle_rankings`
- 展示字段映射：`docs/rules/video-mapping.md`
- 脚本：`jobs/dws/dws_social_creator_published_t1_daily.py`, `jobs/ads/ads_daily_social_creator_suspected_throttle_rankings.py`
- DDL：`ddl/dws/dws_social_creator_published_t1_daily.sql`, `ddl/ads/ads_daily_social_creator_suspected_throttle_rankings.sql`
- 编排：`env/etl_config.json` 中的独立表级入口 `dws_social_creator_published_t1_daily` 与 `ads_daily_social_creator_suspected_throttle_rankings`
- 性能：疑似限流账号榜只读取中间表的 10 个账号发布日期分区；视频级 T+1 真实快照完整性在 DWS 一次计算并复用，避免 Superset 和 ADS 重复扫描视频明细
- Superset：物理 dataset `ads_daily_social_creator_suspected_throttle_rankings`（id=273），chart `684 / 685 / 686`，dashboard `【Social Content】疑似限流账号监控`（id=123）
