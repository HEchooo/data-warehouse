# 【Social Content】疑似限流账号监控

## 报表

- Dashboard：[【Social Content】疑似限流账号监控](https://bi.alvinclub.ca/superset/dashboard/social-content-suspected-throttle-monitor/)（id=123），独立看板，不接入现有每日排行榜 Dashboard
- 账号发布日期 T+1 中间表：`decom.dws_social_creator_published_t1_daily`
- 账号播放波动准入中间表：`decom.dws_social_creator_view_fluctuation_daily`（固化所有合格账号及其历史基准/近期日均，下降榜与上升榜的共同上游）
- 疑似限流账号日榜表（下降方向）：`decom.ads_daily_social_creator_suspected_throttle_rankings`
- 播放量波动增加账号日榜表（上升方向）：`decom.ads_daily_social_creator_view_boost_rankings`
- Superset 物理 dataset：`ads_daily_social_creator_suspected_throttle_rankings`（id=273）、`ads_daily_social_creator_view_boost_rankings`（id=278）；主时间列均为 `dt`
- Superset chart：疑似限流账号数（id=684，big number）、各平台疑似限流账号数（id=695，按 `channel` 计数）、疑似限流账号 Ranking（下降比例）（id=686，按 `decline_rate` 降序）、疑似限流账号 Ranking（下降绝对值）（id=696，按 `decline_abs` 降序）、播放量波动增加账号 Ranking（上升比例）（id=697，按 `boost_ratio` 降序）；不再区分疑似程度，原饼图 id=685 已删除
- T+1 中间表粒度：`published_dt + channel + creator_id`
- 波动准入中间表粒度：`dt + channel + creator_id`（每行是某统计日一个通过准入的账号）
- 日榜粒度（两张榜一致）：`dt + channel + creator_id`；`dt` 表示已完整结束的美东自然日
- 下降榜与上升榜共用同一套准入口径（进池 / 观察对象 / 历史基准），单点固化在 DWS 波动准入中间表 `dws_social_creator_view_fluctuation_daily`；两张 ADS 榜均为自包含脚本，只从该中间表按方向 `filter + rank`，不再重复计算准入逻辑

## 字段

### 榜单口径

| 需求字段 | 定义 |
|------|------|
| 疑似限流账号 Ranking | `dt=D`；账号按 `channel + creator_id` 区分，样本视频首先必须在 T+1 当天存在真实播放快照，再仅保留当前启用账号：YouTube `videos.yt_channels.is_active = 1`、TikTok `videos.users.is_active = 1`、Instagram `videos.ig_channels.is_active = 1`。不使用 `dim_video.is_formal_video` 作为准入或排除条件。**判定以「发布日」为粒度**：过去 28 天有效样本视频数除以 4 得到周均发布量，只有周均发布量大于 3 的账号进入监控池。取账号在监控池内按发布日期倒序的最近 8 个发布日，最近 1 个发布日为观察对象、此前 7 个发布日为历史基准。历史基准 = 此前 7 个发布日**各自的当日 T+1 日均播放，再对 7 天求平均（各天等权，与当天视频条数无关）**；观察对象 = 最近发布日的当日 T+1 日均播放（同日多条取当日均值）。观察对象的发布日期必须恰好是 `D-1`——即**只在账号发布视频的次日榜单上评估一次，不发视频当天不进榜**；若观察对象当日日均低于历史基准的 30%，则进入疑似限流榜。全量历史分区已按该发布日粒度口径回刷 |
| 播放量波动增加账号 Ranking | 准入口径与疑似限流榜完全一致（同池、同观察对象 `D-1`、同历史基准）；方向相反：观察对象当日 T+1 日均**高于**历史基准（任意上升，`recent > baseline`）即进入上升榜。上升比例 `boost_ratio = 近期日均 / 基准 - 1`，按其降序排名。注意「任意上升」口径较宽，且基准接近 0 的小号会产生极大的上升比例，榜首以此类账号为主 |

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
| 下降比例 | `decline_rate` | `1 - throttle_ratio`；chart 686 的排序维度 |
| 下降绝对值 | `decline_abs` | `baseline_t1_avg_view_count - recent_t1_avg_view_count`，历史基准与近期日均的绝对差；chart 696 的排序维度，偏向基准播放高的大号 |

### 播放量波动增加账号日榜字段（上升方向）

粒度、准入、监控池、历史基准、观察对象等口径与疑似限流榜完全一致（同源 DWS 波动准入中间表 `dws_social_creator_view_fluctuation_daily`），下表只列方向差异字段，其余同名字段口径相同。

| 需求字段 | 落地字段 | 口径 |
|------|------|------|
| 上升比例 | `boost_ratio` | `recent_t1_avg_view_count / baseline_t1_avg_view_count - 1`，观察发布日日均相对历史基准的上升幅度；按降序排名 |
| 命中条件 | — | `recent_t1_avg_view_count > baseline_t1_avg_view_count`（任意上升）；不含 `throttle_ratio` / `decline_rate` / `decline_abs` / 逐条视频字段 |

### 账号播放波动准入中间表字段

`dws_social_creator_view_fluctuation_daily`：`dt + channel + creator_id` 粒度，收录某统计日通过监控池准入的**全部**账号（下降/持平/上升都在内），供两张 ADS 榜按方向过滤。不含方向判定字段（`throttle_ratio` / `boost_ratio` 等由各 ADS 自行派生）。

| 需求字段 | 落地字段 | 口径 |
|------|------|------|
| 统计日 / 监控池日期 | `dt` / `pool_window_start_dt` / `pool_window_end_dt` | `dt` 为已完整结束的美东自然日；监控池为 `D-28 ~ D-1` |
| 账号 | `channel` / `creator_id` / `account_name` / `account_handle` / `account_url` | 展示属性取观察发布日（`D-1`）对应账号属性 |
| 历史基准 | `baseline_start_dt` / `baseline_end_dt` / `baseline_active_day_count` / `baseline_video_count` / `baseline_t1_view_count` / `baseline_t1_avg_view_count` | 最近 7 个发布日；`baseline_t1_avg_view_count` = 各发布日日均再对 7 天求平均 |
| 近期观察 | `recent_start_dt` / `recent_end_dt` / `recent_video_count` / `recent_t1_view_count` / `recent_t1_avg_view_count` | 观察发布日恒为 `D-1`；`recent_t1_avg_view_count` 为当日 T+1 日均 |
| 发布频率 | `pool_published_video_count` / `weekly_avg_published_video_count` | 过去 28 天有效样本视频数及其周均；准入要求周均 > 3 |
| 准入范围 | — | 仅保留 `周均 > 3` 且 `凑满 7 个基准发布日` 且 `观察对象 = D-1` 且 `baseline_t1_avg_view_count > 0` 的账号 |

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
- 中间表：`decom.dws_social_creator_published_t1_daily`（发布日 T+1 事实）、`decom.dws_social_creator_view_fluctuation_daily`（波动准入，下降/上升两张榜的共同上游）
- 报表表：`decom.ads_daily_social_creator_suspected_throttle_rankings`（下降）、`decom.ads_daily_social_creator_view_boost_rankings`（上升）
- 展示字段映射：`docs/rules/video-mapping.md`
- 共享口径落地：准入 CTE 固化在 DWS 波动准入中间表 `dws_social_creator_view_fluctuation_daily`；两张 ADS 榜脚本自包含，只从该表按方向 `filter + rank`
- 脚本：`jobs/dws/dws_social_creator_published_t1_daily.py`, `jobs/dws/dws_social_creator_view_fluctuation_daily.py`, `jobs/ads/ads_daily_social_creator_suspected_throttle_rankings.py`, `jobs/ads/ads_daily_social_creator_view_boost_rankings.py`
- DDL：`ddl/dws/dws_social_creator_published_t1_daily.sql`, `ddl/dws/dws_social_creator_view_fluctuation_daily.sql`, `ddl/ads/ads_daily_social_creator_suspected_throttle_rankings.sql`, `ddl/ads/ads_daily_social_creator_view_boost_rankings.sql`
- 编排：`env/etl_config.json` 中的表级入口 `dws_social_creator_published_t1_daily` → `dws_social_creator_view_fluctuation_daily` → `ads_daily_social_creator_suspected_throttle_rankings` / `ads_daily_social_creator_view_boost_rankings`（两张 ADS 依赖波动准入中间表）
- 性能：波动准入中间表只读取 T+1 中间表的滚动 28 个发布日期分区，准入/基准/近期日均一次算好；两张 ADS 各自只按 `dt` 分区读该中间表做方向过滤和排名，不重复回扫
- Superset：物理 dataset `ads_daily_social_creator_suspected_throttle_rankings`（id=273）与 `ads_daily_social_creator_view_boost_rankings`（id=278），chart `684 / 686 / 695 / 696 / 697`（原按疑似程度的饼图 685 已删除），dashboard `【Social Content】疑似限流账号监控`（id=123）
