# 【Social Content】疑似限流账号监控

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 指标 | T+1 篇均播放 | 次日平均播放量、发布后 1 日平均 Views |
| 指标 | 限流指数 | 疑似限流分、播放下降指数 |
| 指标 | 最近 3 个发布日零播放 | 连续零播放、零播放账号 |
| 指标 | 发布频率 | 发布日数 |

## 适用范围

- 业务线：`ECN 视频`, `TikTok 橱窗号`
- 适合：查询账号 T+1 播放相对历史基准的下降、上升和连续零播放监控结果。
- 不适合：单视频播放排行应使用[每日排行榜](../【Social Content】每日排行榜/fact.md)；渠道总体趋势应使用[Social Content 社媒表现](../【Social Content】社媒表现数据看板/fact.md)。
- 需要澄清：需要确认下降、上升或零播放方向，渠道与统计日。

## 报表

### ads_daily_social_creator_suspected_throttle_rankings

- 表：`my-project-8584-jetonai.decom.ads_daily_social_creator_suspected_throttle_rankings`
- 状态：`active`
- 粒度：一行代表一个 `dt × channel × creator_id`
- 时区：`America/Toronto`
- 数据范围：相对历史基准下降的疑似限流账号日榜。
- 必要过滤：`baseline_t1_avg_view_count > 0`。
- 不包含：上升和连续零播放方向。

### ads_daily_social_creator_view_boost_rankings

- 表：`my-project-8584-jetonai.decom.ads_daily_social_creator_view_boost_rankings`
- 状态：`active`
- 粒度：一行代表一个 `dt × channel × creator_id`
- 时区：`America/Toronto`
- 数据范围：相对历史基准上升的账号日榜。
- 必要过滤：`baseline_t1_avg_view_count > 0`。
- 不包含：下降和连续零播放方向。

### ads_daily_social_creator_zero_view_rankings

- 表：`my-project-8584-jetonai.decom.ads_daily_social_creator_zero_view_rankings`
- 状态：`active`
- 粒度：一行代表一个 `dt × channel × creator_id`
- 时区：`America/Toronto`
- 数据范围：最近发布日连续零播放的账号日榜。
- 必要过滤：使用共同准入池；允许历史基准为 0。
- 不包含：一般播放下降或上升账号。

- Dashboard：[【Social Content】疑似限流账号监控](https://bi.alvinclub.ca/superset/dashboard/social-content-suspected-throttle-monitor/)（id=123），独立看板，不接入现有每日排行榜 Dashboard
- 账号发布日期 T+1 中间表：`decom.dws_social_creator_published_t1_daily`
- 账号播放波动准入中间表：`decom.dws_social_creator_view_fluctuation_daily`（固化所有合格账号及其历史基准/近期日均/最近 3 个发布日零播放计数，三张榜的共同上游）
- 疑似限流账号日榜表（下降方向）：`decom.ads_daily_social_creator_suspected_throttle_rankings`
- 播放量波动增加账号日榜表（上升方向）：`decom.ads_daily_social_creator_view_boost_rankings`
- 连续零播放账号日榜表（零播放方向）：`decom.ads_daily_social_creator_zero_view_rankings`
- Superset 物理 dataset：`ads_daily_social_creator_suspected_throttle_rankings`（id=273）、`ads_daily_social_creator_view_boost_rankings`（id=278）、`ads_daily_social_creator_zero_view_rankings`（id=294）；主时间列均为 `dt`
- Superset chart：疑似限流账号数（id=684，big number）、各平台疑似限流账号数（id=695，按 `channel` 计数）、疑似限流账号 Ranking（下降比例）（id=686，按 `decline_rate` 降序）、疑似限流账号 Ranking（下降绝对值）（id=696，按 `decline_abs` 降序）、播放量波动增加账号 Ranking（上升比例）（id=697，按 `boost_ratio` 降序）、连续零播放账号明细（id=720，表格，raw 模式，按 `baseline_t1_avg_view_count` 降序）；不再区分疑似程度，原饼图 id=685 已删除
- T+1 中间表粒度：`published_dt + channel + creator_id`
- 波动准入中间表粒度：`dt + channel + creator_id`（每行是某统计日一个通过准入的账号）
- 日榜粒度（三张榜一致）：`dt + channel + creator_id`；`dt` 表示已完整结束的美东自然日
- 三张榜共用同一套准入口径（进池 / 观察对象 / 历史基准），单点固化在 DWS 波动准入中间表 `dws_social_creator_view_fluctuation_daily`；三张 ADS 榜均为自包含脚本，只从该中间表按方向 `filter + rank`，不再重复计算准入逻辑
- DWS 波动准入中间表是**池准入宽表**：只做「周均发布 > 3 + 凑满 7 个基准发布日 + 观察对象 = D-1」三条准入，**不含** `baseline_t1_avg_view_count > 0`。基准为 0 的账号（长期零播放）保留在中间表内供零播放榜使用；下降榜与上升榜各自在 ADS 侧显式加 `baseline_t1_avg_view_count > 0` 护栏，避免基准为 0 时比例无意义

## 字段

### 三张榜共用的准入口径

账号按 `channel + creator_id` 区分。样本视频必须在 T+1 当天存在真实播放快照，且账号当前启用：YouTube `videos.yt_channels.is_active = 1`、TikTok `videos.users.is_active = 1`、Instagram `videos.ig_channels.is_active = 1`。不使用 `dim_video.is_formal_video` 作为准入或排除条件。

判定以**发布日**为粒度：过去 28 天有效样本视频数除以 4 得到周均发布量，周均大于 3 的账号进入监控池；取账号在池内按发布日期倒序的最近 8 个发布日，最近 1 个为观察对象、此前 7 个为历史基准。观察对象的发布日期必须恰好是 `D-1`——**只在账号发视频的次日评估一次，不发视频当天不进榜**，所以同一账号在榜上是断续出现的。

三张榜的共用字段（口径完全一致，各榜只按方向过滤）：

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 榜单日期 | `dt` | 自然日 | 已完整结束的美东自然日 `D`；观察对象是发布日期恰好等于 `D-1` 且已完成 T+1 观测的发布日 | 单日筛选 | 不聚合 |
| 监控池窗口 | `pool_window_start_dt` / `pool_window_end_dt` | 自然日 | `D-28 ~ D-1`，共 28 个完整自然日 | 无 | 不聚合 |
| 账号 | `channel` / `creator_id` / `account_name` / `account_handle` / `account_url` | 账号 | 账号按 `channel + creator_id` 区分，不做跨平台合并；展示属性取观察发布日（`D-1`）的账号属性 | 按目标账号过滤 | 不聚合 |
| 监控池发布频率 | `pool_published_video_count` / `weekly_avg_published_video_count` | 视频 | 过去 28 天具有真实 T+1 快照的有效样本视频数，及其除以 4 的周均发布量 | 准入要求周均严格大于 3，即 28 天至少 13 个有效样本 | 账号级快照指标，不可跨天或跨账号求和；跨期须回中间表按目标窗口重算 |
| 历史基准窗口 | `baseline_start_dt` / `baseline_end_dt` / `baseline_active_day_count` | 发布日 | 此前 7 个基准发布日中最早与最晚的发布日期，不是固定自然日窗口；发布日可在日历上分散，允许中间断更 | 准入要求凑满 7 个发布日，进榜账号 `baseline_active_day_count` 恒为 7 | 账号级快照指标，不可跨天或跨账号求和；跨期须回中间表按目标窗口重算 |
| 历史基准样本 | `baseline_video_count` / `baseline_t1_view_count` | 视频 | 此前 7 个基准发布日的视频总条数（可大于 7），及这些视频在 T+1 日末的累计播放总和 | 无 | 账号级快照指标，不可跨天或跨账号求和；跨期须回中间表按目标窗口重算 |
| 历史 T+1 篇均播放 | `baseline_t1_avg_view_count` | 视频 | 此前 7 个发布日**各自的当日 T+1 日均，再对 7 天求平均**（各天等权）；不等于 `baseline_t1_view_count / baseline_video_count` | 无 | 账号级快照指标，不可跨天或跨账号求和；跨期须回中间表按目标窗口重算 |
| 近期观察窗口 | `recent_start_dt` / `recent_end_dt` | 发布日 | 观察发布日的发布日期，恒等于 `D-1`，两个字段取值相同 | 无 | 账号级快照指标，不可跨天或跨账号求和；跨期须回中间表按目标窗口重算 |
| 近期样本 | `recent_video_count` / `recent_t1_view_count` | 视频 | 观察发布日当天的视频条数，及这些视频在 T+1 日末的累计播放总和 | 无 | 账号级快照指标，不可跨天或跨账号求和；跨期须回中间表按目标窗口重算 |
| 近期 T+1 篇均播放 | `recent_t1_avg_view_count` | 视频 | `recent_t1_view_count / recent_video_count`，同日多条取均值 | 无 | 账号级快照指标，不可跨天或跨账号求和；跨期须回中间表按目标窗口重算 |

三张榜都不落库排名字段，排序由展示层 chart 按对应指标降序完成。

### ads_daily_social_creator_suspected_throttle_rankings（疑似限流，下降方向）

命中条件：观察对象当日 T+1 日均低于历史基准的 30%。全量历史分区已按该发布日粒度口径回刷。

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 限流指数 | `throttle_ratio` | 视频 | `recent_t1_avg_view_count / baseline_t1_avg_view_count`；按升序排名 | `baseline_t1_avg_view_count > 0` | 不可求和或平均；跨期须用目标窗口的日均重算 |
| 下降比例 | `decline_rate` | 视频 | `1 - throttle_ratio`；chart 686 的排序维度 | 同上 | 不可求和或平均 |
| 下降绝对值 | `decline_abs` | 视频 | `baseline_t1_avg_view_count - recent_t1_avg_view_count`；chart 696 的排序维度，偏向基准播放高的大号 | 同上 | 不可求和或平均 |
| 近期 T+1 播放明细 | `recent_video_1_t1_view_count` / `recent_video_2_t1_view_count` / `recent_video_3_t1_view_count` | 视频 | 兼容旧口径历史分区保留的逐条视频字段；新口径按发布日聚合，三者固定为 `NULL` | 无 | 不使用 |

### ads_daily_social_creator_view_boost_rankings（播放量波动增加，上升方向）

命中条件：`recent_t1_avg_view_count > baseline_t1_avg_view_count`（任意上升）。该口径较宽，且基准接近 0 的小号会产生极大的上升比例，榜首以此类账号为主。

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 上升比例 | `boost_ratio` | 视频 | `recent_t1_avg_view_count / baseline_t1_avg_view_count - 1`；按降序排名 | `baseline_t1_avg_view_count > 0` | 不可求和或平均 |

本榜不含 `throttle_ratio` / `decline_rate` / `decline_abs` / 逐条视频字段。

### ads_daily_social_creator_zero_view_rankings（连续零播放）

命中条件：最近 3 个发布日的 T+1 播放全为 0（`recent_3day_zero_day_count = 3`）。窗口按**发布日**计数，允许中间断更，不要求连续自然日。本榜不设 `baseline_t1_avg_view_count > 0` 护栏——长期零播放账号（基准也为 0）正是目标对象。

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 零播放窗口 | `zero_window_start_dt` / `zero_window_end_dt` | 发布日 | 最近 3 个发布日中最早与最晚的发布日期；`zero_window_end_dt` 恒等于观察发布日 `D-1`。两日期之间的自然日跨度不固定，取决于账号发布节奏 | 无 | 不聚合 |
| 零播放窗口发布日数 | `zero_window_day_count` | 发布日 | 固定为 3，随脚本常量 `ZERO_WINDOW_DAY_COUNT` 落库，便于口径变更后区分历史分区 | 无 | 不聚合 |
| 观察发布日播放 | `recent_t1_view_count` / `recent_t1_avg_view_count` | 视频 | 进榜账号恒为 0，可用作校验；另外两个发布日是否为 0 由 ADS 与中间表的键集交叉核对保证 | 无 | 不聚合 |
| 历史基准日均 | `baseline_t1_avg_view_count` | 视频 | 说明账号「本来能跑到多少」，是本榜最有信息量的排序维度（chart 720 按其降序）；7 个基准发布日中已有 2 个落在零播放窗口内会把基准拉低，账号长期零播放时基准为 0 | 无 | 不可求和或平均 |

「零播放已持续多久」**不落库**：连续段长度受最近 8 个发布日的窗口上限截断，撞到上限后无法区分「刚好 8 个」和「远超 8 个」，报出来会低估账号已死时长；需求只要「最近 3 个发布日全 0」的布尔判定。

### 中间层 dws_social_creator_view_fluctuation_daily

`dws_social_creator_view_fluctuation_daily`（`dt + channel + creator_id`）收录某统计日通过监控池准入的**全部**账号（下降 / 持平 / 上升 / 零播放都在内），供三张 ADS 榜按方向过滤；不含方向判定字段，`throttle_ratio` / `boost_ratio` 由各 ADS 自行派生。其准入范围**不含** `baseline_t1_avg_view_count > 0`——该护栏下移到下降榜和上升榜的 ADS 侧，以便长期零播放账号保留在中间表内供零播放榜使用。除上表共用字段外，另有 `recent_3day_zero_day_count` / `recent_3day_start_dt`：最近 3 个发布日中 T+1 播放为 0 的发布日数（取值 `0 ~ 3`）及这 3 个发布日中最早的发布日期。

#### 中间层 dws_social_creator_published_t1_daily

粒度 `published_dt + channel + creator_id`，`t1_dt = published_dt + 1`，按 `America/Toronto` 切日。

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 发布视频数 | `published_video_count` | 视频 | 账号在该发布日期且 T+1 当天存在真实快照的有效样本视频数 | 无 | 可跨发布日求和 |
| T+1 真实观测视频数 | `observed_t1_video_count` | 视频 | 与 `published_video_count` 一致；中间表由真实快照直接驱动，不用 dense state 的沿用值代替 | 无 | 可跨发布日求和 |
| T+1 真实观测播放总和 | `observed_t1_view_count` | 视频 | 只汇总存在 T+1 真实快照的视频日末累计播放 | 无 | 可跨发布日求和 |
| T+1 篇均播放 | `t1_avg_view_count` | 视频 | `observed_t1_view_count / observed_t1_video_count` | 无 | 不可求和或平均；跨发布日须先汇总分子分母 |
| T+1 快照完整 | `is_t1_snapshot_complete` | 视频 | 兼容字段；中间表只保留真实 T+1 快照样本，固定为 `TRUE` | 无 | 不聚合 |
| 视频 T+1 明细 | `video_samples` | 视频 | 账号当日各视频的 `video_key`、发布时间、T+1 播放量和真实快照标记；不改变中间表粒度。日榜新口径已不再消费该数组 | 无 | 不聚合 |

## 依赖

- 播放事实：`decom.dws_social_video_snapshot_daily_v2`
- 视频维表：`decom.dim_video`；承接三端视频、账号展示属性、Social Content 范围标记和三端账号当前启用状态 `is_active_account`；`is_formal_video` 仅供其他主题使用，本主题不作准入或排除判断
- 中间表：`decom.dws_social_creator_published_t1_daily`（发布日 T+1 事实）、`decom.dws_social_creator_view_fluctuation_daily`（波动准入，三张榜的共同上游）
- 报表表：`decom.ads_daily_social_creator_suspected_throttle_rankings`（下降）、`decom.ads_daily_social_creator_view_boost_rankings`（上升）、`decom.ads_daily_social_creator_zero_view_rankings`（连续零播放）
- 展示字段映射：`docs/rules/video-mapping.md`
- 共享口径落地：准入 CTE 固化在 DWS 波动准入中间表 `dws_social_creator_view_fluctuation_daily`；三张 ADS 榜脚本自包含，只从该表按方向 `filter + rank`
- 脚本：`jobs/dws/dws_social_creator_published_t1_daily.py`, `jobs/dws/dws_social_creator_view_fluctuation_daily.py`, `jobs/ads/ads_daily_social_creator_suspected_throttle_rankings.py`, `jobs/ads/ads_daily_social_creator_view_boost_rankings.py`, `jobs/ads/ads_daily_social_creator_zero_view_rankings.py`
- DDL：`ddl/dws/dws_social_creator_published_t1_daily.sql`, `ddl/dws/dws_social_creator_view_fluctuation_daily.sql`, `ddl/ads/ads_daily_social_creator_suspected_throttle_rankings.sql`, `ddl/ads/ads_daily_social_creator_view_boost_rankings.sql`, `ddl/ads/ads_daily_social_creator_zero_view_rankings.sql`
- 编排：`env/etl_config.json` 中的表级入口 `dws_social_creator_published_t1_daily` → `dws_social_creator_view_fluctuation_daily` → `ads_daily_social_creator_suspected_throttle_rankings` / `ads_daily_social_creator_view_boost_rankings` / `ads_daily_social_creator_zero_view_rankings`（三张 ADS 依赖波动准入中间表）
- 性能：波动准入中间表只读取 T+1 中间表的滚动 28 个发布日期分区，准入/基准/近期日均/零播放计数一次算好；三张 ADS 各自只按 `dt` 分区读该中间表做方向过滤，排序交给展示层，不落库排名字段
- Superset：物理 dataset `ads_daily_social_creator_suspected_throttle_rankings`（id=273）、`ads_daily_social_creator_view_boost_rankings`（id=278）、`ads_daily_social_creator_zero_view_rankings`（id=294），chart `684 / 686 / 695 / 696 / 697 / 720`（原按疑似程度的饼图 685 已删除），dashboard `【Social Content】疑似限流账号监控`（id=123）
