# 【Social Content】每日排行榜

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | Social Content 每日排行榜 | Social Content 视频监控看板 |
| 指标 | 增量播放量 | 新增 Views、播放增量 |

## 适用范围

- 业务线：`ECN 视频`, `TikTok 橱窗号`
- 适合：查询社媒视频按日增播放等指标生成的 Top 排行榜。
- 不适合：账号疑似限流应使用[疑似限流账号监控](../【Social Content】疑似限流账号监控/fact.md)；渠道总体趋势应使用[Social Content 社媒表现](../【Social Content】社媒表现数据看板/fact.md)。
- 需要澄清：需要确认榜单类型、渠道、统计日和 Top 数量。

## 报表

### ads_daily_social_video_monitor_rankings_v2

- 表：`my-project-8584-jetonai.decom.ads_daily_social_video_monitor_rankings_v2`
- 状态：`active`
- 粒度：一行代表一个 `dt × ranking_type × rank_no`
- 时区：`America/Toronto`
- 数据范围：两类社媒视频日榜，每类固定 Top 10。
- 必要过滤：按目标 `ranking_type` 和渠道过滤。
- 不包含：账号限流排行和渠道总体趋势。

- Dashboard：[【Social Content】每日排行榜](https://bi.alvinclub.ca/superset/dashboard/122/)（id=122）
- 视频 Top 10 日榜表：`decom.ads_daily_social_video_monitor_rankings_v2`
- 表名中的 `v2` 沿用上游 `dws_social_video_state_daily_v2` 播放快照链路命名；不存在对应的排名表 v1
- 视频 Top 10 Superset 物理 dataset：`ads_daily_social_video_monitor_rankings_v2`（id=272，主时间列 `dt`）
- 其他榜单 Superset 虚拟 dataset：`virtual_social_content_video_monitor_rankings_v2`（id=271，主时间列 `dt`）
- 视频日榜粒度：`dt + ranking_type + rank_no`
- 视频日榜：每天固定产出 2 类榜单，每类 10 行
- Lark 日报推送：两类视频榜单均取 Top 5，以原生 table 展示视频链接和增量播放量；更新时间使用美东时间，卡片底部链接至本 Dashboard

## 字段

### 榜单口径

| 需求字段 | 定义 |
|------|------|
| T-1 发布视频增量 Views Top 10 | `dt=D`；取 `published_dt = D-1` 的视频，以 `D day_end_views - D-1 day_end_views` 作为 D 当日增量播放量，按增量降序取前 10；累计播放量为 D 日末累计值 |
| T-1 发布视频增量 Views Bottom 10 | 与上一条使用同一视频 cohort，排除增量播放量为 `0` 的视频后按增量升序取前 10；负增量不额外排除 |
| T-1 之前发布视频增量 Views Top 10 | `dt=D`；取 `published_dt < D-1` 的视频，按 D 相比 D-1 的增量播放量降序取前 10 |
| 所有社媒账号播放数增量 Top 10（过去 1 周） | 账号按 `channel + creator_id` 区分；汇总账号下截至 `D-1` 已发布的所有历史视频，以单视频 `D-1 day_end_views - D-7 day_end_views` 计算过去一周增量，累计播放量取 D-1 日末值，再按账号增量降序取前 10 |
| 所有社媒账号播放数增量 Bottom 10（过去 1 周） | 与上一条使用同一个全部历史视频样本和周区间增量口径，排除增量播放量为 `0` 的账号后按增量升序取前 10；负增量不额外排除 |

### ads_daily_social_video_monitor_rankings_v2（视频日榜）

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 榜单日期 | `dt` | 自然日 | 已完整结束的美东自然日 `D`；Superset 日期筛选字段，默认用 Advanced Time Range 把实例 `now` 后移 5 小时再截日，严格筛一个完整美东日 | 单日筛选 | 不聚合 |
| 对比基准日期 | `target_dt` | 自然日 | `D-1`；既是增量对比基准，也是 T-1 发布视频池的发布日期 | 无 | 不聚合 |
| 榜单类型 | `ranking_type` | 榜单 | `t1_published_video_top`（`published_dt = D-1`）/ `historical_video_top`（`published_dt < D-1`） | 按目标榜单过滤 | 两类榜单不可一起聚合 |
| 排名 | `rank_no` | 视频 | 每个 `dt + ranking_type` 按增量降序生成 `1-10` | 无 | 不聚合 |
| 渠道 | `channel` | 账号 | 固定枚举 `youtube / tiktok / ins`；账号按 `channel + creator_id` 区分，不做跨平台合并 | 无 | 不聚合 |
| 视频 | `video_id` / `video_key` / `video_title` / `video_link` | 视频 | `video_key = channel + ':' + video_id`；标题 YouTube / TikTok 取 `title`，Instagram 取 `caption`；三端映射见 `docs/rules/video-mapping.md` | 无 | 不聚合 |
| 账号 | `account_name` / `account_handle` / `account_url` | 账号 | 三端映射见 `docs/rules/video-mapping.md` 的「视频与账号展示字段」 | 无 | 不聚合 |
| 增量播放量 | `view_increment` | 视频 | D 日末累计播放量减 D-1 日末累计播放量 | 无 | 只是当日 Top 10 的截断样本，不能当全量增量跨天求和 |
| 累计播放量 | `total_view_count` | 视频 | D 日末累计播放量 | 无 | 日末累计值，不可跨天求和 |
| 发布时间 | `published_ts_utc` / `published_dt` | 视频 | UTC 时间戳，及按 `America/Toronto` 转换的自然日 | 无 | 不聚合 |

补充说明：

- 物理日榜只覆盖两类视频增量 Top 10（每天最多 20 行）；Bottom 10 与两张账号周榜仍由 Superset 虚拟 dataset 271 动态计算，不落库
- 周榜增量范围为 `D-7 ~ D-1`，样本为截至 `D-1` 已发布的所有历史视频；账号增量为 `SUM(单视频 D-1 日末累计 - 单视频 D-7 日末累计)`，累计播放量为 `SUM(单视频 D-1 日末累计)`，按 `channel + creator_id` 分组
- YouTube 范围沿用 Social Content v2 的 10 个测试 / 内部频道排除规则

### Superset 展示字段

| Chart | 展示字段顺序 |
|------|------|
| T-1 发布视频增量 Views Top 10 | `dt`、排名、渠道、视频链接、视频标题、增量播放量、累计播放量、发布时间（美东） |
| T-1 发布视频增量 Views Bottom 10（不含 0） | 与 T-1 发布视频增量 Views Top 10 完全一致 |
| T-1 前发布视频增量 Views Top 10 | `dt`、排名、渠道、视频链接、视频标题、增量播放量、累计播放量、发布时间（美东） |
| 所有社媒账号播放数增量 Top / Bottom 10（过去 1 周） | `dt`、`week range`、排名、渠道、账号名称、账号 URL、增量播放量、累计播放量 |

- `week range` 展示 `D-7 ~ D-1`，例如 `dt=2026-07-12` 时展示 `2026-07-05 ~ 2026-07-11`
- `target_dt=D-1` 保留为物理日榜的计算字段，但不在 Chart 中展示
- Chart 中的 `dt` 统一按 `YYYY-MM-DD` 展示，不显示时分秒
- 视频链接与账号 URL 均展示为可点击且在新窗口打开的超链接

### Lark 推送字段

| 榜单 | 推送字段 |
|------|------|
| T-1 发布视频增量 Views Top 5 | 视频链接、增量播放量 |
| T-1 前发布视频增量 Views Top 5 | 视频链接、增量播放量 |

## 依赖

- 播放事实：`decom.dws_social_video_state_daily_v2`
- 视频维表：`decom.dim_video`；承接三端视频 / 账号展示属性和 Social Content 范围标记
- 视频日榜：`decom.ads_daily_social_video_monitor_rankings_v2`
- 展示字段映射：`docs/rules/video-mapping.md`
- 脚本：`jobs/dim/dim_video.py`, `jobs/ads/ads_daily_social_video_monitor_rankings_v2.py`, `jobs/ads/ads_daily_ecn_lark_push.py`
- DDL：`ddl/dim/dim_video.sql`, `ddl/ads/ads_daily_social_video_monitor_rankings_v2.sql`
- 编排：`env/etl_config.json` 中的独立表级入口 `ads_daily_social_video_monitor_rankings_v2`
- 性能：物理视频日榜每个 `dt` 只读取 D / D-1 两个状态分区，排名结果持久化后每天最多 20 行；虚拟周榜利用日末全量快照，即使统计所有历史视频也只读取 D / D-1 / D-7 三个必要分区
- Superset chart：`679 / 681` 使用物理 dataset 272 和 `dt` 单日筛选；`680 / 682 / 683` 使用固定计算最近完整日 `dt` 的虚拟 dataset 271
