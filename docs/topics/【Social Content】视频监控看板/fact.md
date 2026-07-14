# 【Social Content】视频监控看板

## 报表

- Dashboard：[【Social Content】视频监控看板](https://bi.alvinclub.ca/superset/dashboard/122/)（id=122）
- 视频 Top 10 日榜表：`decom.ads_daily_social_video_monitor_rankings_v2`
- 表名中的 `v2` 沿用上游 `dws_social_video_state_daily_v2` 播放快照链路命名；不存在对应的排名表 v1
- 视频 Top 10 Superset 物理 dataset：`ads_daily_social_video_monitor_rankings_v2`（id=272，主时间列 `dt`）
- 其他榜单 Superset 虚拟 dataset：`virtual_social_content_video_monitor_rankings_v2`（id=271，主时间列 `dt`）
- 视频日榜粒度：`dt + ranking_type + rank_no`
- 视频日榜：每天固定产出 2 类榜单，每类 10 行

## 字段

### 榜单口径

| 需求字段 | 定义 |
|------|------|
| T-1 发布视频增量 Views Top 10 | `dt=D`；取 `published_dt = D-1` 的视频，以 `D day_end_views - D-1 day_end_views` 作为 D 当日增量播放量，按增量降序取前 10；累计播放量为 D 日末累计值 |
| T-1 发布视频增量 Views Bottom 10 | 与上一条使用同一视频 cohort，排除增量播放量为 `0` 的视频后按增量升序取前 10；负增量不额外排除 |
| T-1 之前发布视频增量 Views Top 10 | `dt=D`；取 `published_dt < D-1` 的视频，按 D 相比 D-1 的增量播放量降序取前 10 |
| 过去一周新发布视频账号增量 Views Top 10 | 账号按 `channel + creator_id` 区分；取 `published_dt` 在 `D-7 ~ D-1` 且首尾都包含的视频，按账号汇总这些视频的 `D-1 day_end_views - D-7 day_end_views`，累计播放量取 D-1 日末值，再按增量降序取前 10 |
| 过去一周新发布视频账号增量 Views Bottom 10 | 与上一条使用同一个 `D-7 ~ D-1` 账号样本和周区间增量口径，排除增量播放量为 `0` 的账号后按增量升序取前 10；负增量不额外排除 |

### 视频 Top 10 日榜字段

| 需求字段 | 落地字段 | 口径 |
|------|------|------|
| `dt` | `dt` | 已完整结束的美东自然日 `D`；Superset 日期筛选字段，默认使用 Advanced Time Range 将实例 `now` 向后平移 5 小时后截日，严格筛选一个完整美东日 |
| 对比基准日期 | `target_dt` | `D-1`；用于计算 D 当日增量，也是 T-1 发布视频池的发布日期 |
| 榜单类型 | `ranking_type` | `t1_published_video_top` / `historical_video_top` |
| 排名 | `rank_no` | 每个 `dt + ranking_type` 按增量降序生成 `1-10` |
| 渠道 | `channel` | 固定枚举 `youtube / tiktok / ins`；账号按 `channel + creator_id` 区分，不做跨平台合并 |
| 视频 ID | `video_id` / `video_key` | `video_key = channel + ':' + video_id` |
| 视频标题 | `video_title` | YouTube / TikTok 取 `title`，Instagram 取 `caption`；三端映射统一引用 `docs/rules/video-mapping.md` |
| 视频链接 | `video_link` | 三端映射统一引用 `docs/rules/video-mapping.md` 的“视频与账号展示字段” |
| 账号名称 | `account_name` | 三端映射统一引用 `docs/rules/video-mapping.md` 的“视频与账号展示字段” |
| 账号用户名 / Handle | `account_handle` | 三端映射统一引用 `docs/rules/video-mapping.md` 的“视频与账号展示字段” |
| 账号 URL | `account_url` | 三端映射统一引用 `docs/rules/video-mapping.md` 的“视频与账号展示字段” |
| 增量播放量 | `view_increment` | D 日末累计播放量减 D-1 日末累计播放量 |
| 累计播放量 | `total_view_count` | D 日末累计播放量 |
| 发布时间 | `published_ts_utc` / `published_dt` | UTC 时间戳及按 `America/Toronto` 转换的自然日 |

补充说明：

- 周榜的发布范围和增量范围均为 `D-7 ~ D-1`：视频样本限定为该区间内发布，增量取区间结束日 D-1 日末累计减区间开始日 D-7 日末累计
- YouTube 范围沿用 Social Content v2 的 10 个测试 / 内部频道排除规则
- 其余 3 类榜单仍由 Superset 虚拟 dataset 271 动态计算；本轮历史化只覆盖两类视频增量 Views Top 10

### Superset 展示字段

| Chart | 展示字段顺序 |
|------|------|
| T-1 发布视频增量 Views Top 10 | `dt`、排名、渠道、视频链接、视频标题、增量播放量、累计播放量、发布时间（美东） |
| T-1 发布视频增量 Views Bottom 10（不含 0） | 与 T-1 发布视频增量 Views Top 10 完全一致 |
| T-1 前发布视频增量 Views Top 10 | `dt`、排名、渠道、视频链接、视频标题、增量播放量、累计播放量、发布时间（美东） |
| 过去一周新发布视频账号增量 Views Top / Bottom 10 | `dt`、`week range`、排名、渠道、账号名称、账号 URL、增量播放量、累计播放量 |

- `week range` 展示 `D-7 ~ D-1`，例如 `dt=2026-07-12` 时展示 `2026-07-05 ~ 2026-07-11`
- `target_dt=D-1` 保留为物理日榜的计算字段，但不在 Chart 中展示
- Chart 中的 `dt` 统一按 `YYYY-MM-DD` 展示，不显示时分秒
- 视频链接与账号 URL 均展示为可点击且在新窗口打开的超链接
- 周榜账号增量播放量为 `SUM(单视频 D-1 日末累计播放量 - 单视频 D-7 日末累计播放量)`，累计播放量为 `SUM(单视频 D-1 日末累计播放量)`；只汇总 `published_dt` 位于 `D-7 ~ D-1` 的视频，并按 `channel + creator_id` 分组

## 依赖

- 播放事实：`decom.dws_social_video_state_daily_v2`
- 视频维表：`decom.dim_video`；承接三端视频 / 账号展示属性和 Social Content 范围标记
- 视频日榜：`decom.ads_daily_social_video_monitor_rankings_v2`
- 展示字段映射：`docs/rules/video-mapping.md`
- 脚本：`jobs/dim/dim_video.py`, `jobs/ads/ads_daily_social_video_monitor_rankings_v2.py`
- DDL：`ddl/dim/dim_video.sql`, `ddl/ads/ads_daily_social_video_monitor_rankings_v2.sql`
- 编排：`env/etl_config.json` 中的独立表级入口 `ads_daily_social_video_monitor_rankings_v2`
- 性能：物理视频日榜每个 `dt` 只读取 D / D-1 两个状态分区，排名结果持久化后每天最多 20 行；虚拟周榜只读取 D / D-1 / D-7 三个必要快照分区，不扫描完整周区间
- Superset chart：`679 / 681` 使用物理 dataset 272 和 `dt` 单日筛选；`680 / 682 / 683` 使用固定计算最近完整日 `dt` 的虚拟 dataset 271
