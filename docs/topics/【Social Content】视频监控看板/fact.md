# 【Social Content】视频监控看板

## 报表

- Dashboard：[【Social Content】视频监控看板](https://bi.alvinclub.ca/superset/dashboard/122/)（id=122）
- 视频 Top 10 日榜表：`decom.ads_daily_social_video_monitor_rankings_v2`
- 表名中的 `v2` 沿用上游 `dws_social_video_state_daily_v2` 播放快照链路命名；不存在对应的排名表 v1
- 视频 Top 10 Superset 物理 dataset：`ads_daily_social_video_monitor_rankings_v2`（id=272，主时间列 `dt`）
- 其他榜单 Superset 虚拟 dataset：`virtual_social_content_video_monitor_rankings_v2`（id=271）
- 视频日榜粒度：`dt + ranking_type + rank_no`
- 视频日榜：每天固定产出 2 类榜单，每类 10 行

## 字段

### 榜单口径

| 需求字段 | 定义 |
|------|------|
| T-1 发布视频增量 Views Top 10 | 播放统计日期为 `D`；取 `published_dt = D-1` 的视频，以 `D day_end_views - D-1 day_end_views` 作为 D 当日增量播放量，按增量降序取前 10；累计播放量为 D 日末累计值 |
| T-1 发布视频增量 Views Bottom 10 | 与上一条使用同一视频 cohort，排除增量播放量为 `0` 的视频后按增量升序取前 10；负增量不额外排除 |
| T-1 之前发布视频增量 Views Top 10 | 播放统计日期为 `D`；取 `published_dt < D-1` 的视频，按 D 相比 D-1 的增量播放量降序取前 10 |
| 过去一周新发布视频账号增量 Views Top 10 | 账号按 `channel + creator_id` 区分；取 `published_dt` 在 `T-7 ~ T-1` 的视频，按账号汇总这些视频在 T-1 当天的播放增量与 T-1 日末累计播放量，再按增量降序取前 10 |
| 过去一周新发布视频账号增量 Views Bottom 10 | 与上一条使用同一账号样本，按增量升序取前 10；需求未要求排除 `0`，因此保留零增量与负增量账号 |

### 视频 Top 10 日榜字段

| 需求字段 | 落地字段 | 口径 |
|------|------|------|
| 播放统计日期 | `dt` | 已完整结束的美东自然日 `D`；Superset 日期筛选字段，默认 `Last day` |
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

- “过去一周”限定新发布视频样本范围；账号的“增量 Views”仍指这些视频在 T-1 当天的播放增量，不是七天累计增量
- YouTube 范围沿用 Social Content v2 的 10 个测试 / 内部频道排除规则
- 其余 3 类榜单仍由 Superset 虚拟 dataset 271 动态计算；本轮历史化只覆盖两类视频增量 Views Top 10

## 依赖

- 播放事实：`decom.dws_social_video_state_daily_v2`
- 视频维表：`decom.dim_video`；承接三端视频 / 账号展示属性和 Social Content 范围标记
- 视频日榜：`decom.ads_daily_social_video_monitor_rankings_v2`
- 展示字段映射：`docs/rules/video-mapping.md`
- 脚本：`jobs/dim/dim_video.py`, `jobs/ads/ads_daily_social_video_monitor_rankings_v2.py`
- DDL：`ddl/dim/dim_video.sql`, `ddl/ads/ads_daily_social_video_monitor_rankings_v2.sql`
- 编排：`env/etl_config.json` 中的独立表级入口 `ads_daily_social_video_monitor_rankings_v2`
- 性能：每个播放统计日只读取 D / D-1 两个视频状态分区，排名结果持久化后每天最多 20 行；Superset 不再为这两张图扫描并重算历史视频事实
- Superset chart：`679 / 681` 使用物理 dataset 272 和播放统计日期筛选；`680 / 682 / 683` 仍使用虚拟 dataset 271
