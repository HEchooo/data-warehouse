# 【Social Content】橱窗号数据

## 报表

- Dashboard：[【Social Content】橱窗号数据（账号维度）](https://bi.alvinclub.ca/superset/dashboard/130/)（id=130）
- Dashboard：[【Social Content】橱窗号数据（视频维度）](https://bi.alvinclub.ca/superset/dashboard/131/)（id=131）
- 账号维度表：`decom.ads_daily_social_affiliate_creator`，粒度 `dt × creator_id`
- 视频维度表：`decom.ads_daily_social_affiliate_video`，粒度 `dt × video_key`
- Superset 物理 dataset：`ads_daily_social_affiliate_creator`（id=295）、`ads_daily_social_affiliate_video`（id=296），主时间列均为 `dt`
- Superset chart：`721`（账号维度日报 table）、`722`（视频维度日报 table）
- 橱窗号范围：`videos.users.usage_type = 'tiktok-affiliate'`，当前 10 个账号，只有 TikTok 有这类账号
- 视频范围：排除账号绑定前的存量视频（`dim_video.is_pre_binding_video = TRUE`，来源 `platform_video_tags` 的 `tag_type='origin' AND tag_value='pre_binding'`），只统计绑定后新发布的视频；1161 个橱窗号视频中排除 1114 个，剩 47 个，10 个账号全部保留
- 统计口径：所有互动指标同时给出 `当日增量` 与 `日末累计`；增量 = `D 日末累计 - D-1 日末累计`
- `dt` 为已完整结束的美东自然日；Superset 默认用 Advanced Time Range 将实例 `now` 向后平移 5 小时后截日，严格筛选一个完整美东日

## 字段

### 账号维度需求字段

| 需求字段 | 落地字段 | 口径 |
|------|------|------|
| 统计日 | `dt` | 美东自然日 |
| 渠道 | `channel` | 固定 `tiktok` |
| 账号 | `creator_id` / `account_name` / `account_handle` / `account_url` | `creator_id = videos.users.id`；展示字段取账号日快照，`account_handle = users.username` |
| 发布的视频数 | `new_video_count` / `total_video_count` | 当日新发布（`dim_video.published_dt = dt`）与截至当日累计已发布（`published_dt <= dt`） |
| 播放量 | `view_increment` / `total_view_count` | 账号下全部历史视频的当日增量之和、日末累计之和 |
| 点赞数 | `like_increment` / `total_like_count` | 同上 |
| 评论数 | `comment_increment` / `total_comment_count` | 同上 |
| 分享数 | `share_increment` / `total_share_count` | 同上 |
| 新增粉丝数 | `follower_increment` / `total_follower_count` | 账号日快照 `day_end_follower_count` 相减；前一日无快照时为空值，不按 0 起算 |

### 视频维度需求字段

| 需求字段 | 落地字段 | 口径 |
|------|------|------|
| 统计日 | `dt` | 美东自然日 |
| 视频 | `video_id` / `video_key` / `video_link` / `published_ts_utc` / `published_dt` | `video_key = 'tiktok:' + video_id`；链接与发布时间取 `dim_video` |
| 视频标题 | `video_title` | `dim_video.video_title`，TikTok 取 `videos.videos.title`；映射统一引用 `docs/rules/video-mapping.md` |
| 播放量 | `view_increment` / `total_view_count` | 单视频当日增量与日末累计 |
| 点赞数 | `like_increment` / `total_like_count` | 同上 |
| 评论数 | `comment_increment` / `total_comment_count` | 同上 |
| 分享数 | `share_increment` / `total_share_count` | 同上 |
| 账号 | `creator_id` / `account_name` / `account_handle` / `account_url` | 与账号维度同源 |

补充说明：

- 视频当天首发时前一日没有日末状态，当日增量等于日末累计值
- 账号维度按 `dt × 橱窗号` 补齐骨架，账号当日没有任何视频也出零值行；粉丝字段保持空值语义，不补 0
- 橱窗号名单取账号日快照的最新分区；名单变化会同步影响历史分区的账号集合

### 数据边界

| 边界 | 说明 |
|------|------|
| 报表起点 | `2026-08-12`（美东），即橱窗号视频在 `dws_social_video_snapshot_daily_v2` 的首个真实快照日；更早分区没有采集数据，不产出 |
| 存量视频排除 | 橱窗号视频绝大多数是账号绑定前的存量视频（每个账号 84%~98%），已按 `is_pre_binding_video` 排除。排除后每个账号只剩 3~9 个视频，历史所有分区的播放、点赞、评论、分享、视频数都会大幅下降；粉丝数取账号快照，不受影响 |
| 采集范围扩大 | 橱窗号视频采集覆盖是逐步放开的：`08-12/08-13` 约 162 个视频、`08-14 ~ 08-16` 约 420 个、`08-17` 起才覆盖全部 1146 个视频。`08-14` 与 `08-17` 的播放、评论、分享增量包含新纳入采集视频的历史存量，不代表当日真实增量，`08-17` 之前的增量口径不可用于趋势判断 |
| 粉丝数历史 | `videos.users` 只有当前状态，没有历史，粉丝快照从 `dws_social_creator_snapshot_daily` 首次运行日（`2026-08-21`）起逐日累积；`2026-08-22` 起才有第一个可用的 `follower_increment`，更早分区的粉丝字段恒为空 |
| 分享数平台差异 | `day_end_shares` 只有 YouTube 保持空值（源表没有该字段）；TikTok 与 Instagram 与播放、点赞、评论一致按 0 起算。橱窗号只涉及 TikTok，两个维度的分享数字段都不会出现空值 |
| 历史评论/分享 | 两个日末字段是本次新增列，已于 `2026-08-21` 用 `--full-refresh` 从 `2026-03-13` 全量回刷完成，三端历史分区均已有值 |

## 依赖

- 账号快照：`decom.dws_social_creator_snapshot_daily`（`videos.users` 逐日快照，只重写当天分区，历史不可回溯）
- 视频播放事实：`decom.dws_social_video_snapshot_daily_v2` → `decom.dws_social_video_state_daily_v2`
- 视频维表：`decom.dim_video`
- 脚本：`jobs/dws/dws_social_creator_snapshot_daily.py`, `jobs/ads/ads_daily_social_affiliate_creator.py`, `jobs/ads/ads_daily_social_affiliate_video.py`
- DDL：`ddl/dws/dws_social_creator_snapshot_daily.sql`, `ddl/ads/ads_daily_social_affiliate_creator.sql`, `ddl/ads/ads_daily_social_affiliate_video.sql`
- 编排：`env/etl_config.json` 中的独立表级入口 `dws_social_creator_snapshot_daily`、`ads_daily_social_affiliate_creator`、`ads_daily_social_affiliate_video`
- 展示字段映射：`docs/rules/video-mapping.md`
- 性能：两张 ADS 每个 `dt` 只读取 D / D-1 两个日末状态分区，并先按橱窗号名单收窄到 10 个账号；账号快照每天只写 1 个分区、10 行
