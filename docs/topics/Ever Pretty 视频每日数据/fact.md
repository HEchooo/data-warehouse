# Ever Pretty 视频每日数据

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | Ever Pretty 视频每日数据 | Ever Pretty Demo |

## 适用范围

- 业务线：`ECN 视频`
- 适合：固定八位 AI 博主、九个平台账号在美东 2026-09-01 起发布的视频，每天累计表现与记录变化。
- 不适合：ECN 全量正式号首采正播放统计，见[ECN 数据看板](../ECN%20数据看板/fact.md)。
- 需要澄清：扩大账号范围或追溯换绑前历史时，须重新核验账号 ID 和生效日期。

## 报表

### ads_daily_ever_pretty_video

- 表：`my-project-8584-jetonai.decom.ads_daily_ever_pretty_video`
- 状态：`active`
- 粒度：一行代表一个 `dt × video_key`。
- 时区：`America/Toronto`。
- 数据范围：名单来自 `env/ever_pretty_accounts.json`，按平台与稳定账号 ID 关联。包含这些账号的全部入库视频，不额外要求单视频品牌标记或正播放。
- 必要过滤：默认发布日 `published_dt >= 2026-09-01`；统计日 `dt >= published_dt`。当天未完整结束时标记 `is_complete_day = FALSE`。
- 不包含：已停用的同名账号、维表已排除的绑定前存量视频、没有平台视频 ID 的 ECN 任务。不能把缺失回流当零发布。
- 交付：本地 Superset 趋势 Demo，默认整体单条曲线，按博主 → 平台 → 视频标题级联单选。展示累计播放、记录增量、采集视频数、点赞和评论；尚未接入生产定时刷新。

## 字段

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 统计日期 | `dt` | 自然日 | 美东统计日期 | 无 | 不聚合 |
| 博主 | `blogger_name` / `blogger_id` | AI 博主 | 核验后固定名单 | 无 | 博主数按 ID 去重 |
| 平台与账号 | `channel` / `creator_id` / `account_handle` | 平台账号 | 平台+稳定 ID；handle 仅展示 | 无 | 不用 handle 做主键 |
| 视频 | `video_key` / `video_id` / `video_title` / `video_link` | 平台视频 | 复用 dim_video 展示属性 | 无 | 视频数按 video_key 去重，不做跨平台内容去重 |
| 发布日期 | `published_dt` / `published_ts_utc` / `published_time_basis` | 平台视频 | 优先平台主数据；仅 tag 的占位视频明确标为待回流 | 真实发布时间分析排除 tag 时间占位 | 非首次正播放采集日 |
| 累计播放 | `total_views` | 平台视频 | 该日最后已知累计播放；首次真实采集前 NULL | 无 | 不可跨日相加，同一日不同视频可相加 |
| 累计互动 | `total_likes` / `total_comments` / `total_shares` | 平台视频 | 同累计播放；YouTube 分享无源字段，保持 NULL | 无 | 不可跨日相加；混平台分享统计须保留缺失提示 |
| 较前日记录增量 | `view_increment` / `like_increment` / `comment_increment` / `share_increment` | 平台视频 | 当日已知累计减前日已知累计；任一日未知则 NULL；保留负值 | 无 | 缺采期间沿用零差值不代表真实零增长；首采 NULL 不能补0或当当日累计 |
| 采集情况 | `has_snapshot` / `last_snapshot_dt` / `collection_status` | 视频日 | snapshot证明当天采集；state沿用值单独标识 | 无 | 不聚合 |
| 状态可用性 | `state_available` / `is_complete_day` | 视频日 | 上游状态是否已到、自然日是否结束 | 无 | 不聚合 |

## 关联与边界

- 2026-10-12 核验仍为 8 博主、9 个启用绑定。Viviana Ricci 使用 `leo_s_little_world`，不按同名把已停用旧号 `viviana_ricci` 混入。
- UnfilteredMomFinds TikTok 是外部发布渠道，当前没有可关联的 BigQuery 视频。ECN 的完成状态、无 ID 的外部渠道记录不证明实际平台发布。看板说明保留第九个账号的缺口。
- `dim_video` 对 tag-only 视频使用正式号标记时间兜底；本表按“标题和链接皆空且发布时间等于tag时间”标注该情况，不将其伪装成已核实的平台发布时间。
- 本主题复用 DIM / DWS，只做限定名单的展示投影、逐日展开与差值，不新增空转 ODS/DWD/DIM/DWS；数据口径复用既有视频日状态链路。

## 依赖

- 上游：`decom.dim_video`、`decom.dws_social_video_state_daily_v2`、`decom.dws_social_video_snapshot_daily_v2`。
- 脚本：`jobs/ads/ads_daily_ever_pretty_video.py`。
- SQL：`jobs/ads/ads_daily_ever_pretty_video.sql`。
- DDL：`ddl/ads/ads_daily_ever_pretty_video.sql`。
- 规则：`docs/rules/video-mapping.md`、`docs/rules/data.md`、`docs/rules/layering.md`。
- 本地启动说明：`demo/ever_pretty/README.md`。

## 本次范围调整

2026-10-12 按用户要求将发布起点从9月30日扩大至9月1日，保留同一套已核验九账号映射。重建 Demo 表共145个视频、2570条视频日记录；9月1日至10月11日有数据，9月30个日期均有真实采集。旧表已保存七天备份；上游、生产调度未改动。
