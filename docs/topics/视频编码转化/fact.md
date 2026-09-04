# 视频编码转化

## 报表

- 表：`ads_daily_video_code_conversion`
- 粒度：`dt × stat_scope × task_id`
- Dashboard：[【AI-Fashion】视频编码转化看板](https://bi.alvinclub.ca/superset/dashboard/107/)（id=107）

### `dws_video_code_conversion_daily`

- 粒度：`dt × task_id`
- 说明：视频编码转化主题中间层，不是最终报表层

### `dws_video_code_search_user_daily`

- 粒度：`dt × promotion_code × user_id`
- 说明：视频编码搜索用户级中间层，用于 task 粒度搜索指标聚合和 overall 全局去重

## 字段

| 需求字段 | 定义 |
|---|---|
| 日期 | 报表日期（`America/Toronto`） |
| task_id | 自动发布任务 ID（由 `videos.video_upload_tasks.task_id` 提供） |
| 8位编码 | `JSON_VALUE(videos.video_upload_tasks.ext_info, '$.promotion_code')` |
| 播放增量 | `task_id` 关联视频在 `dt` 当日的播放增量（复用 `dws_video_daily.daily_view_increment`） |
| 搜索用户数 | 当日发生 `event_name IN ('c_home_search','c_shop_search','c_tag')` 且 `search_content` 为该 8 位编码的去重已登录用户数 |
| 首日访问搜索用户数 | 搜索用户中满足 `dim_user_first_active.first_active_date = dt` 的去重用户数 |
| 播放->搜索转化率 | `search_user_count / play_view_increment` |
| 播放->下载转化率 | `first_active_search_user_count / play_view_increment` |
| 整体口径 | 同日对已映射到视频任务的全部 8 位编码搜索用户按用户全局去重后的同口径指标 |

## 已落地字段

| 需求字段 | 落地表 | 落地字段 | 口径 |
|---|---|---|---|
| 日期 | `decom.ads_daily_video_code_conversion` | `dt` | 按 `America/Toronto` 切天 |
| 粒度标记 | `decom.ads_daily_video_code_conversion` | `stat_scope` | `task_id` 表示任务粒度，`overall` 表示整体汇总粒度 |
| task_id | `decom.ads_daily_video_code_conversion` | `task_id` | 仅 `stat_scope = 'task_id'` 时非空 |
| 8位编码 | `decom.ads_daily_video_code_conversion` | `promotion_code` | 仅 `stat_scope = 'task_id'` 时非空 |
| 搜索用户明细 | `decom.dws_video_code_search_user_daily` | `dt + promotion_code + user_id` | 事件过滤 `event_name IN ('c_home_search','c_shop_search','c_tag')` + `REGEXP_CONTAINS(JSON_VALUE(ext,'$.search_content'), r'^\\d{8}$')`，按 `America/Toronto` 切天后对用户去重 |
| 搜索当日首活标记 | `decom.dws_video_code_search_user_daily` | `is_first_active_today` | `dim_user_first_active.first_active_date = dt` |
| 播放增量 | `decom.dws_video_code_conversion_daily` | `play_view_increment` | 按 `task_id -> dim_auto_publish_video_task.video_key -> dws_video_daily.daily_view_increment` 聚合 |
| 搜索用户数 | `decom.dws_video_code_conversion_daily` | `search_user_count` | 基于 `decom.dws_video_code_search_user_daily` 按 `promotion_code -> task_id` 聚合，并按 `COUNT(DISTINCT user_id)` |
| 首日访问搜索用户数 | `decom.dws_video_code_conversion_daily` | `first_active_search_user_count` | 基于 `decom.dws_video_code_search_user_daily.is_first_active_today` 按 `promotion_code -> task_id` 聚合 |
| 播放->搜索转化率 | `decom.ads_daily_video_code_conversion` | `play_to_search_conversion_rate` | `search_user_count / play_view_increment`；`play_view_increment <= 0` 时返回 `0` |
| 播放->下载转化率 | `decom.ads_daily_video_code_conversion` | `play_to_download_conversion_rate` | `first_active_search_user_count / play_view_increment`；`play_view_increment <= 0` 时返回 `0` |
| 整体口径 | `decom.ads_daily_video_code_conversion` | `stat_scope = 'overall'` 行 | 分母对同日全部 task 粒度行求和；分子基于 `decom.dws_video_code_search_user_daily` 在同日全部已映射编码上按用户全局去重后再计算 |

补充说明：
- 用户原始描述中的 `decom.dwd_event_id` 在当前仓库/数据集中不存在；本主题实际使用 `decom.dwd_event_log`
- 本主题 `ADS` 只消费 `DWS`，不直接读取 `DWD` 或外部源表
- 播放增量来自不经过 `dim_video` 的 `decom.dws_video_daily`，因此不会自动排除 pre-binding 视频，当前仍可能包含账号绑定前的存量视频
- 当前未做历史回填；若要让既有历史日期按新口径重刷，需要单独确认回填窗口

## 依赖

- 上游：`videos.video_upload_tasks`, `decom.dwd_event_log`, `decom.dws_video_daily`, `decom.dim_auto_publish_video_task`, `decom.dim_user_first_active`
- 中间表：`decom.dws_video_code_search_user_daily`, `decom.dws_video_code_conversion_daily`
- 报表表：`decom.ads_daily_video_code_conversion`
- Superset：物理 dataset `ads_daily_video_code_conversion`（id=237），chart `视频编码转化（日表）-明细表`（id=587）、`视频编码转化（日表）-整体播放->搜索转化率趋势`（id=588），dashboard `【AI-Fashion】视频编码转化看板`（id=107）
- 脚本：`jobs/dws/dws_video_code_search_user_daily.py`, `jobs/dws/dws_video_code_conversion_daily.py`, `jobs/ads/ads_daily_video_code_conversion.py`
- DDL：`ddl/dws/dws_video_code_search_user_daily.sql`, `ddl/dws/dws_video_code_conversion_daily.sql`, `ddl/ads/ads_daily_video_code_conversion.sql`
- 编排：`env/etl_config.json`, `etl_run.py`, `jobs/ads/ads_daily_video_code_conversion.py`
