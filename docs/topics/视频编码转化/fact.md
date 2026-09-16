# 视频编码转化

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | 视频编码转化 | 视频编码转化看板 |
| 指标 | 搜索用户数 | 编码搜索人数 |
| 指标 | 播放->搜索转化率 | 播放搜索转化率 |

## 适用范围

- 业务线：`Alvin's Club App`, `ECN 视频`, `TikTok 橱窗号`
- 适合：查询视频播放到编码搜索、注册和 App 行为的转化。
- 不适合：视频播放和点赞趋势应使用[Social Content 社媒表现](../【Social Content】社媒表现数据看板/fact.md)。
- 需要澄清：需要确认编码任务、统计范围、日期和转化漏斗阶段。

## 报表

### ads_daily_video_code_conversion

- 表：`my-project-8584-jetonai.decom.ads_daily_video_code_conversion`
- 状态：`active`
- 粒度：`dt × stat_scope × task_id`
- 时区：`America/Toronto`
- 数据范围：视频播放、编码搜索、注册和 App 行为转化。
- 必要过滤：整体结果使用文档定义的 `overall` 统计范围；任务分析按 `task_id` 过滤。
- 不包含：视频自身的播放、点赞趋势分析。
- Dashboard：[【AI-Fashion】视频编码转化看板](https://bi.alvinclub.ca/superset/dashboard/107/)（id=107）

### `dws_video_code_conversion_daily`

- 粒度：`dt × task_id`
- 说明：视频编码转化主题中间层，不是最终报表层

### `dws_video_code_search_user_daily`

- 粒度：`dt × promotion_code × user_id`
- 说明：视频编码搜索用户级中间层，用于 task 粒度搜索指标聚合和 overall 全局去重

## 字段

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 统计范围 | `stat_scope` | — | `task_id` 表示任务粒度行，`overall` 表示同日全局去重的汇总行 | 全局结论取 `stat_scope = 'overall'` | `overall` 与 `task_id` 行不可一起聚合 |
| 自动发布任务 ID | `task_id` | 发布任务 | 来自 `videos.video_upload_tasks.task_id`；仅 `stat_scope = 'task_id'` 时非空 | 单任务分析按 `task_id` 过滤 | 不聚合 |
| 8 位编码 | `promotion_code` | 发布任务 | `JSON_VALUE(video_upload_tasks.ext_info, '$.promotion_code')`；仅 `stat_scope = 'task_id'` 时非空 | 无 | 不聚合 |
| 播放增量 | `play_view_increment` | 视频 | 任务关联视频当日播放增量，按 `task_id -> dim_auto_publish_video_task.video_key -> dws_video_daily.daily_view_increment` 聚合 | 无 | 可跨天求和；不可混合 `overall` 与 `task_id` 行 |
| 搜索用户数 | `search_user_count` | 已登录用户 | 当日 `event_name IN ('c_home_search','c_shop_search','c_tag')` 且搜索内容为该 8 位编码的 `COUNT(DISTINCT user_id)`；`overall` 行在同日全部已映射编码上按用户全局去重 | `REGEXP_CONTAINS(JSON_VALUE(ext,'$.search_content'), r'^\d{8}$')` | 不可跨天或跨任务直接求和；跨范围须回 `dws_video_code_search_user_daily` 重新去重 |
| 首日访问搜索用户数 | `first_active_search_user_count` | 已登录用户 | 搜索用户中 `dim_user_first_active.first_active_date = dt` 的去重用户数 | 同上 | 不可跨天或跨任务直接求和 |
| 播放->搜索转化率 | `play_to_search_conversion_rate` | 已登录用户 / 视频 | `search_user_count / play_view_increment`；`play_view_increment <= 0` 时返回 `0` | 无 | 不可 `AVG`；跨天须先汇总分子分母再重算 |
| 播放->下载转化率 | `play_to_download_conversion_rate` | 已登录用户 / 视频 | `first_active_search_user_count / play_view_increment`；`play_view_increment <= 0` 时返回 `0` | 无 | 不可 `AVG`；跨天须先汇总分子分母再重算 |

中间层落地位置：搜索用户明细在 `decom.dws_video_code_search_user_daily`（`dt + promotion_code + user_id`，含 `is_first_active_today` 首活标记），任务级播放与搜索指标在 `decom.dws_video_code_conversion_daily`。

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
