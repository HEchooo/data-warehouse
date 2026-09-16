# 内容维度日报

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | 内容维度日报 | 内容维度日报（日×帖子） |
| 指标 | post_exposure_uv | 帖子曝光 UV、内容曝光人数 |
| 指标 | read_rate | 阅读率 |
| 指标 | like_rate | 点赞率 |
| 指标 | follow_rate | 关注率 |
| 指标 | tryon_total_count | 帖子试穿次数 |

## 适用范围

- 业务线：`Alvin's Club App`
- 适合：按帖子、内容模块和专栏查询每日曝光、阅读、试穿与互动表现。
- 不适合：平台整体内容趋势应使用[平台整体内容表现](../平台整体内容表现/fact.md)；专栏汇总应使用[专栏追踪明细](../专栏追踪明细/fact.md)。
- 需要澄清：需要确认帖子、模块或专栏范围，以及日期和指标。

## 报表

### ads_daily_post_performance

- 表：`my-project-8584-jetonai.decom.ads_daily_post_performance`
- 状态：`active`
- 粒度：`dt × post_id × module × column_id`
- 时区：`America/Toronto`
- 数据范围：App 帖子在模块和专栏中的日表现。
- 必要过滤：按目标帖子、模块或专栏过滤。
- 不包含：平台级汇总和非帖子内容。
- Dashboard：[【AI-Fashion】内容维度日报（日×帖子）](https://bi.alvinclub.ca/superset/dashboard/94/)（id=94）

## 字段

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 帖子 | `post_id` / `post_code` / `post_name` / `creator` | 帖子 | 由 `post_code` 经 `dim_post` 映射得到 | 按目标帖子过滤 | 不聚合 |
| 模块与专栏 | `module` / `column_id` / `column_name` | 专栏 | `column_id` 经 `dim_column` 映射为 `column_name` | 按目标模块或专栏过滤 | 不聚合 |
| 帖子曝光 UV | `post_exposure_uv` | 已登录用户 | 事件 `v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail`, `v_star_post_feeds`, `v_magazine_post_feeds`, `v_brand_post_feeds` 的 `COUNT(DISTINCT prop_user_id)` | 仅已登录事件 | 不可跨天或跨帖子求和 |
| 点赞次数 | `like_total_count` | 点赞记录 | 服务端表 `v3_decom.community_post_like`，按 `gmt_create` 切天后计数，经 `dim_post` 映射回 `post_code / column_id` | 无 | 可跨天、跨帖子求和 |
| 点赞率 | `like_rate` | 已登录用户 | 点赞已登录用户数 / `post_exposure_uv`；分母为 0 时返回 `0` | 无 | 不可 `AVG`；跨天、跨帖子须用汇总分子分母重算 |
| 阅读率 | `read_rate` | 已登录用户 | 阅读已登录用户数 / `post_exposure_uv`；阅读事件 `r_star_post_detail`, `r_magazine_post_detail`, `r_brand_post_detail`, `r_kol_post_detail`；分母为 0 时返回 `0` | 仅已登录事件 | 不可 `AVG`；跨天、跨帖子须用汇总分子分母重算 |
| 关注次数 | `follow_total_count` | 关注记录 | 服务端表 `v3_decom.community_star_follow`，按 `gmt_create` 切天后计数；服务端事实没有帖子主键，按同日「同用户 + 同专栏 + 关注前最后一次帖子详情曝光」做 last-touch 归因到帖子 | 无 | 可跨天、跨帖子求和 |
| 关注率 | `follow_rate` | 已登录用户 | 归因关注已登录用户数 / `post_exposure_uv`；分母为 0 时返回 `0` | 无 | 不可 `AVG`；跨天、跨帖子须用汇总分子分母重算 |
| 试穿次数 | `tryon_total_count` | 试穿任务 | 服务端表 `v3_decom.common_ext_ai_task`，`task_type IN ('TRY_ON', 'STYLE_ME')` 且 `request_param.postId` 非空，按任务行计数 | 无 | 可跨天、跨帖子求和 |

补充说明：
- 关注的 last-touch 归因逻辑已沉到 `dws_post_interaction_daily`，不在 ADS 现算
- `ads_daily_post_performance` 当前只消费 `dws_post_interaction_daily.platform = 'all'` 的总计口径，避免把跨平台 UV 直接相加

## 依赖

- 直接消费：`dim_post`, `dim_column`, `dws_post_interaction_daily`
- 上游来源：`dwd_event_log`, `v3_decom.community_post_like`, `v3_decom.community_star_follow`, `v3_decom.common_ext_ai_task`
- 实体脚本：`jobs/dim/dim_post.py`, `jobs/dim/dim_column.py`
- 实体 DDL：`ddl/dim/dim_post.sql`, `ddl/dim/dim_column.sql`
- 中间层脚本：`jobs/dws/dws_post_interaction_daily.py`
- 中间层 DDL：`ddl/dws/dws_post_interaction_daily.sql`
- 脚本：`jobs/ads/ads_daily_post_performance.py`
- DDL：`ddl/ads/ads_daily_post_performance.sql`
- 编排：`env/etl_config.json` 已注册独立 `ADS` 任务 `ads_daily_post_performance`；不再继续挂共享 `ADS` 旧入口
