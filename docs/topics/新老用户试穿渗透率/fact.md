# 新老用户试穿渗透率

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | 新老用户试穿渗透率 | 新老用户试穿看板 |
| 指标 | 试穿渗透率 | 试穿率 |
| 指标 | 试穿 UV | 试穿人数 |
| 指标 | DAU | 日活、日活用户数 |

## 适用范围

- 业务线：`Alvin's Club App`
- 适合：比较新老用户的试穿渗透率，并查看新用户按 App 版本拆分的试穿表现。
- 不适合：试穿点击次数应使用[穿搭工具点击使用](../穿搭工具点击使用/fact.md)；试穿后加购应使用[试穿结果转化](../试穿结果转化/fact.md)。
- 需要澄清：需要确认新老用户组、是否按 App 版本拆分和日期范围。

## 报表

### 新老用户试穿渗透率

- 表：`my-project-8584-jetonai.decom.ads_daily_user_tryon_penetration`
- 状态：`active`
- 粒度：`dt + user_group`
- 时区：`America/Toronto`
- 数据范围：新老用户试穿人数、DAU 和渗透率。
- 必要过滤：按目标 `user_group` 过滤。
- 不包含：App 版本维度。
- Dashboard：[【AI-Fashion】新老用户试穿渗透率](https://bi.alvinclub.ca/superset/dashboard/110/)（id=110）

### 新用户分版本试穿明细

- 表：`my-project-8584-jetonai.decom.ads_daily_user_tryon_penetration_app_version`
- 状态：`active`
- 粒度：`dt + user_group + app_version`
- 时区：`America/Toronto`
- 数据范围：新用户按 App 版本拆分的试穿表现。
- 必要过滤：`user_group = 'new_user'`，并按目标 `app_version` 过滤。
- 不包含：老用户版本拆分。
- Dashboard：[【AI-Fashion】新老用户试穿渗透率](https://bi.alvinclub.ca/superset/dashboard/110/)（id=110）

## 字段

### ads_daily_user_tryon_penetration

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 用户分群 | `user_group` | 已登录用户 | 固定取值 `新用户` / `老用户`；新用户 = `first_active_date = dt`，老用户 = `first_active_date < dt`，`first_active_date` 来自 `decom.dim_user_first_active` | 按目标分群过滤 | 两个分群不可与总量混用 |
| 活跃用户数 | `dau_uv` | 已登录用户 | 基于 `decom.dws_user_activity_daily` 按 `dt × user_id` 去重后 `COUNT(DISTINCT user_id)` | 无 | 不可跨天或跨分群直接求和 |
| 试穿用户数 | `tryon_uv` | 已登录用户 | `decom.dws_user_activity_daily.used_ai_tryon = 1` 的 `COUNT(DISTINCT user_id)`；试穿判定来自服务端 `v3_decom.common_ext_ai_task` 的 `task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')` | 无 | 不可跨天或跨分群直接求和 |
| 试穿渗透率 | 未落地，查询时按 `tryon_uv / dau_uv` 现算 | 已登录用户 | 分子 `tryon_uv`，分母 `dau_uv`；分母为 0 时返回 `NULL` | 同一 `dt + user_group` | 不可 `AVG`；跨天或跨分群须汇总去重分子分母后重算 |

### ads_daily_user_tryon_penetration_app_version

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 用户分群 | `user_group` | 已登录用户 | 与主表同口径；底表保留 `新用户 / 老用户`，看板默认只展示新用户 | `user_group = 'new_user'` | 两个分群不可与总量混用 |
| 完整版本号 | `app_version` | 已登录用户 | 用户当天命中过的完整版本号，取自 `decom.dwd_event_log.prop_app_version`；空值落为 `unknown` | 按目标版本过滤 | 同一用户当天多版本会在各版本重复计入，版本之和不回对主题总 UV |
| 活跃用户数 | `dau_uv` | 已登录用户 | 基于 `decom.dws_user_activity_app_version_daily` 按 `dt × user_group × app_version` 统计 `COUNT(DISTINCT user_id)` | 无 | 不可跨天、跨版本或跨分群直接求和 |
| 试穿用户数 | `tryon_uv` | 已登录用户 | `COUNT(DISTINCT IF(used_ai_tryon = 1, user_id, NULL))`，同粒度统计 | 无 | 不可跨天、跨版本或跨分群直接求和 |
| 试穿渗透率 | `tryon_penetration_rate` | 已登录用户 | `tryon_uv / dau_uv`；分母为 0 时返回 `NULL` | 无 | 不可 `AVG`；跨天、跨版本或跨分群须汇总去重分子分母后重算 |

补充说明：

- 新老用户分群复用 `decom.dim_user_first_active` 下沉后的 `first_active_date`，不在 ADS 直接现算“首日访问”
- 该看板是独立 dashboard，不与 `【AI-Fashion】平台整体内容表现` 共用主题文档
- 版本明细链路代码、DDL 与调度配置已补齐；是否创建生产表、刷新历史分区并创建 Superset 资产，需要单独执行

## 依赖

- 上游：`decom.dim_user_first_active`, `decom.dwd_event_log`, `v3_decom.common_ext_ai_task`
- 中层事实：`decom.dws_user_activity_daily`, `decom.dws_user_activity_app_version_daily`
- 中层脚本：`jobs/dws/dws_user_activity_daily.py`, `jobs/dws/dws_user_activity_app_version_daily.py`
- 中层 DDL：`ddl/dws/dws_user_activity_daily.sql`, `ddl/dws/dws_user_activity_app_version_daily.sql`
- 终层脚本：`jobs/ads/ads_daily_user_tryon_penetration.py`, `jobs/ads/ads_daily_user_tryon_penetration_app_version.py`
- DDL：`ddl/ads/ads_daily_user_tryon_penetration.sql`, `ddl/ads/ads_daily_user_tryon_penetration_app_version.sql`
- 编排：`env/etl_config.json`（`dws_user_activity_daily`、`dws_user_activity_app_version_daily`、`ads_daily_user_tryon_penetration`、`ads_daily_user_tryon_penetration_app_version`）
