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

### 主题共用口径

| 字段 | 口径 |
|------|------|
| user_group | 用户分群；固定取值为 `新用户` / `老用户`；新用户 = `first_active_date = dt`，老用户 = `first_active_date < dt` |
| dau_uv | 当天活跃已登录用户数；基于 `decom.dws_user_activity_daily` 按 `dt × user_id` 去重后的 cohort，按 `COUNT(DISTINCT user_id)` 统计 |
| tryon_uv | 当天发生 AI 试穿的已登录用户数；复用 `decom.dws_user_activity_daily.used_ai_tryon = 1` 的用户集合，按 `COUNT(DISTINCT user_id)` 统计 |

### 新用户分版本试穿明细

| 字段 | 口径 |
|------|------|
| app_version | 用户当天命中过的完整版本号；取自 `decom.dwd_event_log.prop_app_version`；空值统一落为 `unknown` |
| dau_uv | 当天活跃已登录用户数；按 `dt × user_group × app_version` 统计 `COUNT(DISTINCT user_id)` |
| tryon_uv | 当天发生 AI 试穿的已登录用户数；按 `dt × user_group × app_version` 统计 `COUNT(DISTINCT IF(used_ai_tryon = 1, user_id, NULL))` |
| tryon_penetration_rate | 试穿渗透率；`tryon_uv / dau_uv` |

补充说明：

- 新老用户分群复用 `decom.dim_user_first_active` 下沉后的 `first_active_date`，不在 ADS 直接现算“首日访问”
- `tryon_uv` 与平台整体内容表现看板一致，统一基于服务端 `v3_decom.common_ext_ai_task` 的 `task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')`
- 该报表当前只落地基础事实 `DAU / 试穿 UV`，不物化 `试穿渗透率`
- 该看板是独立 dashboard，不与 `【AI-Fashion】平台整体内容表现` 共用主题文档
- 版本口径按“用户当天命中过的所有完整版本号”展开；同一用户同一天若上报多个版本，会在多个版本下分别计入，因此按版本汇总后的 `dau_uv / tryon_uv` 之和默认不会回对到主题总 UV
- `tryon_penetration_rate` 的分母为 0 时返回 `NULL`；跨天、跨版本或跨用户组必须汇总去重分子、分母后重算
- 版本明细报表默认只新增 chart 展示 `user_group = 新用户`，但底表仍同时保留 `新用户 / 老用户` 两类，方便后续复用

## 已落地字段

### 新老用户试穿渗透率

| 需求字段 | 落地表 | 落地字段 | 口径 |
|------|------|------|------|
| 用户分群 | `decom.ads_daily_user_tryon_penetration` | `user_group` | 固定取值为 `新用户 / 老用户` |
| 当天活跃已登录用户数 | `decom.ads_daily_user_tryon_penetration` | `dau_uv` | 基于 `decom.dws_user_activity_daily` 按 `dt × user_id` 去重后统计 |
| 当天试穿已登录用户数 | `decom.ads_daily_user_tryon_penetration` | `tryon_uv` | 基于 `decom.dws_user_activity_daily.used_ai_tryon = 1` 统计 |

### 新用户分版本试穿明细

| 需求字段 | 落地表 | 落地字段 | 口径 |
|------|------|------|------|
| 用户分群 | `decom.ads_daily_user_tryon_penetration_app_version` | `user_group` | 固定取值为 `新用户 / 老用户` |
| 完整版本号 | `decom.ads_daily_user_tryon_penetration_app_version` | `app_version` | 取自 `decom.dwd_event_log.prop_app_version`；空值统一落为 `unknown` |
| 当天活跃已登录用户数 | `decom.ads_daily_user_tryon_penetration_app_version` | `dau_uv` | 基于 `decom.dws_user_activity_app_version_daily` 按 `dt × user_group × app_version` 统计去重用户数 |
| 当天试穿已登录用户数 | `decom.ads_daily_user_tryon_penetration_app_version` | `tryon_uv` | 基于 `decom.dws_user_activity_app_version_daily.used_ai_tryon = 1` 按 `dt × user_group × app_version` 统计去重用户数 |
| 试穿渗透率 | `decom.ads_daily_user_tryon_penetration_app_version` | `tryon_penetration_rate` | `tryon_uv / dau_uv` |

补充说明：

- 版本明细链路代码、DDL 与调度配置已补齐；是否创建生产表、刷新历史分区并创建 Superset 资产，需要单独执行

## 依赖

- 上游：`decom.dim_user_first_active`, `decom.dwd_event_log`, `v3_decom.common_ext_ai_task`
- 中层事实：`decom.dws_user_activity_daily`, `decom.dws_user_activity_app_version_daily`
- 中层脚本：`jobs/dws/dws_user_activity_daily.py`, `jobs/dws/dws_user_activity_app_version_daily.py`
- 中层 DDL：`ddl/dws/dws_user_activity_daily.sql`, `ddl/dws/dws_user_activity_app_version_daily.sql`
- 终层脚本：`jobs/ads/ads_daily_user_tryon_penetration.py`, `jobs/ads/ads_daily_user_tryon_penetration_app_version.py`
- DDL：`ddl/ads/ads_daily_user_tryon_penetration.sql`, `ddl/ads/ads_daily_user_tryon_penetration_app_version.sql`
- 编排：`env/etl_config.json`（`dws_user_activity_daily`、`dws_user_activity_app_version_daily`、`ads_daily_user_tryon_penetration`、`ads_daily_user_tryon_penetration_app_version`）
