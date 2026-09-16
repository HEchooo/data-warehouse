# 首页新老用户曝光点击

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 指标 | DAU | 日活、日活用户数、活跃用户数 |
| 指标 | feeds_exposure_uv | 首页 Feeds 曝光 UV、Feeds 曝光人数 |
| 指标 | feeds_click_uv | 首页 Feeds 点击 UV、Feeds 点击人数 |
| 维度 | 新用户 | 首日活跃用户 |

## 适用范围

- 业务线：`Alvin's Club App`
- 适合：比较首页新老用户的 DAU、Feeds 与模块曝光点击表现。
- 不适合：不分新老用户的首页模块明细应使用[首页模块曝光与点击明细](../首页模块曝光与点击明细/fact.md)。
- 需要澄清：需要确认新老用户组、首页区域、日期和 UV 或 PV。

## 报表

### ads_daily_user_home_module_performance

- 表：`my-project-8584-jetonai.decom.ads_daily_user_home_module_performance`
- 状态：`active`
- 粒度：`dt + user_group`
- 时区：`America/Toronto`
- 数据范围：首页新老用户的 DAU、Feeds 和模块曝光点击日汇总。
- 必要过滤：按目标 `user_group` 过滤。
- 不包含：不区分新老用户的模块明细。
- Dashboard：[【AI-Fashion】首页新老用户曝光点击](https://bi.alvinclub.ca/superset/dashboard/113/)（id=113）

## 字段

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 用户分群 | `user_group` | 已登录用户 | 固定取值 `新用户` / `老用户`；新用户 = `first_active_date = dt`，老用户 = `first_active_date < dt`，复用 `decom.dim_user_first_active` | 按目标分群过滤 | 两个分群互斥，但同一用户跨天会重复出现，UV 不可跨天求和 |
| 活跃用户数 | `dau_uv` | 已登录用户 | 基于 `decom.dws_user_activity_daily` 按 `dt × user_id` 去重后 `COUNT(DISTINCT user_id)` | 无 | 不可跨天或跨分群求和 |
| Feeds 模块曝光 UV | `feeds_exposure_uv` | 已登录用户 | `dws_user_activity_daily.exposed_home_feeds = 1` 的去重用户数；事件 `v_home_feeds` | `prop_user_id IS NOT NULL AND prop_user_id != ''` | 不可跨天、跨模块或跨分群求和 |
| Feeds 模块点击 UV | `feeds_click_uv` | 已登录用户 | `dws_user_activity_daily.clicked_home_feeds = 1` 的去重用户数；事件 `c_home_feeds` | `prop_user_id IS NOT NULL AND prop_user_id != ''` | 不可跨天、跨模块或跨分群求和 |
| Star 模块曝光 UV | `star_exposure_uv` | 已登录用户 | `dws_user_activity_daily.exposed_home_star = 1` 的去重用户数；事件 `v_home_star` | `prop_user_id IS NOT NULL AND prop_user_id != ''` | 不可跨天、跨模块或跨分群求和 |
| Star 模块点击 UV | `star_click_uv` | 已登录用户 | `dws_user_activity_daily.clicked_home_star = 1` 的去重用户数；事件 `c_home_star` | `prop_user_id IS NOT NULL AND prop_user_id != ''` | 不可跨天、跨模块或跨分群求和 |
| Magazine 模块曝光 UV | `magazine_exposure_uv` | 已登录用户 | `dws_user_activity_daily.exposed_home_magazine = 1` 的去重用户数；事件 `v_home_magazine` | `prop_user_id IS NOT NULL AND prop_user_id != ''` | 不可跨天、跨模块或跨分群求和 |
| Magazine 模块点击 UV | `magazine_click_uv` | 已登录用户 | `dws_user_activity_daily.clicked_home_magazine = 1` 的去重用户数；事件 `c_home_magazine` | `prop_user_id IS NOT NULL AND prop_user_id != ''` | 不可跨天、跨模块或跨分群求和 |
| Brand 模块曝光 UV | `brand_exposure_uv` | 已登录用户 | `dws_user_activity_daily.exposed_home_brand = 1` 的去重用户数；事件 `v_home_brand` | `prop_user_id IS NOT NULL AND prop_user_id != ''` | 不可跨天、跨模块或跨分群求和 |
| Brand 模块点击 UV | `brand_click_uv` | 已登录用户 | `dws_user_activity_daily.clicked_home_brand = 1` 的去重用户数；事件 `c_home_brand` | `prop_user_id IS NOT NULL AND prop_user_id != ''` | 不可跨天、跨模块或跨分群求和 |

四个模块的点击率（`click_uv / exposure_uv`）不物化，由 Superset chart metric 现算；分母为 0 时返回 `NULL`，跨天、跨模块或跨用户组必须汇总去重分子分母后重算。

补充说明：

- 曝光 / 点击 UV 口径与 `decom.dws_home_module_daily` 完全对齐
- 历史回填范围：`2026-05-01` 至今

## 依赖

- 上游：`decom.dim_user_first_active`, `decom.dwd_event_log`
- 中层事实：`decom.dws_user_activity_daily`
- 中层脚本：`jobs/dws/dws_user_activity_daily.py`
- 中层 DDL：`ddl/dws/dws_user_activity_daily.sql`
- 终层脚本：`jobs/ads/ads_daily_user_home_module_performance.py`
- DDL：`ddl/ads/ads_daily_user_home_module_performance.sql`
- 编排：`env/etl_config.json`（`dws_user_activity_daily`、`ads_daily_user_home_module_performance`）
