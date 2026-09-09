# 首页新老用户曝光点击

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 指标 | DAU | 日活、日活用户数、活跃用户数 |
| 指标 | feeds_exposure_uv | 首页 Feeds 曝光 UV、Feeds 曝光人数 |
| 指标 | feeds_click_uv | 首页 Feeds 点击 UV、Feeds 点击人数 |
| 维度 | 新用户 | 新增用户、首日活跃用户 |
| 维度 | 老用户 | 存量用户、历史活跃用户 |

## 报表

### 首页新老用户曝光点击

- 表：`ads_daily_user_home_module_performance`
- 粒度：`dt + user_group`
- Dashboard：[【AI-Fashion】首页新老用户曝光点击](https://bi.alvinclub.ca/superset/dashboard/113/)（id=113）

## 字段

| 字段 | 口径 |
|------|------|
| user_group | 用户分群；固定取值为 `新用户` / `老用户`；新用户 = `first_active_date = dt`，老用户 = `first_active_date < dt` |
| dau_uv | 当天活跃已登录用户数；基于 `decom.dws_user_activity_daily` 按 `dt × user_id` 去重后的 cohort，按 `COUNT(DISTINCT user_id)` 统计 |
| feeds_exposure_uv | 当天首页 feeds 模块曝光的已登录 UV；复用 `dws_user_activity_daily.exposed_home_feeds = 1` 的用户集合 |
| feeds_click_uv | 当天首页 feeds 模块点击的已登录 UV；复用 `dws_user_activity_daily.clicked_home_feeds = 1` |
| star_exposure_uv | 当天首页 star 模块曝光的已登录 UV；复用 `dws_user_activity_daily.exposed_home_star = 1` |
| star_click_uv | 当天首页 star 模块点击的已登录 UV；复用 `dws_user_activity_daily.clicked_home_star = 1` |
| magazine_exposure_uv | 当天首页 magazine 模块曝光的已登录 UV；复用 `dws_user_activity_daily.exposed_home_magazine = 1` |
| magazine_click_uv | 当天首页 magazine 模块点击的已登录 UV；复用 `dws_user_activity_daily.clicked_home_magazine = 1` |
| brand_exposure_uv | 当天首页 brand 模块曝光的已登录 UV；复用 `dws_user_activity_daily.exposed_home_brand = 1` |
| brand_click_uv | 当天首页 brand 模块点击的已登录 UV；复用 `dws_user_activity_daily.clicked_home_brand = 1` |

补充说明：

- 新老用户分群复用 `decom.dim_user_first_active` 下沉后的 `first_active_date`，不在 ADS 直接现算"首日访问"
- 曝光/点击 UV 口径与 `decom.dws_home_module_daily` 完全对齐：`prop_user_id IS NOT NULL AND prop_user_id != ''`，事件名 `v_home_* / c_home_*`
- 4 个模块的点击率（`click_uv / exposure_uv`）不物化，统一由 Superset chart metric 现算
- 历史回填范围：`2026-05-01` 至今

## 依赖

- 上游：`decom.dim_user_first_active`, `decom.dwd_event_log`
- 中层事实：`decom.dws_user_activity_daily`
- 中层脚本：`jobs/dws/dws_user_activity_daily.py`
- 中层 DDL：`ddl/dws/dws_user_activity_daily.sql`
- 终层脚本：`jobs/ads/ads_daily_user_home_module_performance.py`
- DDL：`ddl/ads/ads_daily_user_home_module_performance.sql`
- 编排：`env/etl_config.json`（`dws_user_activity_daily`、`ads_daily_user_home_module_performance`）
