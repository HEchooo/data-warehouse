# 首页模块曝光与点击明细

## 报表

- 表：`ads_daily_home_module_performance`
- 粒度：`dt × module`
- Dashboard：[【AI-Fashion】首页模块曝光/点击明细（日×模块）](https://bi.alvinclub.ca/superset/dashboard/92/)（id=92）

## 字段

| 字段 | 口径 |
|------|------|
| module_exposure_uv | 已登录用户模块曝光 UV；`COUNT(DISTINCT prop_user_id)`，且仅统计 `prop_user_id IS NOT NULL AND prop_user_id != ''` 的曝光事件 |
| module_click_uv | 已登录用户模块点击 UV；`COUNT(DISTINCT prop_user_id)`，且仅统计 `prop_user_id IS NOT NULL AND prop_user_id != ''` 的点击事件 |
| click_rate | 点击 UV / 曝光 UV |
| click_pv | 已登录用户模块点击 PV；仅统计 `prop_user_id IS NOT NULL AND prop_user_id != ''` 的点击事件，并按 `raw_event_id` 去重 |
| avg_click_count_per_user | 点击 PV / 点击 UV |

补充说明：
- 固定模块：`star`, `magazine`, `brand`, `feeds`
- 登录用户口径：所有指标仅纳入 `prop_user_id` 非空的事件
- 事件映射：`v_home_* / c_home_*`

## 依赖

- 上游：`dwd_event_log`
- 脚本：`jobs/ads/ads_daily_home_module_performance.py`
- DDL：`ddl/ads/ads_daily_home_module_performance.sql`
- 编排：`jobs/ads/ads_daily.py`
