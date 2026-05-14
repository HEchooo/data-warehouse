# 新老用户试穿渗透率

## 报表

- 表：`ads_daily_user_tryon_penetration`
- 粒度：`dt + user_group`
- Dashboard：[【AI-Fashion】新老用户试穿渗透率](https://bi.alvinclub.ca/superset/dashboard/110/)（id=110）

## 字段

| 字段 | 口径 |
|------|------|
| user_group | 用户分群；固定取值为 `新用户` / `老用户`；新用户 = `first_active_date = dt`，老用户 = `first_active_date < dt` |
| dau_uv | 当天活跃已登录用户数；基于 `decom.dws_user_activity_daily` 按 `dt × user_id` 去重后的 cohort，按 `COUNT(DISTINCT user_id)` 统计 |
| tryon_uv | 当天发生 AI 试穿的已登录用户数；复用 `decom.dws_user_activity_daily.used_ai_tryon = 1` 的用户集合，按 `COUNT(DISTINCT user_id)` 统计 |

补充说明：

- 新老用户分群复用 `decom.dim_user_first_active` 下沉后的 `first_active_date`，不在 ADS 直接现算“首日访问”
- `tryon_uv` 与平台整体内容表现看板一致，统一基于服务端 `v3_decom.common_ext_ai_task` 的 `task_type IN ('TRY_ON', 'OUTFIT')`
- 该报表当前只落地基础事实 `DAU / 试穿 UV`，不物化 `试穿渗透率`
- 该看板是独立 dashboard，不与 `【AI-Fashion】平台整体内容表现` 共用主题文档

## 依赖

- 上游：`decom.dim_user_first_active`, `dwd_event_log`, `v3_decom.common_ext_ai_task`
- 中层事实：`decom.dws_user_activity_daily`
- 中层脚本：`jobs/dws/dws_user_activity_daily.py`
- 中层 DDL：`ddl/dws/dws_user_activity_daily.sql`
- 终层脚本：`jobs/ads/ads_daily_user_tryon_penetration.py`
- DDL：`ddl/ads/ads_daily_user_tryon_penetration.sql`
- 编排：`env/etl_config.json`（`dws_user_activity_daily`、`ads_daily_user_tryon_penetration`）
