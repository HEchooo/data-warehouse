# 用户时长与访问频次

## 报表

- 表：`ads_daily_user_duration_frequency`
- 粒度：`dt`
- Dashboard：[【AI-Fashion】用户时长与访问频次](https://bi.alvinclub.ca/superset/dashboard/95/)（id=95）

## 字段

| 字段 | 口径 |
|------|------|
| dau_uv | 日活 UV；任意事件活跃的 `COUNT(DISTINCT visitor_id)` |
| total_duration_min | 总使用时长（分钟）；`app_launch` 的 `args_session_duration` 按 `COALESCE(raw_event_id, hash_id)` 去重后求和 |
| avg_duration_min | `total_duration_min / dau_uv` |
| app_launch_count | 启动次数；事件：`app_launch`；按 `COALESCE(raw_event_id, hash_id)` 去重 |
| avg_visit_freq | `app_launch_count / dau_uv` |
| next_day_retention_rate | 当日活跃 cohort 在 dt+1 仍活跃的比例；未到期返回 NULL |
| day_7_retention_rate | 当日活跃 cohort 在 dt+7 仍活跃的比例；未到期返回 NULL |
| day_30_retention_rate | 当日活跃 cohort 在 dt+30 仍活跃的比例；未到期返回 NULL |
| mau_uv | 当月月活（自然月） |
| dau_mau | `dau_uv / mau_uv` |

补充说明：
- 访客去重：`visitor_id = COALESCE(NULLIF(prop_user_id,''), NULLIF(prop_device_id,''))`
- 留存 cohort 和 MAU 都按同一 visitor_id 口径理解

## 依赖

- 上游：`dwd_event_log`
- 脚本：`jobs/ads/ads_daily_user_duration_frequency.py`
- DDL：`ddl/ads/ads_daily_user_duration_frequency.sql`
- 编排：`jobs/ads/ads_daily.py`
