# 用户时长与访问频次

## 报表

- 表：`ads_daily_user_duration_frequency`
- 粒度：需求粒度为 `dt × user_segment`
- Dashboard：[【AI-Fashion】用户时长与访问频次](https://bi.alvinclub.ca/superset/dashboard/95/)（id=95）

## 字段

| 字段 | 口径 |
|------|------|
| user_segment | 用户分群；固定取值为 `全量设备` / `游客` / `已登录用户` |
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
- `全量设备` 口径：不感知 `prop_user_id`，按 `NULLIF(prop_device_id,'')` 去重设备
- `游客` 口径：仅保留 `prop_user_id IS NULL OR prop_user_id = ''` 的事件，按 `NULLIF(prop_device_id,'')` 去重设备
- `已登录用户` 口径：仅保留 `prop_user_id IS NOT NULL AND prop_user_id != ''` 的事件，按 `NULLIF(prop_user_id,'')` 去重用户
- 留存 cohort 和 MAU 都按各自分群下的同一统计主体理解
- `全量设备`、`游客`、`已登录用户` 的统计主体不完全一致，三档结果不能直接相加校验

## 已落地字段

| 需求字段 | 当前已落地 | 说明 |
|------|------|------|
| `dt` | `dt` | 已在线上表落地 |
| `user_segment` | 未落地 | 代码与 DDL 已补齐，待执行表结构变更和历史分区回填 |
| 其余指标字段 | 已落地 | 当前线上表已存在 `dau_uv`、`total_duration_min`、`avg_duration_min`、`app_launch_count`、`avg_visit_freq`、`next_day_retention_rate`、`day_7_retention_rate`、`day_30_retention_rate`、`mau_uv`、`dau_mau` |

## 依赖

- 上游：`dwd_event_log`
- 脚本：`jobs/ads/ads_daily_user_duration_frequency.py`
- DDL：`ddl/ads/ads_daily_user_duration_frequency.sql`
- 编排：`jobs/ads/ads_daily.py`
