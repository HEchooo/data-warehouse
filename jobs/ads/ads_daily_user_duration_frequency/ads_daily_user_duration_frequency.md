# 用户时长与访问频次日报 (ads_daily_user_duration_frequency)

## 报表定义

统计每日平台整体「用户时长与访问频次」核心指标（每日一行，仅“全部”维度），用于监控活跃、时长、访问频次与留存表现。

## 表结构

| 字段 | 类型 | 说明 |
|------|------|------|
| dt | DATE | 日期（多伦多时间） |
| dau_uv | INT64 | 日活 UV（visitor_id 去重） |
| total_duration_min | NUMERIC | 总使用时长（分钟） |
| avg_duration_min | NUMERIC | 人均使用时长（分钟） |
| app_launch_count | INT64 | 启动次数（总） |
| avg_visit_freq | NUMERIC | 人均访问频次 |
| next_day_retention_rate | NUMERIC | 次日留存率 |
| day_7_retention_rate | NUMERIC | 7 日留存率 |
| day_30_retention_rate | NUMERIC | 30 日留存率 |
| mau_uv | INT64 | 当月月活 UV（自然月去重） |
| dau_mau | NUMERIC | DAU/MAU |
| update_time | TIMESTAMP | 更新时间（UTC） |

**分区**: `dt`（按日期分区）

## 指标口径

### 访客去重口径
- **visitor_id**: `COALESCE(NULLIF(prop_user_id, ''), NULLIF(prop_device_id, ''))`
- **说明**: 兼容登录与未登录用户，优先使用用户 ID；若用户登录状态在天与天之间发生变化，留存会按上述 visitor_id 口径跟随。

### 日活 UV (dau_uv)
- **定义**: 当日有任意埋点事件的去重访客数。
- **计算方式**: `COUNT(DISTINCT visitor_id)`（visitor_id 非空）

### 启动次数（总）(app_launch_count)
- **事件范围**: `event_name = 'app_launch'`
- **去重键**: `duration_event_id = COALESCE(raw_event_id, hash_id)`
- **计算方式**: `COUNT(DISTINCT duration_event_id)`

### 总使用时长（分钟）(total_duration_min)
- **事件范围**: `event_name = 'app_launch'`
- **来源字段**: `args_session_duration`
- **单位**: 毫秒（ms）
- **去重口径**: 按 `duration_event_id` 去重后取 `MAX(args_session_duration)`，再求和
- **换算**: `SUM(duration_ms) / 60000`

### 人均使用时长（分钟）(avg_duration_min)
- **定义**: 当天用户平均使用时长。
- **计算方式**: `total_duration_min / dau_uv`（dau_uv 为 0 时返回 0）
- **注意**: 目前分母使用 `dau_uv`（任意事件活跃），而不是仅 `app_launch` 活跃；如后续需要“仅启动用户的人均时长”，需要调整分母口径。

### 人均访问频次 (avg_visit_freq)
- **定义**: 当天人均启动次数。
- **分子**: `app_launch_count`（仅 `app_launch`，按 `duration_event_id` 去重后的启动次数）
- **计算方式**: `app_launch_count / dau_uv`（dau_uv 为 0 时返回 0）

### 留存率（次日 / 7 日 / 30 日）
- **cohort 定义**: 当日活跃的 visitor_id 集合（任意事件活跃）
- **留存定义**: 该 cohort 在 `dt + N` 仍然活跃（任意事件活跃）
- **计算方式**: `retained_uv / active_uv`（active_uv 为 0 时返回 0）
- **到期控制**: 当 `dt + N` 晚于“昨天”（多伦多时间）时，留存字段返回 `NULL`，避免未到期的留存被误解为 0。

### 当月月活（自然月）(mau_uv)
- **定义**: `dt` 所属自然月内有任意事件活跃的去重 visitor_id 数。
- **计算方式**: `COUNT(DISTINCT visitor_id)` on `active_visitors.dt BETWEEN 当月1号 AND MIN(当月最后一天, 多伦多昨天)`
- **说明**: 若 `dt` 属于当前月，则因未来日期尚无数据，统计范围会自然截止到“多伦多昨天”（效果等价于当月累计 MTD）。

### DAU/MAU (dau_mau)
- **定义**: 当天活跃占当月月活（自然月）的比例。
- **计算方式**: `dau_uv / mau_uv`（mau_uv 为 0 时返回 0）

## 数据来源

```
ads_daily_user_duration_frequency
    └── dwd_event_log
```

## 时区说明

| 字段 | 时区 |
|------|------|
| dt | America/Toronto（多伦多时间） |
| update_time | UTC |

## 更新机制

- **调度周期**: T+1（每日更新）
- **更新方式**: DELETE + INSERT（幂等）
- **增量判断**: `dws_device_daily.update_time > ads_daily_user_duration_frequency.update_time`
- **到期回刷**: 入口编排会额外纳入 `dt-1`、`dt-7`、`dt-30`，用于留存到期后自动回刷对应日期的数据

## 相关文件

- **DDL**: `ddl/ads/ads_daily_user_duration_frequency.sql`
- **任务脚本**: `jobs/ads/ads_daily_user_duration_frequency/ads_daily_user_duration_frequency.py`（函数 `run_ads_daily_user_duration_frequency`）
- **入口脚本**: `jobs/ads/ads_daily/ads_daily.py`
