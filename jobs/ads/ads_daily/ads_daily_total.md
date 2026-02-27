# 总量日报 (ads_daily_total)

## 报表定义

统计每日**全部活跃设备**和**全部活跃用户**的核心指标，用于监测业务活跃情况。

## 表结构

| 字段 | 类型 | 说明 |
|------|------|------|
| dt | DATE | 日期（多伦多时间） |
| platform | STRING | 端标识（iOS / Android / h5） |
| device_count | INT64 | 活跃设备数 |
| user_count | INT64 | 活跃注册用户数 |
| avg_duration_sec | NUMERIC | 活跃设备平均停留时长（秒） |
| avg_content_consume_count | NUMERIC | 活跃设备人均内容消费数 |
| next_day_retention_rate | NUMERIC | 活跃设备次日留存率 |
| update_time | TIMESTAMP | 更新时间（UTC） |

**分区**: `dt`（按日期分区）

## 指标口径

### 活跃设备数 (device_count)
- **定义**: 当日有埋点事件的去重设备数
- **判断逻辑**: 只要当日在 `dwd_event_log` 中有记录即算活跃
- **去重维度**: `prop_device_id`

### 活跃注册用户数 (user_count)
- **定义**: 当日有埋点事件的去重注册用户数
- **判断逻辑**: `prop_user_id IS NOT NULL` 且当日有事件
- **去重维度**: `prop_user_id`

### 平均停留时长 (avg_duration_sec)
- **定义**: 活跃设备在当日的平均停留时长
- **计算方式**: `SUM(session_duration_sec) / COUNT(DISTINCT prop_device_id)`
- **session_duration_sec 计算**: 每个 session 的 `MAX(logAt_timestamp) - MIN(logAt_timestamp)`

### 人均内容消费数 (avg_content_consume_count)
- **定义**: 活跃设备在当日的人均内容消费次数
- **计算方式**: `SUM(content_consume_count) / COUNT(DISTINCT prop_device_id)`
- **内容消费事件**: `v_product_detail`, `v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail`, `v_kol_post_detail`

### 次日留存率 (next_day_retention_rate)
- **定义**: 当日活跃设备中，次日仍然活跃的比例
- **计算方式**: `次日仍活跃的活跃设备数 / 当日活跃设备总数`
- **精度**: 保留 4 位小数（如 0.4521 表示 45.21%）

## 数据来源

```
ads_daily_total
    └── dws_device_daily (全量设备, 按 platform 分组)
    └── dws_user_daily (全量用户, 按 platform 分组)
            └── dwd_event_log (prop_url 判断 platform)
```

## 时区说明

| 字段 | 时区 |
|------|------|
| dt | America/Toronto（多伦多时间） |
| update_time | UTC |

**注意**: UTC 凌晨 00:00-04:59（冬令时）或 00:00-03:59（夏令时）的数据会归属到多伦多前一天，这是预期行为。

## 更新机制

- **调度周期**: T+1（每日更新）
- **更新方式**: DELETE + INSERT（幂等）
- **T+1 回刷**: 每次刷新包含「更新日期 - 1 天」的数据，用于回刷次日留存率
- **增量判断**: 基于 `dws_device_daily.update_time > ads_daily_total.update_time`

## 使用示例

```sql
-- 查询最近 7 天的活跃趋势
SELECT
    dt,
    device_count,
    user_count,
    ROUND(avg_duration_sec, 2) AS avg_duration_sec,
    ROUND(avg_content_consume_count, 2) AS avg_content_consume_count,
    ROUND(next_day_retention_rate * 100, 2) AS retention_rate_pct
FROM `my-project-8584-jetonai.decom.ads_daily_total`
WHERE dt >= DATE_SUB(CURRENT_DATE('America/Toronto'), INTERVAL 7 DAY)
ORDER BY dt DESC;
```

## 与 ads_daily_new 的区别

| 对比项 | ads_daily_new | ads_daily_total |
|--------|---------------|-----------------|
| 统计范围 | 当日**首次**活跃的设备/用户 | 当日**所有**活跃的设备/用户 |
| 判断条件 | `is_new_device = TRUE` | 无限制（全量） |
| 适用场景 | 新用户增长分析 | 整体活跃度分析 |

## 相关文件

- **DDL**: `ddl/ads/ads_daily_total.sql`
- **ETL 脚本**: `jobs/ads/ads_daily.py` (函数 `run_ads_daily_total`)
- **依赖表**:
  - `dws_device_daily`
  - `dws_user_daily`
