# 用增日报 (ads_daily_new)

## 报表定义

统计每日**新增设备**、**新增注册用户**和**新增下载量**的核心指标，用于监测业务增长情况。

## 表结构

| 字段 | 类型 | 说明 |
|------|------|------|
| dt | DATE | 日期（多伦多时间） |
| platform | STRING | 端标识（iOS / Android / h5） |
| device_count | INT64 | 新增设备数 |
| user_count | INT64 | 新增注册用户数 |
| avg_duration_sec | NUMERIC | 新增设备平均停留时长（秒） |
| avg_content_consume_count | NUMERIC | 新增设备人均内容消费数 |
| next_day_retention_rate | NUMERIC | 新增设备次日留存率 |
| new_download_count | INT64 | 新增下载量（按端） |
| update_time | TIMESTAMP | 更新时间（UTC） |

**分区**: `dt`（按日期分区）

## 指标口径

### 新增设备数 (device_count)
- **定义**: 当日首次活跃的去重设备数
- **判断逻辑**: `dim_device_first_active.first_active_date = dt`
- **去重维度**: `prop_device_id`

### 新增注册用户数 (user_count)
- **定义**: 当日首次活跃的去重注册用户数
- **判断逻辑**: `dim_user_first_active.first_active_date = dt`
- **去重维度**: `prop_user_id`

### 平均停留时长 (avg_duration_sec)
- **定义**: 新增设备在当日的平均停留时长
- **计算方式**: `SUM(session_duration_sec) / COUNT(DISTINCT prop_device_id)`
- **session_duration_sec 计算**: `event_name = 'app_launch'` 的 `args_session_duration` 按原始事件去重后求和
- **去重键**: 优先使用 `raw_event_id`，缺失时回退到 `hash_id`
- **单位**: `args_session_duration` 原始值为毫秒，入 DWS/ADS 时统一换算为秒

### 人均内容消费数 (avg_content_consume_count)
- **定义**: 新增设备在当日的人均内容消费次数
- **计算方式**: `SUM(content_consume_count) / COUNT(DISTINCT prop_device_id)`
- **内容消费事件**: `v_product_detail`, `v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail`, `v_kol_post_detail`

### 次日留存率 (next_day_retention_rate)
- **定义**: 当日新增设备中，次日仍然活跃的比例
- **计算方式**: `次日仍活跃的新增设备数 / 当日新增设备总数`
- **精度**: 保留 4 位小数（如 0.3523 表示 35.23%）

### 新增下载量 (new_download_count)
- **定义**: 应用商店当日新增下载量，按端统计
- **来源层**: `dws_download_daily`
- **iOS 口径**: `product_type_identifier = 1` 的下载量（已在 DWD 层统一）
- **Android 口径**: `daily_device_installs`（已在 DWD 层统一）
- **h5 口径**: 固定为 0

## 数据来源

```
ads_daily_new
    +-- dws_device_daily (dt, platform, is_new_device = TRUE)
    +-- dws_user_daily (dt, platform, is_new_user = TRUE)
    +-- dws_download_daily (dt, platform, new_download_count)
    |   \-- dwd_download (统一下载口径)
    |       +-- ods_ios_download
    |       \-- ods_android_download
    +-- dim_device_first_active
    +-- dim_user_first_active
    \-- dwd_event_log (prop_url 判断 platform)
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
- **增量判断**:
  - 事件指标: `dws_device_daily.update_time > ads_daily_total.update_time`
  - 下载指标: `dws_download_daily.update_time > ads_daily_new.update_time`

## 使用示例

```sql
-- 查询最近 7 天的新增趋势
SELECT
    dt,
    platform,
    device_count,
    user_count,
    new_download_count,
    ROUND(avg_duration_sec, 2) AS avg_duration_sec,
    ROUND(avg_content_consume_count, 2) AS avg_content_consume_count,
    ROUND(next_day_retention_rate * 100, 2) AS retention_rate_pct
FROM `my-project-8584-jetonai.decom.ads_daily_new`
WHERE dt >= DATE_SUB(CURRENT_DATE('America/Toronto'), INTERVAL 7 DAY)
ORDER BY dt DESC;
```

## 相关文件

- **DDL**: `ddl/ads/ads_daily_new.sql`
- **任务脚本**: `jobs/ads/ads_daily_new/ads_daily_new.py` (函数 `run_ads_daily_new`)
- **入口脚本**: `jobs/ads/ads_daily/ads_daily.py`
- **依赖表**:
  - `dws_device_daily`
  - `dws_user_daily`
  - `dws_download_daily`
  - `dwd_download`
  - `ods_ios_download`
  - `ods_android_download`
  - `dim_device_first_active`
  - `dim_user_first_active`
