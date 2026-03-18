# 首页模块表现日报 (ads_daily_home_module_performance)

## 报表定义

统计首页四个模块（star / magazine / brand / feeds）的每日曝光与点击表现（每日每模块一行），用于监控模块点击率与点击深度。

## 表结构

| 字段 | 类型 | 说明 |
|------|------|------|
| dt | DATE | 日期（多伦多时间） |
| module | STRING | 首页模块标识（star / magazine / brand / feeds） |
| module_exposure_uv | INT64 | 模块曝光 UV |
| module_click_uv | INT64 | 模块点击 UV |
| click_rate | NUMERIC | 点击率（点击 UV / 曝光 UV） |
| click_pv | INT64 | 模块点击 PV（按 raw_event_id 去重） |
| avg_click_count_per_user | NUMERIC | 点击人均次数（点击 PV / 点击 UV） |
| update_time | TIMESTAMP | 更新时间（UTC） |

**分区**: `dt`（按日期分区）

## 指标口径

### 访客去重口径
- **visitor_id**: `COALESCE(NULLIF(prop_user_id, ''), NULLIF(prop_device_id, ''))`
- **说明**: 兼容登录和未登录用户，优先使用用户 ID。

### 模块映射与事件范围

| module | 曝光事件 | 点击事件 |
|--------|----------|----------|
| star | v_home_star | c_home_star |
| magazine | v_home_magazine | c_home_magazine |
| brand | v_home_brand | c_home_brand |
| feeds | v_home_feeds | c_home_feeds |

### 模块曝光 UV (module_exposure_uv)
- **定义**: 当天触发对应模块曝光事件的去重访客数。
- **计算方式**: `COUNT(DISTINCT visitor_id)`（限定为曝光事件）

### 模块点击 UV (module_click_uv)
- **定义**: 当天触发对应模块点击事件的去重访客数。
- **计算方式**: `COUNT(DISTINCT visitor_id)`（限定为点击事件）

### 模块点击 PV (click_pv)
- **定义**: 当天触发模块点击事件的去重点击次数。
- **去重键**: `raw_event_id`
- **计算方式**: `COUNT(DISTINCT raw_event_id)`（限定为点击事件）

### 点击率 (click_rate)
- **定义**: 模块点击 UV / 模块曝光 UV。
- **计算方式**: `module_click_uv / module_exposure_uv`（曝光 UV 为 0 时返回 0）

### 点击人均次数 (avg_click_count_per_user)
- **定义**: 点击用户的人均点击次数。
- **计算方式**: `click_pv / module_click_uv`（点击 UV 为 0 时返回 0）

## 数据来源

```
ads_daily_home_module_performance
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
- **增量判断**: `dws_device_daily.update_time > ads_daily_home_module_performance.update_time`
- **补 0 行机制**: 通过 `date_list × module_list` 生成键，保证每天四个模块都产出行（无数据时指标为 0）

## 相关文件

- **DDL**: `ddl/ads/ads_daily_home_module_performance.sql`
- **任务脚本**: `jobs/ads/ads_daily_home_module_performance/ads_daily_home_module_performance.py`（函数 `run_ads_daily_home_module_performance`）
- **入口脚本**: `jobs/ads/ads_daily/ads_daily.py`

## Superset 看板

### 看板名称

- `【AI-Fashion】首页模块曝光/点击明细（日×模块）`

### 看板内容

- 图表
  - `首页模块曝光/点击明细（日×模块）-规模趋势`
  - `首页模块曝光/点击明细（日×模块）-转化趋势`
  - `首页模块曝光/点击明细（日×模块）-明细表`
- 筛选器（Native Filters）
  - `日期范围`：默认 `Last week`
  - `模块`：多选，字段 `module`（值：`star` / `magazine` / `brand` / `feeds`）

### 生成脚本

- `.agents/skills/superset/scripts/create_ads_daily_home_module_exposure_click_dashboard.py`

### 使用方式

默认先 dry-run（只输出计划，不写入 Superset）：

```bash
python3 .agents/skills/superset/scripts/create_ads_daily_home_module_exposure_click_dashboard.py
```

真正创建/更新（写入 Superset）：

```bash
python3 .agents/skills/superset/scripts/create_ads_daily_home_module_exposure_click_dashboard.py --apply
```
