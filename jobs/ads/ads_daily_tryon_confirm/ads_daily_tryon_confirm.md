# 穿搭工具“点击使用”日汇总 (ads_daily_tryon_confirm)

## 报表定义

统计穿搭工具“点击使用”行为的每日汇总。

**事件**：`c_tryon_confirm`

**粒度**：`dt`

## Filters（筛选维度）

- 日期：`dt`

> 说明：若需要按来源帖子筛选（post_id），请使用 `ads_daily_post_tryon_confirm`（粒度 `dt × post_id`）。

## 表结构

| 字段 | 类型 | 说明 |
|------|------|------|
| dt | DATE | 日期（多伦多时间） |
| click_use_pv | INT64 | 点击使用次数（PV，按 raw_event_id 去重） |
| click_use_uv | INT64 | 点击使用用户数（UV，visitor_id 去重） |
| avg_click_use_count_per_user | NUMERIC | 人均点击使用次数（PV / UV） |
| update_time | TIMESTAMP | 更新时间（UTC） |

**分区**：`dt`

## 指标口径

### 访客去重口径（UV）

`visitor_id = COALESCE(NULLIF(prop_user_id,''), NULLIF(prop_device_id,''))`

### 点击使用次数（click_use_pv）

- **事件范围**：`c_tryon_confirm`
- **去重**：`COUNT(DISTINCT raw_event_id)`
- **注意**：`dwd_event_log` 会展开 `args_post` / `args_spu`，若不按 `raw_event_id` 去重会导致 PV 被放大

### 点击使用用户数（click_use_uv）

- **事件范围**：`c_tryon_confirm`
- **去重**：`COUNT(DISTINCT visitor_id)`

### 人均点击使用次数（avg_click_use_count_per_user）

`click_use_pv / click_use_uv`

## Filters 说明（来源帖子）

本表为全量日汇总，不包含 `post_id` 维度，避免在“不筛 post_id”的场景下 UV 被多帖子重复计数。

若要按来源帖子筛选，请使用 `ads_daily_post_tryon_confirm`。

## 数据范围与丢弃规则

- 仅统计包含来源帖子（`post_code`）的事件
- `post_code` 为空的事件舍弃

## 数据来源

```
ads_daily_tryon_confirm
    └── dwd_event_log (event_name='c_tryon_confirm')
```

## 时区说明

| 字段 | 时区 |
|------|------|
| dt | America/Toronto（多伦多时间） |
| update_time | UTC |

## 更新机制

- **调度周期**：T+1（每日更新）
- **更新方式**：DELETE + INSERT（幂等）
- **增量判断**：由 `jobs/ads/ads_daily/ads_daily.py` 统一识别日期

## 相关文件

- **DDL**：`ddl/ads/ads_daily_tryon_confirm.sql`
- **任务脚本**：`jobs/ads/ads_daily_tryon_confirm/ads_daily_tryon_confirm.py`（函数 `run_ads_daily_tryon_confirm`）
- **入口脚本**：`jobs/ads/ads_daily/ads_daily.py`

