# 穿搭工具“点击使用”日汇总（日×帖子）(ads_daily_post_tryon_confirm)

## 报表定义

统计穿搭工具“点击使用”行为在来源帖子维度的每日汇总，支持按来源帖子筛选。

**事件**：`c_tryon_confirm`

**粒度**：`dt × post_id`

## Filters（筛选维度）

- 日期：`dt`
- 使用来源帖子：`post_id`（可选）

## 表结构

| 字段 | 类型 | 说明 |
|------|------|------|
| dt | DATE | 日期（多伦多时间） |
| post_id | INT64 | 来源帖子 ID（由 post_code 映射） |
| post_code | STRING | 来源帖子 code（埋点字段 args.post） |
| post_name | STRING | 帖子名称（由 post_code 映射） |
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
- **去重**：按 `dt × post_code × raw_event_id` 去重后计数
- **注意**：`dwd_event_log` 会展开 `args_post` / `args_spu`，PV 必须按 `raw_event_id` 去重避免放大

### 点击使用用户数（click_use_uv）

- **事件范围**：`c_tryon_confirm`
- **去重**：`COUNT(DISTINCT visitor_id)`（按 `dt × post_id` 粒度统计）

### 人均点击使用次数（avg_click_use_count_per_user）

`click_use_pv / click_use_uv`

## 维度映射

### post_code → post_id/post_name

使用表：`v3_decom.community_post`（`id`, `post_code`, `title`）

## 数据范围与丢弃规则

- `post_code` 为空的事件舍弃
- `post_code` 无法映射到 `post_id` 的事件舍弃（确保粒度为 `dt × post_id`）

## 数据来源

```
ads_daily_post_tryon_confirm
    ├── dwd_event_log (event_name='c_tryon_confirm')
    └── v3_decom.community_post
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

## 使用建议（避免 UV 重复计数）

- 需要按帖子分析时：使用本表（`dt × post_id`）
- 需要“不筛 post_id”的全量日趋势时：使用 `ads_daily_tryon_confirm`（`dt` 粒度），避免 UV 被多帖子重复计数

## 相关文件

- **DDL**：`ddl/ads/ads_daily_post_tryon_confirm.sql`
- **任务脚本**：`jobs/ads/ads_daily_post_tryon_confirm/ads_daily_post_tryon_confirm.py`（函数 `run_ads_daily_post_tryon_confirm`）
- **入口脚本**：`jobs/ads/ads_daily/ads_daily.py`

