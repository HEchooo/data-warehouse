# 穿搭工具点击使用

## ads_daily_tryon_confirm

### 报表

- 表：`ads_daily_tryon_confirm`
- 粒度：`dt`
- 事件：`c_tryon_confirm`

### 字段

| 字段 | 口径 |
|------|------|
| click_use_pv | 点击使用次数；按 `raw_event_id` 去重；仅统计 `post_code` 非空的事件 |
| click_use_uv | 点击使用用户数；`COUNT(DISTINCT visitor_id)` |
| avg_click_use_count_per_user | `click_use_pv / click_use_uv` |

### 依赖

- 上游：`dwd_event_log`
- 脚本：`jobs/ads/ads_daily_tryon_confirm.py`
- DDL：`ddl/ads/ads_daily_tryon_confirm.sql`

## ads_daily_post_tryon_confirm

### 报表

- 表：`ads_daily_post_tryon_confirm`
- 粒度：`dt × post_id`
- 事件：`c_tryon_confirm`
- Dashboard：[【AI-Fashion】穿搭工具点击使用日汇总（日×帖子）](https://bi.alvinclub.ca/superset/dashboard/96/)（id=96）

### 字段

| 字段 | 口径 |
|------|------|
| click_use_pv | 点击使用次数；按 `dt × post_code × raw_event_id` 去重 |
| click_use_uv | 点击使用用户数；`COUNT(DISTINCT visitor_id)` |
| avg_click_use_count_per_user | `click_use_pv / click_use_uv` |

### 依赖

- 上游：`dwd_event_log`, `v3_decom.community_post`
- 脚本：`jobs/ads/ads_daily_post_tryon_confirm.py`
- DDL：`ddl/ads/ads_daily_post_tryon_confirm.sql`

## 补充说明

- 访客去重：`visitor_id = COALESCE(NULLIF(prop_user_id,''), NULLIF(prop_device_id,''))`
- 维度映射：`post_code → post_id/post_name/creator` 用 `v3_decom.community_post`
- 按帖子分析用帖子明细表；全量日趋势用汇总表
- 编排：`jobs/ads/ads_daily.py`
