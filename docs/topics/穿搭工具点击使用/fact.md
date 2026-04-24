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
| click_use_pv | 点击使用次数；仅统计 `prop_user_id IS NOT NULL AND prop_user_id != ''` 的事件，并按 `dt × post_code × raw_event_id` 去重 |
| click_use_uv | 点击使用用户数；仅统计 `prop_user_id IS NOT NULL AND prop_user_id != ''` 的事件，并按 `COUNT(DISTINCT prop_user_id)` |
| avg_click_use_count_per_user | `click_use_pv / click_use_uv` |

### 依赖

- 上游：`dwd_event_log`, `v3_decom.community_post`
- 已落地实体层：`dim_post`
- 实体脚本：`jobs/dim/dim_post.py`
- 实体 DDL：`ddl/dim/dim_post.sql`
- 脚本：`jobs/ads/ads_daily_post_tryon_confirm.py`
- DDL：`ddl/ads/ads_daily_post_tryon_confirm.sql`
- 当前状态：Batch 1 已落地统一帖子实体层；本表现阶段仍直连 `v3_decom.community_post`，后续切换时默认通过 `dim_post` 补齐帖子维度

## 补充说明

- 访客去重：`visitor_id = COALESCE(NULLIF(prop_user_id,''), NULLIF(prop_device_id,''))`
- `ads_daily_post_tryon_confirm` 和 dashboard 96 当前改为已登录用户口径：事件侧仅保留 `prop_user_id IS NOT NULL AND prop_user_id != ''`
- 维度映射：`post_code → post_id/post_name/creator` 用 `v3_decom.community_post`
- Batch 1 已落地 `dim_post`，但当前脚本尚未切换消费
- 按帖子分析用帖子明细表；全量日趋势用汇总表
- 编排：`jobs/ads/ads_daily.py`
