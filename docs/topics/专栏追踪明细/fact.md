# 专栏追踪明细

## 报表

- 表：`ads_daily_column_performance`
- 粒度：`dt × creator × module × column_id`
- Dashboard：[【AI-Fashion】专栏追踪明细（日×专栏）](https://bi.alvinclub.ca/superset/dashboard/93/)（id=93）

## 字段

| 字段 | 口径 |
|------|------|
| column_exposure_uv | 专栏曝光 UV；事件：`v_star_post_feeds`, `v_brand_post_feeds`, `v_star_post_detail`, `v_brand_post_detail`, `v_magazine_post_detail` |
| follow_total_count | 关注点击次数；事件：`c_follow`；按 `raw_event_id` 去重；`creator` 在 `module + column_id` 范围内按最近一次专栏曝光归因 |
| follow_rate | `follow_uv / column_exposure_uv` |
| read_post_count | 去重帖子数；`COUNT(DISTINCT CONCAT(visitor_id,'-',post_code))` |
| avg_read_post_count_per_user | `read_post_count / column_exposure_uv` |

补充说明：
- 访客去重：`visitor_id = COALESCE(NULLIF(prop_user_id,''), NULLIF(prop_device_id,''))`
- 维度映射：`creator` 用 `v3_decom.community_post.creator`；`column_id → column_name` 用 `v3_decom.kol_rel`

## 依赖

- 上游：`dwd_event_log`, `v3_decom.community_post`, `v3_decom.kol_rel`
- 脚本：`jobs/ads/ads_daily_column_performance.py`
- DDL：`ddl/ads/ads_daily_column_performance.sql`
- 编排：`jobs/ads/ads_daily.py`
