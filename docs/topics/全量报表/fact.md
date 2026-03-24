# 全量报表

## 报表

- 表：`ads_daily_total`
- 粒度：`dt × platform`
- Dashboard：[【AI-Fashion】全量报表](https://bi.alvinclub.ca/superset/dashboard/89/)（id=89）

## 字段

| 字段 | 口径 |
|------|------|
| device_count | 当日有埋点事件的去重设备数（`prop_device_id`） |
| user_count | 当日有埋点事件且 `prop_user_id IS NOT NULL` 的去重用户数 |
| avg_duration_sec | `SUM(session_duration_sec) / COUNT(DISTINCT prop_device_id)`；`app_launch` 的 `args_session_duration` 按 `COALESCE(raw_event_id, hash_id)` 去重后求和，单位秒 |
| avg_content_consume_count | `SUM(content_consume_count) / COUNT(DISTINCT prop_device_id)`；内容消费事件：`v_product_detail`, `v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail`, `v_kol_post_detail` |
| next_day_retention_rate | 次日仍活跃设备数 / 当日活跃设备总数 |

## 依赖

- 上游：`dws_device_daily`, `dws_user_daily`
- 脚本：`jobs/ads/ads_daily_total.py`
- DDL：`ddl/ads/ads_daily_total.sql`
- 编排：`jobs/ads/ads_daily.py`
