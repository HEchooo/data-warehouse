# 全量报表

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 指标 | 设备数 | DAU、活跃设备数、日活设备数 |
| 指标 | 用户数 | 注册用户 DAU、活跃注册用户数 |
| 指标 | next_day_retention_rate | 次日留存率、D1 留存率 |
| 指标 | avg_duration_sec | 人均停留时长、平均使用时长 |
| 指标 | avg_content_consume_count | 人均内容消费、人均浏览内容数 |

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

补充说明：
- `dws_device_daily` / `dws_user_daily` 的活跃主体会排除 `send_push`、`send_email` 这类被动触达事件；它们不能单独把设备或用户算成活跃或留存

## 依赖

- 上游：`dws_device_daily`, `dws_user_daily`
- 脚本：`jobs/ads/ads_daily_total.py`
- DDL：`ddl/ads/ads_daily_total.sql`
