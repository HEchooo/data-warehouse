# 用增报表

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | 用增报表 | 用增表 |
| 指标 | 设备数 | DAU、活跃设备数、日活设备数 |
| 指标 | 用户数 | 注册用户 DAU、活跃注册用户数 |
| 指标 | new_download_count | 新增下载数、新增安装数 |
| 指标 | next_day_retention_rate | 次日留存率、D1 留存率 |
| 指标 | day_7_retention_rate | 7 日留存率、D7 留存率 |

## 报表

- 表：`ads_daily_new`
- 粒度：`dt × platform`
- Dashboard：[【AI-Fashion】用增报表](https://bi.alvinclub.ca/superset/dashboard/88/)（id=88）

## 字段

| 字段 | 口径 |
|------|------|
| device_count | 当日首次活跃的去重设备数（由 `dws_device_daily.is_new_device = TRUE` 汇总） |
| user_count | 当日首次活跃的去重注册用户数（由 `dws_user_daily.is_new_user = TRUE` 汇总） |
| new_user_login_rate | 新增注册用户 / 新增设备，即 `user_count / device_count` |
| anonymous_device_rate | 当日 `dws_device_daily.has_anonymous_event = TRUE` 的去重设备数 / 当日全部去重设备数 |
| anonymous_old_device_rate | 当日 `dws_device_daily.has_anonymous_event = TRUE AND is_new_device = FALSE` 的去重设备数 / 当日全部去重设备数 |
| avg_duration_sec | `SUM(session_duration_sec) / COUNT(DISTINCT prop_device_id)`；`app_launch` 的 `args_session_duration` 按 `COALESCE(raw_event_id, hash_id)` 去重后求和，单位秒 |
| avg_content_consume_count | `SUM(content_consume_count) / COUNT(DISTINCT prop_device_id)`；内容消费事件：`v_product_detail`, `v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail`, `v_kol_post_detail` |
| next_day_retention_rate | 次日仍活跃新增注册用户数 / 当日新增注册用户数；基于 `dws_user_daily.is_new_user = TRUE` 的 `prop_user_id` cohort，同平台关联 `dt+1` 活跃；未成熟返回 `NULL` |
| day_7_retention_rate | 第七日仍活跃新增注册用户数 / 当日新增注册用户数；基于 `dws_user_daily.is_new_user = TRUE` 的 `prop_user_id` cohort，同平台关联 `dt+7` 活跃；未成熟返回 `NULL` |
| new_download_count | AppsFlyer 当日新增下载量；iOS / Android 取 `install`，按 `America/Toronto` 切天，h5=0 |

补充说明：
- `dws_user_daily` / `dws_device_daily` 的活跃与首活都会排除 `send_push`、`send_email` 这类被动触达事件；它们不能单独把用户或设备算成新增、活跃或留存

## 依赖

- 上游：`dws_device_daily`, `dws_user_daily`, `dws_appsflyer_download_daily`
- 脚本：`jobs/ads/ads_daily_new.py`
- DDL：`ddl/ads/ads_daily_new.sql`
