# 用增报表

## 报表

- 表：`ads_daily_new`
- 粒度：`dt × platform`
- Dashboard：[【AI-Fashion】用增报表](https://bi.alvinclub.ca/superset/dashboard/88/)（id=88）

## 字段

| 字段 | 口径 |
|------|------|
| device_count | 当日首次活跃的去重设备数（`dim_device_first_active.first_active_date = dt`） |
| user_count | 当日首次活跃的去重注册用户数（`dim_user_first_active.first_active_date = dt`） |
| avg_duration_sec | `SUM(session_duration_sec) / COUNT(DISTINCT prop_device_id)`；`app_launch` 的 `args_session_duration` 按 `COALESCE(raw_event_id, hash_id)` 去重后求和，单位秒 |
| avg_content_consume_count | `SUM(content_consume_count) / COUNT(DISTINCT prop_device_id)`；内容消费事件：`v_product_detail`, `v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail`, `v_kol_post_detail` |
| next_day_retention_rate | 次日仍活跃新增设备数 / 当日新增设备总数 |
| new_download_count | 应用商店当日新增下载量；iOS=`product_type_identifier = 1`，Android=`daily_device_installs`，h5=0 |

## 依赖

- 上游：`dws_device_daily`, `dws_user_daily`, `dws_download_daily`, `dim_device_first_active`, `dim_user_first_active`
- 脚本：`jobs/ads/ads_daily_new.py`
- DDL：`ddl/ads/ads_daily_new.sql`
- 编排：`jobs/ads/ads_daily.py`
