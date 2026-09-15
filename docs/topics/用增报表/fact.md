# 用增报表

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | 用增报表 | 用增表 |
| 指标 | 设备数 | 新增设备数、首活设备数 |
| 指标 | 用户数 | 新增注册用户数 |
| 指标 | new_download_count | 新增下载数、新增安装数 |
| 指标 | next_day_retention_rate | 次日留存率、D1 留存率 |
| 指标 | day_7_retention_rate | 7 日留存率、D7 留存率 |

## 适用范围

- 业务线：`Alvin's Club App`
- 适合：查询 App 内按天、按平台统计的新增设备、新增注册用户、新增下载、匿名设备占比和新增用户留存。
- 不适合：全量活跃设备、全量活跃用户和全量设备留存应使用[全量报表](../全量报表/fact.md)；跨业务线核心经营汇总应使用[投资人报表](../投资人报表【核心业务报表】/fact.md)。
- 需要澄清：问题未说明设备还是注册用户、平台、日期范围或留存 cohort 时，需要先确认。

## 报表

### ads_daily_new

- 表：`my-project-8584-jetonai.decom.ads_daily_new`
- 状态：`active`
- 粒度：一行代表一个 `dt × platform`
- 时区：`America/Toronto`
- 数据范围：App 内当日首次活跃设备、新增注册用户及 AppsFlyer 新增下载。
- 必要过滤：无。
- 不包含：全量活跃主体；仅有 `send_push`、`send_email` 等被动触达事件的设备或用户。
- Dashboard：[【AI-Fashion】用增报表](https://bi.alvinclub.ca/superset/dashboard/88/)（id=88）

## 字段

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 新增设备数 | `device_count` | 设备 | `dws_device_daily.is_new_device = TRUE` 的去重设备数 | 按目标 `platform` 过滤 | 不可跨平台直接求和 |
| 新增注册用户数 | `user_count` | 用户 | `dws_user_daily.is_new_user = TRUE` 的去重注册用户数 | 按目标 `platform` 过滤 | 不可跨平台直接求和 |
| 新增用户登录率 | `new_user_login_rate` | 设备 / 用户 | `user_count / device_count` | 按目标 `platform` 过滤 | 不可跨天或跨平台求和、平均 |
| 匿名设备占比 | `anonymous_device_rate` | 设备 | 当日 `has_anonymous_event = TRUE` 的去重设备数 / 当日全部去重设备数 | 按目标 `platform` 过滤 | 不可跨天或跨平台求和、平均 |
| 匿名老设备占比 | `anonymous_old_device_rate` | 设备 | 当日 `has_anonymous_event = TRUE AND is_new_device = FALSE` 的去重设备数 / 当日全部去重设备数 | 按目标 `platform` 过滤 | 不可跨天或跨平台求和、平均 |
| 新增设备人均停留时长 | `avg_duration_sec` | 设备 | `SUM(session_duration_sec) / COUNT(DISTINCT prop_device_id)`；`app_launch.args_session_duration` 按 `COALESCE(raw_event_id, hash_id)` 去重，单位秒 | `is_new_device = TRUE`；按目标 `platform` 过滤 | 不可跨天或跨平台求和、平均 |
| 新增设备人均内容消费 | `avg_content_consume_count` | 设备 | `SUM(content_consume_count) / COUNT(DISTINCT prop_device_id)`；内容消费事件为 `v_product_detail`、`v_star_post_detail`、`v_magazine_post_detail`、`v_brand_post_detail`、`v_kol_post_detail` | `is_new_device = TRUE`；按目标 `platform` 过滤 | 不可跨天或跨平台求和、平均 |
| 次日留存率 | `next_day_retention_rate` | 新增注册用户 cohort | `dt+1` 同平台仍活跃用户数 / 当日新增注册用户数；未成熟返回 `NULL` | `is_new_user = TRUE`；按目标 `platform` 过滤 | 不可跨天或跨平台求和、平均 |
| 7 日留存率 | `day_7_retention_rate` | 新增注册用户 cohort | `dt+7` 同平台仍活跃用户数 / 当日新增注册用户数；未成熟返回 `NULL` | `is_new_user = TRUE`；按目标 `platform` 过滤 | 不可跨天或跨平台求和、平均 |
| 新增下载数 | `new_download_count` | 安装 | AppsFlyer 当日新增下载量；iOS / Android 取 `install`，h5=0 | 按目标 `platform` 过滤 | 可跨天求和；不可把平台汇总与分平台行同时求和 |

## 依赖

- 上游：`dws_device_daily`, `dws_user_daily`, `dws_appsflyer_download_daily`
- 脚本：`jobs/ads/ads_daily_new.py`
- DDL：`ddl/ads/ads_daily_new.sql`
- 规则：`docs/rules/data.md`
