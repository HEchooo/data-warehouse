# 全量报表

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | 全量报表 | 全平台 |
| 指标 | 设备数 | DAU、活跃设备数、日活设备数 |
| 指标 | 用户数 | 注册用户 DAU、活跃注册用户数 |
| 指标 | next_day_retention_rate | 次日留存率、D1 留存率 |
| 指标 | avg_duration_sec | 人均停留时长、平均使用时长 |
| 指标 | avg_content_consume_count | 人均内容消费、人均浏览内容数 |

## 适用范围

- 业务线：`Alvin's Club App`
- 适合：查询 App 内按天、按平台统计的全量活跃设备、活跃注册用户、使用时长、内容消费和活跃设备次日留存。
- 不适合：新增设备、首活用户和新增用户留存应使用[用增报表](../用增报表/fact.md)；跨业务线核心经营汇总应使用[投资人报表](../投资人报表【核心业务报表】/fact.md)。
- 需要澄清：问题未说明设备还是注册用户、平台或日期范围时，需要先确认。

## 报表

### ads_daily_total

- 表：`my-project-8584-jetonai.decom.ads_daily_total`
- 状态：`active`
- 粒度：一行代表一个 `dt × platform`
- 时区：`America/Toronto`
- 数据范围：App 内当日有主动埋点行为的设备和注册用户。
- 必要过滤：无。
- 不包含：仅有 `send_push`、`send_email` 等被动触达事件的设备或用户；新增 cohort 专属指标。
- Dashboard：[【AI-Fashion】全量报表](https://bi.alvinclub.ca/superset/dashboard/89/)（id=89）

## 字段

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 活跃设备数 | `device_count` | 设备 | 当日有主动埋点事件的去重 `prop_device_id` 数 | 按目标 `platform` 过滤 | 不可跨天或跨平台直接求和 |
| 活跃注册用户数 | `user_count` | 用户 | 当日有主动埋点事件且 `prop_user_id IS NOT NULL` 的去重用户数 | 按目标 `platform` 过滤 | 不可跨天或跨平台直接求和 |
| 人均停留时长 | `avg_duration_sec` | 设备 | `SUM(session_duration_sec) / COUNT(DISTINCT prop_device_id)`；`app_launch.args_session_duration` 按 `COALESCE(raw_event_id, hash_id)` 去重，单位秒 | 按目标 `platform` 过滤 | 不可跨天或跨平台求和、平均 |
| 人均内容消费 | `avg_content_consume_count` | 设备 | `SUM(content_consume_count) / COUNT(DISTINCT prop_device_id)`；内容消费事件为 `v_product_detail`、`v_star_post_detail`、`v_magazine_post_detail`、`v_brand_post_detail`、`v_kol_post_detail` | 按目标 `platform` 过滤 | 不可跨天或跨平台求和、平均 |
| 次日留存率 | `next_day_retention_rate` | 设备 cohort | 次日同平台仍活跃设备数 / 当日活跃设备总数 | 按目标 `platform` 过滤 | 不可跨天或跨平台求和、平均 |

## 依赖

- 上游：`dws_device_daily`, `dws_user_daily`
- 脚本：`jobs/ads/ads_daily_total.py`
- DDL：`ddl/ads/ads_daily_total.sql`
- 规则：`docs/rules/data.md`
