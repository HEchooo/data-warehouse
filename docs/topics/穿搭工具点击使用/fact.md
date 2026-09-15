# 穿搭工具点击使用

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | 穿搭工具点击使用 | 穿搭工具点击使用日汇总、Try On 点击使用 |
| 指标 | 点击使用次数 | 试穿点击 PV、Try On 点击数 |
| 指标 | 点击使用用户数 | 试穿点击 UV、Try On 点击人数 |
| 指标 | avg_click_use_count_per_user | 人均点击使用次数、人均试穿点击数 |

## 适用范围

- 业务线：`Alvin's Club App`
- 适合：查询试穿确认点击的全量日趋势，或按帖子查看已登录用户的点击使用表现。
- 不适合：试穿后加购应使用[试穿结果转化](../试穿结果转化/fact.md)；按商品查看商详试穿应使用[商详页试穿维度](../商详页试穿维度/fact.md)。
- 需要澄清：需要确认全量或帖子粒度、日期范围，以及访客或已登录用户主体。

## 报表

### ads_daily_tryon_confirm

- 表：`my-project-8584-jetonai.decom.ads_daily_tryon_confirm`
- 状态：`active`
- 粒度：一行代表一个 `dt`
- 时区：`America/Toronto`
- 数据范围：带 `post_code` 的 `c_tryon_confirm` 事件，访客按用户 ID、设备 ID 依次回退去重。
- 必要过滤：无。
- 不包含：帖子维度和未确认的试穿事件。

### ads_daily_post_tryon_confirm

- 表：`my-project-8584-jetonai.decom.ads_daily_post_tryon_confirm`
- 状态：`active`
- 粒度：一行代表一个 `dt × post_id`
- 时区：`America/Toronto`
- 数据范围：已登录用户产生且能映射到帖子的 `c_tryon_confirm` 事件。
- 必要过滤：按目标 `post_id` 过滤。
- 不包含：匿名设备事件。
- Dashboard：[【AI-Fashion】穿搭工具点击使用日汇总（日×帖子）](https://bi.alvinclub.ca/superset/dashboard/96/)（id=96）

## 字段

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 点击使用次数 | `click_use_pv` | 事件 | 全量表按 `raw_event_id` 去重且要求 `post_code` 非空；帖子表按 `dt × post_code × raw_event_id` 去重 | 事件名为 `c_tryon_confirm` | 可跨天求和 |
| 点击使用用户数 | `click_use_uv` | 访客 / 用户 | 全量表去重 `COALESCE(prop_user_id, prop_device_id)`；帖子表只去重非空 `prop_user_id` | 帖子表仅已登录用户 | 不可跨天或跨帖子直接求和 |
| 人均点击使用次数 | `avg_click_use_count_per_user` | 访客 / 用户 | `click_use_pv / click_use_uv` | 与对应表主体一致 | 不可求和或平均；跨天须用汇总分子分母重算 |

## 依赖

### ads_daily_tryon_confirm

- 上游：`dwd_event_log`
- 中间表：`dws_tryon_confirm_daily`
- 脚本：`jobs/ads/ads_daily_tryon_confirm.py`
- DDL：`ddl/ads/ads_daily_tryon_confirm.sql`

### ads_daily_post_tryon_confirm

- 上游：`dwd_event_log`
- 中间表：`dws_tryon_confirm_post_daily`
- 实体层：`dim_post`
- 脚本：`jobs/ads/ads_daily_post_tryon_confirm.py`
- DDL：`ddl/ads/ads_daily_post_tryon_confirm.sql`
- 实体脚本：`jobs/dim/dim_post.py`
- 实体 DDL：`ddl/dim/dim_post.sql`
