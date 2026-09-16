# 试穿结果转化

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 指标 | tryon_uv | 试穿 UV、试穿人数 |
| 指标 | 加购 UV | 加购人数、加入购物车用户数 |
| 指标 | add_cart_rate | 加购率、试穿加购转化率 |

## 适用范围

- 业务线：`Alvin's Club App`
- 适合：查询试穿用户后续加购人数和试穿加购转化率。
- 不适合：试穿入口点击应使用[穿搭工具点击使用](../穿搭工具点击使用/fact.md)；按商品拆分试穿应使用[商详页试穿维度](../商详页试穿维度/fact.md)。
- 需要澄清：需要确认日期范围，以及试穿 UV、加购 UV 或转化率。

## 报表

### ads_daily_tryon_add_cart_conversion

- 表：`my-project-8584-jetonai.decom.ads_daily_tryon_add_cart_conversion`
- 状态：`active`
- 粒度：`dt`
- 时区：`America/Toronto`
- 数据范围：App 试穿用户到加购行为的日转化。
- 必要过滤：无。
- 不包含：试穿入口点击和商品维度拆分。
- Dashboard：[【AI-Fashion】试穿结果转化](https://bi.alvinclub.ca/superset/dashboard/97/)（id=97）

## 字段

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 试穿 UV | `tryon_uv` | 访客（`visitor_id`） | 事件 `c_tryon`，先按 `COALESCE(raw_event_id, hash_id)` 去重事件，再按 `visitor_id = COALESCE(NULLIF(prop_user_id,''), NULLIF(prop_device_id,''))` 去重 | 无 | 不可跨天求和 |
| 加购 UV | `add_cart_uv` | 访客（`visitor_id`） | 事件 `c_add_cart` 的全量加购去重访客数，不做试穿归因 | 无 | 不可跨天求和 |
| 加购率 | `add_cart_rate` | 访客（`visitor_id`） | `add_cart_uv / tryon_uv`；`tryon_uv = 0` 时返回 `0`；分母只含试穿访客而分子是全量加购，数值可能大于 1 | 无 | 不可 `AVG`；跨天须用重新去重的加购 UV 与试穿 UV 重算 |

## 依赖

- 上游：`dwd_event_log`
- 中间表：`dws_tryon_confirm_daily`, `dws_tryon_user_daily`
- 脚本：`jobs/ads/ads_daily_tryon_add_cart_conversion.py`
- DDL：`ddl/ads/ads_daily_tryon_add_cart_conversion.sql`
- 编排：独立任务 `jobs/ads/ads_daily_tryon_add_cart_conversion.py`
