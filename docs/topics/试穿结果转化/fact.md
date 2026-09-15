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

| 字段 | 口径 |
|------|------|
| tryon_uv | 试穿用户数；事件：`c_tryon`；按 `COALESCE(raw_event_id, hash_id)` 去重事件后统计 UV |
| add_cart_uv | 加购用户数；事件：`c_add_cart`；全量加购 UV，不做试穿归因 |
| add_cart_rate | `add_cart_uv / tryon_uv`；`tryon_uv=0` 时为 0 |

补充说明：
- 访客去重：`visitor_id = COALESCE(NULLIF(prop_user_id,''), NULLIF(prop_device_id,''))`
- 由于口径为全量加购 UV，`add_cart_rate` 可能大于 1

## 依赖

- 上游：`dwd_event_log`
- 中间表：`dws_tryon_confirm_daily`, `dws_tryon_user_daily`
- 脚本：`jobs/ads/ads_daily_tryon_add_cart_conversion.py`
- DDL：`ddl/ads/ads_daily_tryon_add_cart_conversion.sql`
- 编排：独立任务 `jobs/ads/ads_daily_tryon_add_cart_conversion.py`
