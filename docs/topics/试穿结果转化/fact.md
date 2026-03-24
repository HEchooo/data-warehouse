# 试穿结果转化

## 报表

- 表：`ads_daily_tryon_add_cart_conversion`
- 粒度：`dt`
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
- 脚本：`jobs/ads/ads_daily_tryon_add_cart_conversion.py`
- DDL：`ddl/ads/ads_daily_tryon_add_cart_conversion.sql`
- 编排：`jobs/ads/ads_daily.py`
