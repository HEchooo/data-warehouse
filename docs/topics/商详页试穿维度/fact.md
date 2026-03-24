# 商详页试穿维度

## 报表

- 表：`ads_daily_product_tryon_performance`
- 粒度：`dt × spu × sku`
- Dashboard：[【AI-Fashion】商详页试穿维度](https://bi.alvinclub.ca/superset/dashboard/98/)（id=98）

## 字段

| 字段 | 口径 |
|------|------|
| exposure_uv | 商品曝光 UV；事件：`v_product_detail` + `v_home_feeds`；仅统计 `product_code` 非空的行 |
| tryon_total_count | 商品被试穿次数（PV）；事件：`c_tryon`，限 `args_page_key='product_detail'`；按 `COALESCE(raw_event_id, hash_id)` 去重 |
| add_cart_uv | 商品加购 UV；事件：`c_add_cart`；全量加购，不区分来源 |

补充说明：
- SPU：`dwd_event_log.product_code`
- SKU：`dwd_event_log.args_sku`，去掉首尾双引号，`''/null` 视为 NULL
- 访客去重：`visitor_id = COALESCE(NULLIF(prop_user_id,''), NULLIF(prop_device_id,''))`

## 依赖

- 上游：`dwd_event_log`
- 脚本：`jobs/ads/ads_daily_product_tryon_performance.py`
- DDL：`ddl/ads/ads_daily_product_tryon_performance.sql`
- 编排：`jobs/ads/ads_daily.py`
