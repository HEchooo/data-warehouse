# 商详页试穿维度

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | 商详页试穿维度 | 商详页试穿看板 |
| 指标 | exposure_uv | 商详页曝光 UV、商详曝光人数 |
| 指标 | 试穿次数 | 试穿数 |
| 指标 | 加购 UV | 加购人数、加入购物车用户数 |

## 适用范围

- 业务线：`Alvin's Club App`
- 适合：按商品 SPU、SKU 查询商详页曝光、试穿和试穿次数。
- 不适合：带投放参数的 H5 商详页应使用[AI-Fashion 商详页投放](../【AI-Fashion】商详页投放/fact.md)；试穿后的加购转化应使用[试穿结果转化](../试穿结果转化/fact.md)。
- 需要澄清：需要确认 SPU 或 SKU、日期范围和曝光或试穿指标。

## 报表

### ads_daily_product_tryon_performance

- 表：`my-project-8584-jetonai.decom.ads_daily_product_tryon_performance`
- 状态：`active`
- 粒度：`dt × spu × sku`
- 时区：`America/Toronto`
- 数据范围：App 商详页商品曝光和试穿日表现。
- 必要过滤：按目标 SPU 或 SKU 过滤。
- 不包含：投放 H5 流量和试穿后加购。
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
- `exposure_uv`、`add_cart_uv` 均按访客去重；不可跨天、SPU 或 SKU 直接求和，汇总时必须回到访客级事实重算

## 依赖

- 上游：`dwd_event_log`
- 中间表：`dws_product_behavior_daily`
- 脚本：`jobs/ads/ads_daily_product_tryon_performance.py`
- DDL：`ddl/ads/ads_daily_product_tryon_performance.sql`
- 编排：独立任务 `jobs/ads/ads_daily_product_tryon_performance.py`
