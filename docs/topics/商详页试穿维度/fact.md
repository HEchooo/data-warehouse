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

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| SPU | `spu` | 商品 | `dwd_event_log.product_code` | 按目标 SPU 过滤 | 不聚合 |
| SKU | `sku` | 商品 | `dwd_event_log.args_sku`，去掉首尾双引号，`''` / `null` 视为 `NULL` | 按目标 SKU 过滤 | 不聚合 |
| 商品曝光 UV | `exposure_uv` | 访客（`visitor_id`） | 事件 `v_product_detail` + `v_home_feeds`，按 `visitor_id = COALESCE(NULLIF(prop_user_id,''), NULLIF(prop_device_id,''))` 去重 | 仅统计 `product_code` 非空的行 | 不可跨天、跨 SPU 或跨 SKU 直接求和；汇总须回访客级事实重算 |
| 商品试穿次数 | `tryon_total_count` | 事件 | 事件 `c_tryon` 的 PV，按 `COALESCE(raw_event_id, hash_id)` 去重 | `args_page_key = 'product_detail'` | 可跨天、跨 SPU 或跨 SKU 求和 |
| 商品加购 UV | `add_cart_uv` | 访客（`visitor_id`） | 事件 `c_add_cart` 的全量加购去重访客数，不区分来源 | 无 | 不可跨天、跨 SPU 或跨 SKU 直接求和；汇总须回访客级事实重算 |

## 依赖

- 上游：`dwd_event_log`
- 中间表：`dws_product_behavior_daily`
- 脚本：`jobs/ads/ads_daily_product_tryon_performance.py`
- DDL：`ddl/ads/ads_daily_product_tryon_performance.sql`
- 编排：独立任务 `jobs/ads/ads_daily_product_tryon_performance.py`
