# 试穿与商品行为临时取数（2026-05）踩坑记录

## `product_code` 非空 + `c/v` 前缀事件集合远大于预期

- 第 11/12 条若直接写成“`event_name` 以 `c/v` 开头且 `product_code` 非空”，会纳入大量商品相关曝光事件，不仅是 `home/shop`。
- 临时探查结果显示，`v_shop_feeds`、`v_tryon_result_detail`、`v_category_product`、`v_post_product` 等事件量级明显更大。
- 因此在看板阶段应明确是否需要：
  - 保持全量 `c* / v*` 商品事件口径；
  - 或收敛到业务指定事件白名单（例如仅 `home/shop/product_detail`）。

## 日均类指标需要先补全日历再聚合

- 这类口径要求固定按 `25` / `30` 天取均值，不能只对“有数据的日期”做 `AVG`。
- 临时 SQL 通过日期骨架补全后再聚合，避免“无数据日被自动忽略”导致均值偏高。
