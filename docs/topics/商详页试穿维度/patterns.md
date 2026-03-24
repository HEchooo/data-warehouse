# 商详页试穿维度经验

## `exposure_uv` 口径已经统一，不再拆来源字段

- 旧讨论里曾经按来源拆过商详页曝光和 Feed 流曝光
- 现在稳定口径是统一成单一 `exposure_uv`

## `sku` 双引号问题是真实数据清洗问题

- 问题来自 `dwd_event_log.args_sku` 里多层引号或转义
- 最终修复落在 `clean_sku_expr()`
- 如果图表里还看到旧值，先怀疑缓存或旧分区没回刷完全

## 这张表直接从事件明细聚合，不要先去翻 `dws`

- 曝光、试穿、加购都直接依赖事件明细
- 指标异常时，优先回看 `product_code`、`args_sku`、`visitor_id` 和 `COALESCE(raw_event_id, hash_id)` 的去重逻辑
