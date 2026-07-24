# 【Launch】商详页多维度 踩坑记录

## Superset table chart 用计算列做 ORDER BY 时，orderby 要传完整表达式对象

- 背景：周报明细表 / 周报来源概览把 `week_start_dt` + `week_end_dt` 合并展示为单一 `周期`（`CONCAT(FORMAT_DATE(...))` 的 SQL 表达式列），不再展示原始两个日期列
- 踩坑：`order_by_cols` / `query_context.queries[0].orderby` 如果只传该列的中文 `label` 字符串（如 `"周期"`），Superset 报错 `Unknown column used in orderby`；如果改传底层未展示的物理列（如 `week_start_dt`），BigQuery 报错 `ORDER BY clause expression references column ... which is neither grouped nor aggregated`
- 修复：`orderby` / `order_by_cols` 必须传与 `all_columns` 里完全一致的**表达式对象**（`{"expressionType": "SQL", "label": ..., "sqlExpression": ...}`），而不是列名字符串或 label 字符串
- 排查提示：table chart 的 raw 模式下，任何未直接以原始列名出现在 `all_columns` 里的展示列（计算列、重命名列），排序都必须引用同一个表达式对象

## DWS 重建后要重跑全部下游 ADS，不能只看首次跑数结果

- 背景：`dws_launch_product_detail_device_daily` 从 `dt × device_id` 扩展为含 5 个维度后，用 staging + swap 方式重建过一次
- 踩坑：重建后曾出现下游多维度 ADS 表里 `video_id` 等维度列为 `NULL`（并非 `COALESCE` 兜底值），导致按 `video_id` 分桶的指标（如有来源 UV / 无来源 UV）全部落空
- 排查提示：怀疑是重建过程中存在时序竞争或中间态被覆盖；根因未完全定位。任何一次 DWS 全量重建或逻辑变更后，除了校验 DWS 本身，务必重跑并抽查全部下游 ADS 表的维度列真实取值（不能只看行数和聚合结果是否“看起来正常”），必要时重跑 DWS 后再重跑一遍全部下游脚本兜底
