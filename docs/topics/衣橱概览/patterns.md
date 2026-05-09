# 衣橱概览

## 当前记录

- 暂无已沉淀的跨天排查经验；后续若出现时间切天、累计人数漂移或 Superset 聚合误用，再补充到这里。

## `OUTFIT.item_uuids` 不是稳定非空字段

- `user_fashion_closet.type = 'OUTFIT'` 的 `item_uuids` 虽然业务上应表示 outfit 包含的 item 列表，但线上实际存在 `NULL` 值
- 当报表需要把 `OUTFIT` 折算成 item 数量时，不能直接假设 `item_uuids` 一定可拆；应先做空值兜底
- 当前主题处理方式是：`item_uuids` 为空按 `0` 个 item 算；若数组里出现空字符串或 `NULL` 元素，先过滤后再计数
