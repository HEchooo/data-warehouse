# 试穿结果转化经验

## 首次跑通前先确认表是否已经创建

- 排查任务失败时，不要先假设是 SQL 或 Superset 问题

## 看板名称带固定前缀

- 最终命名是 `【AI-Fashion】试穿结果转化`
- 后续如果脚本或对象名对不上，先确认是不是命名没带前缀

## `add_cart_rate` 大于 1 不一定是 bug

- 这张表的 `add_cart_uv` 是全量加购 UV，不做试穿归因
- 因此 `add_cart_rate = add_cart_uv / tryon_uv` 可以大于 1
- Batch 5 后先查 `dws_tryon_user_daily` 的 `has_tryon / has_add_cart` 聚合是否符合预期
