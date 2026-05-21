# push 数据看板

## 当前记录

- 已沉淀 1 条 push 召回事件口径排查经验；后续若出现发送日与已读快照口径混淆、租户混算或 Superset 聚合误用，再继续补充。

## push 召回事件口径

- 现象：`c_push` 直接按事件条数统计时，点击个数会偏大，或和发送个数无法对齐。
- 根因：原始点击事件里存在缺失 `ext.push.recall_strategy`、`ext.push.run_id` 或 `prop_user_id / prop_device_id` 的脏数据，而且同一主体同一 `run_id` 也可能出现重复事件。
- 处理：
  - 先过滤 `recall_strategy`、`run_id`、`actor_id` 任一为空的事件
  - `actor_id` 统一取 `COALESCE(NULLIF(prop_user_id, ''), NULLIF(prop_device_id, ''))`
  - 先在 DWS 按 `dt + recall_strategy + push_run_id + actor_id` 去重，再在 ADS 聚合 `send_count` / `click_count`
- 排查顺序：
  - 先核对 `send_push` / `c_push` 是否都命中过滤条件
  - 再看 `recall_strategy` 是否存在大小写不一致或脏值拆分
  - 最后再比较 ADS 与 DWS 聚合后数量是否一致
