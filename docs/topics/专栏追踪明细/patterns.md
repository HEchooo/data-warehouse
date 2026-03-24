# 专栏追踪明细经验

## `Unrecognized name: prop_user_id`

- 根因是 CTE 从 `base` 读取，但仍引用了 `prop_user_id / prop_device_id`
- follow 相关聚合应直接使用 `base` 中已经算好的 `visitor_id`

## `column_id` 带双引号会让 `column_name` 映射为空

- 排查 `column_name` 为空时，不要只看 join
- 先确认 `column_id` 是否需要先做 `clean_id_expr` 清洗

## 这类日报直接读 `dwd_event_log`，不要错回看 `dws`

- 专栏曝光、关注或阅读异常时，先回看事件名、`visitor_id` 和 `raw_event_id` 去重

## `creator` 粒度升级后，`follow_total_count` 不能顺手改口径

- `c_follow` 的 `creator` 只能在它自身已有的 `module + column_id` 范围内补归因
- 否则会把历史 `follow_total_count` 放大
- 回填前要做聚合校验，要求原 `dt × module × column_id` 视角下的 `follow_total_count / read_post_count` 保持不变

## `creator` 维度 join 后不要再用含糊的 `USING`

- 加了 `creator` 之后多个 CTE 都带同名字段
- 应改成显式 `ON cm.module = k.module AND cm.column_id = k.column_id`
