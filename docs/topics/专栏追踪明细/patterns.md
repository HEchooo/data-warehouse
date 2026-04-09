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

## UV 口径不能再直接 `COALESCE(user_id, device_id)`

- 新需求要求“同日游客后续登录只算 1 个 UV”，直接 `COALESCE` 会把同一人拆成两个主体
- 正确做法是先做同日 `device_id -> user_id` 归并，再生成统一 `visitor_id`
- 对“同日同 device 关联多个 user_id”的边界，人数口径应按 `COUNT(DISTINCT user_id)`，游客态不再额外计 UV

## 在线帖子存量快照不能和试穿日指标混表

- 不要把“在线帖子存量快照”和“试穿日发生指标”混在一张快照表里
- `dws_column_post_daily` 只负责 `community_post.status = 1` 的在线帖子存量快照
- `common_ext_ai_task` 的试穿帖子数 / 试穿点击人数属于日发生指标，应直接在 ADS 当天计算
- 这类快照脚本也不要挂到高频 `jobs/dws/dws_daily.py`，应独立日调度；否则会把“多伦多 0 点后固化昨日快照”的语义跑歪
