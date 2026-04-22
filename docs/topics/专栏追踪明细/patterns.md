# 专栏追踪明细经验

## `Unrecognized name: prop_user_id`

- 根因是 CTE 从 `base` 读取，但仍引用了 `prop_user_id / prop_device_id`
- 历史 visitor 口径下，follow 相关聚合应直接使用 `base` 中已经算好的 `visitor_id`
- 当前已登录口径下，对应地应直接使用 `base` 中已经过滤好的 `user_id`

## `column_id` 带双引号会让 `column_name` 映射为空

- 排查 `column_name` 为空时，不要只看 join
- 先确认 `column_id` 是否需要先做 `clean_id_expr` 清洗

## 这类日报直接读 `dwd_event_log`，不要错回看 `dws`

- 专栏曝光、关注或阅读异常时，先回看事件名、`prop_user_id` 登录过滤和 `raw_event_id` 去重

## 商品事件映射到专栏时，只能走 `args_post`

- `dwd_event_log` 的商品行会把 `args_spu` 展开成 `product_code`，但同一行上的 `post_code` 常常为空
- 这类事件要按上报原文里的 `args_post` 反推帖子，再通过 `community_post.poster_user_id -> kol_rel.user_id` 映射到专栏
- 不要对商品事件混用 `args_star / args_magazine / args_brand` 作为兜底，否则同一套商品指标会出现两套归因口径
- `args_post` 为空、`[]`、`null`，或无法映射到 `community_post` 的商品事件，应直接排除在专栏明细表之外

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

## 改成“已登录用户口径”时，阅读类分子也要一起收口

- 专栏报表如果把 `column_exposure_uv`、`follow_uv` 这类 UV 改成 `COUNT(DISTINCT prop_user_id)`，不能保留原来的 `visitor_id` 阅读分子
- 否则会出现 `avg_read_post_count_per_user = 匿名+登录阅读帖子数 / 已登录曝光 UV` 的混合口径
- 正确做法是事件侧先过滤 `prop_user_id IS NOT NULL AND prop_user_id != ''`，再按 `CONCAT(prop_user_id, '-', post_code)` 统计 `read_post_count`

## 在线帖子存量快照不能和试穿日指标混表

- 不要把“在线帖子存量快照”和“试穿日发生指标”混在一张快照表里
- `dws_column_post_daily` 只负责 `community_post.status = 1` 的在线帖子存量快照
- `common_ext_ai_task` 的试穿帖子数 / 试穿点击人数属于日发生指标，应直接在 ADS 当天计算
- 这类快照脚本也不要挂到高频 `jobs/dws/dws_daily.py`，应独立日调度；否则会把“多伦多 0 点后固化昨日快照”的语义跑歪
