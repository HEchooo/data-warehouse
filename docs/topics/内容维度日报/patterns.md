# 内容维度日报经验

## `c_follow` 自身没有帖子主键

- 关注归因不能直接拿 `c_follow` 做帖子级统计
- 必须按同一天内的帖子详情曝光做 last-touch 归因

## `post_id` 有值但 `column_id / column_name` 为空

- 根因是 last-touch 命中的详情曝光缺少 `args_star / args_magazine / args_brand`
- 修复时要同时过滤 `follow_daily` 和 keys 分支里的空 `module / column_id`

## `column_name` 为空时，不一定是映射表问题

- 还要先检查 `column_id` 是否带了额外双引号
- 这块修复逻辑和专栏追踪明细的映射坑是联动的

## 这类日报直接读 `dwd_event_log`

- 不经过 `dws_*` 中转
- 曝光、点赞、关注归因异常时，先回看详情事件、`visitor_id` 和 `raw_event_id` 去重逻辑

## 新增维度字段后，不能只改增量 ETL

- `creator` 是给已有 ADS 表新增字段，历史分区也要一起补
- 仅等后续增量跑批，会导致 dashboard 默认 `Last week` 里出现新旧分区混用
- 优先用 BigQuery 安全换表回填历史，再刷新 Superset dataset 元数据

## 帖子级率指标聚合时要按曝光 UV 做加权

- `Top帖子` 这类跨天聚合图表里，`like_rate`、`read_rate`、`follow_rate` 不能直接 `AVG`
- 正确做法是按 `post_exposure_uv` 加权，等价于先聚合分子分母，再算整体率
