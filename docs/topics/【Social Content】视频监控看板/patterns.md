# 【Social Content】视频监控看板踩坑记录

## `dt=D` 与对比基准日期 D-1 不应混淆

- `dt=D` 表示已完整结束的播放统计日，累计播放量取 D 日末值，增量为 `D day_end_views - D-1 day_end_views`
- `target_dt=D-1` 是对比基准日；两个视频池分别是 `published_dt = target_dt` 和 `published_dt < target_dt`
- Dashboard 以 `dt` 作为日期筛选字段并默认 `Last day`；只有上游 D 日完整日末分区已落地时，D 日榜单才应产出

## 视频日榜与账号周榜不能复用同一个增量窗口

- 视频日榜增量为 `D day_end_views - D-1 day_end_views`，反映 D 当天增长
- 账号周榜取 `D-7 ~ D-1` 发布的视频，增量为 `D-1 day_end_views - D-7 day_end_views`，累计播放量取 D-1 日末值
- 同一个虚拟 dataset 可以保留两套派生指标；底层只需读取 D / D-1 / D-7 三个快照分区，不需要扫描整个周区间

## Dashboard 时间筛选需要 Chart 指定时间列

- Dashboard 已配置原生 `filter_time` 及默认 `Last day`，不代表 chart 查询一定会产生日期条件
- table chart 必须同时在 `params.granularity_sqla`、`query_context.form_data.granularity_sqla` 和 `query_context.queries[].granularity` 指定 `dt`；否则 Superset 会忽略 `time_range`，导致历史表按其他排序字段直接取 `row_limit`
- 验收时不能只检查 Dashboard `native_filter_configuration`；还要强制执行带 `time_range` 的 `POST /api/v1/chart/data`，确认生成 SQL 实际包含 `dt` 上下界

## Superset 资源列表查重必须显式分页

- 当前实例的 dataset / chart / dashboard 列表接口会把单页结果限制在 100 条；即使请求 `page_size = 1000`，也不能假设拿到了全部资源
- 本次 dataset 已存在，但只查第一页时未命中，后续重复创建返回 `422`
- 创建或更新 Superset 资源前，应按返回的 `count` 逐页读取到完整结果，再按对象名称和必要的 schema / datasource 信息查重

## Chart 切换 dataset 时必须同步 datasource 关系字段

- 现象：单独调用 `POST /api/v1/chart/data` 查询成功，但 Dashboard 中 chart 显示 `Unexpected error`
- 根因：只修改了 chart 的 `params.datasource` 和 `query_context.datasource`，没有修改 chart 独立的 `datasource_id / datasource_type`；Dashboard bootstrap 会按独立关系字段重新组装 `form_data.datasource`，因此仍使用旧 dataset
- 本次实际表现为：chart 679 / 681 的保存查询使用 `272__table`，但 `GET /api/v1/dashboard/122/charts` 仍返回 `271__table`，最终形成“旧 dataset + 新字段”的无效查询
- 修复：通过 `PUT /api/v1/chart/{id}` 同时更新 `datasource_id=272`、`datasource_type=table`，并回查 Dashboard 专用接口确认 `form_data.datasource` 已变为 `272__table`
- 验收时不能只查单个 chart；至少同时回查 `GET /api/v1/dashboard/{id}/charts` 和 `GET /api/v1/dashboard/{id}/datasets`

## Table Chart 的展示列需要同步三处配置

- 只修改 `params.all_columns` 不足以保证 Dashboard 查询与展示同步；还要同步 `query_context.form_data.all_columns` 和 `query_context.queries[].columns`
- 多张同结构 Table Chart 应逐张核对列名与顺序；dataset 新增字段不会自动出现在 Chart 中
- 验收时调用 `POST /api/v1/chart/data`，同时检查返回的 `colnames`、行数和关键字段样例值
