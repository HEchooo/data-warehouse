# 【Social Content】视频监控看板踩坑记录

## 报表日期 D 与播放数据日期 D-1 不应混为一列

- 需求中的“每天 D”表示榜单报表日期；两个视频池分别是 `published_dt = D-1` 和 `published_dt < D-1`
- 播放增量观察的是 `D-1 day_end_views - D-2 day_end_views`，累计播放量取 D-1 日末值
- 如果只保留一个动态 `target_dt=D-1`，讨论历史日期时容易误写成“D 当天发布”和“D 前发布”两个池子
- 落表时同时保留 `dt=D` 与 `target_dt=D-1`，并以 `dt` 作为看板日期筛选字段

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
