# 【AI-Fashion】商详页投放 - 踩坑记录

## 通过 API 建 Superset dashboard 时，只写 `position_json` 不会真正挂载 chart

- 现象：dashboard 129 页面上两个 chart 位置都显示 "There is no chart definition associated with this component, could it have been deleted?"
- 原因：`POST /api/v1/dashboard/` 时把 chart 布局信息写进了 `position_json`（`CHART-xxx.meta.chartId`），但 Superset 的 dashboard-chart 关联是另一个多对多关系（`chart.dashboards`），`position_json` 只负责布局排版，不会自动建立这个关联；chart 侧当时也没有设置 `dashboards` 字段
- 处理：对每个 chart 单独 `PUT /api/v1/chart/{chart_id}`，body 为 `{"dashboards": [dashboard_id]}`，把 chart 显式挂到目标 dashboard 上
- 验证：`GET /api/v1/dashboard/{id}` 返回的 `charts` 字段（chart 名称列表）非空，且每个 `GET /api/v1/chart/{chart_id}` 返回的 `dashboards` 字段包含目标 dashboard id
- 教训：之后凡是走 API 新建 dashboard + chart，`position_json` 写完后必须再对每个 chart 补一次 `PUT .../chart/{id}` 设置 `dashboards`，光靠 `chart/data` 验证查询正确不足以发现这个问题（那只验证了 chart 本身能出数据，不验证 dashboard-chart 关联）

## 当前记录

- 埋点上线首日的核查清单见 `fact.md` 的「当前状态」小节，若发现问题请把排查过程记录到这里
