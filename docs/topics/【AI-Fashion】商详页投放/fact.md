# 【AI-Fashion】商详页投放

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | AI-Fashion 商详页投放 | 商详页投放看板 |

## 报表

### 主题总览

- 主题：监控从带投放参数的落地页进入商详页的曝光效果，以及从商详页点击跳转到第三方商品的转化效果
- 表：`ads_daily_ai_fashion_product_detail_promotion`，粒度 `dt × ac_creative`（含 `ac_creative = 'overall'` 全素材去重汇总行）
- Dashboard：[129](https://bi.alvinclub.ca/superset/dashboard/ai-fashion-product-detail-promotion/)
- 与已有主题的边界：
  - 与 `商详页试穿维度`（`ads_daily_product_tryon_performance`，dashboard 98）、`decom.dws_product_detail_daily` 不是同一口径 —— 那两者的 UV 是 `COALESCE(prop_user_id, prop_device_id)`，本主题严格按 `prop_device_id` 去重，且只统计带投放参数的流量，因此独立建表，不复用
  - `c_third_party_goods` 与 `LTA 数据看板`（dashboard 125）共用同一事件，`LTA` 只看 `args.current_url` 域名为 `mycch.me` 的流量，本主题只看 `prop_url` 域名为 `https://alvinclub.com` 的流量，两者互斥、无重叠

### 投放流量界定（硬性条件）

同时满足以下两个条件才算「投放流量」：
- `prop_url` 的 origin **严格等于** `https://alvinclub.com`：`REGEXP_EXTRACT(prop_url, r'^(https?://[^/]+)') = 'https://alvinclub.com'`（不含 `www.`，不含 `.ai` / `.ca` 等其他站点）
- `prop_params` 中 `ac_channel`、`ac_creative` **同时存在且值非空**：`REGEXP_CONTAINS(prop_params, r'[?&]ac_channel=[^&]')` 且同理 `ac_creative`
- 不限定 `args.page_key`（url + params 已足够界定来源）

已知取舍：
- `ac_channel` / `ac_creative` 的值保持 URL 原样，**不做 percent-decode**（BigQuery 无内置 decode 函数）
- 参数名区分大小写，只认小写 `ac_channel` / `ac_creative`
- 同名参数重复出现时取第一个匹配
- 实测 `prop_url` 非空 ⟺ `prop_app_type = 'h5'`，即投放流量必然是 H5 落地页流量，不会误伤 App 内曝光；**若未来投放落地页改为 App WebView 且 `prop_app_type` 不是 `h5`，域名过滤会全部落空**，需回头确认口径

### 事件名大小写（易错点）

- 前端埋点上报名为 `v_Product_Detail`
- `decom.dwd_event_log` 落库时统一小写化（`jobs/dwd/dwd_event_log/dwd_event_log.py` 的 `row.event_name.lower()`），实际存储值只有小写 `v_product_detail`
- 所有 SQL 必须用小写 `v_product_detail` 匹配，按抓包结果写大写会永远查不到数据

### 当前数据现状

- 截至 `2026-07-31`，满足上述投放流量界定的数据为 **0 条**（`ac_channel` 参数尚未上报）。本次先建管道与看板骨架，投放埋点上报后自然出数，无需额外操作
- 放宽投放参数（只加域名过滤）的历史回归基线：`v_product_detail` 12 PV / 6 UV（2026-02-24 ~ 2026-05-29）、`c_third_party_goods` 1 PV / 1 UV（2026-05-28），可用于核对聚合逻辑本身无误

## 字段

| 字段 | 含义 | 计算 |
|---|---|---|
| `dt` | 日报日期，美东时区（`America/Toronto`）切天 |  |
| `ac_creative` | 投放素材 ID（投放平台原始 UUID，与 `dim_asset.asset_code` 无关，仓库内无名称映射）；`'overall'` 为全素材汇总行 |  |
| `product_detail_exposure_uv` | 商详页曝光 UV | `v_product_detail` 的 `COUNT(DISTINCT prop_device_id)` |
| `product_detail_exposure_pv` | 商详页曝光 PV | `v_product_detail` 的 `COUNT(DISTINCT raw_event_id)` |
| `third_party_click_uv` | 点击跳转 UV | `c_third_party_goods` 的 `COUNT(DISTINCT prop_device_id)` |
| `third_party_click_pv` | 点击跳转 PV | `c_third_party_goods` 的 `COUNT(DISTINCT raw_event_id)` |

## 已落地字段

需求字段已全部落地，无缺口。

## 依赖

### 链路

```
decom.dwd_event_log（v_product_detail 曝光 + c_third_party_goods 点击跳转）
  -> decom.dws_ai_fashion_product_detail_promotion_device_daily
  -> decom.ads_daily_ai_fashion_product_detail_promotion
```

### 表与脚本

- `decom.dws_ai_fashion_product_detail_promotion_device_daily`：投放商详页设备日事实层，粒度 `dt × ac_channel × ac_creative × device_id`，脚本 `jobs/dws/dws_ai_fashion_product_detail_promotion_device_daily.py`，DDL `ddl/dws/dws_ai_fashion_product_detail_promotion_device_daily.sql`
  - 先按 `raw_event_id` 去重（`QUALIFY ROW_NUMBER() ... = 1`）再分组，保证每个事件只归属唯一维度组合，ADS 端 `SUM(pv)` 恒等于 `COUNT(DISTINCT raw_event_id)`
  - `device_id` 为空的事件保留为 `NULL`：计入 PV，由下游 `COUNT(DISTINCT device_id)` 自动忽略、不计入 UV
  - 刻意不使用「目标表行数为 0 → 全量」判定：本主题过滤后当前 0 条数据，行数判定会让每轮调度都全表扫描 `dwd_event_log`（实测约 317 MB/次）且永远写不进行，形成死循环。改用「最近 6 小时 `update_time` + 固定补最近两天」，全量重刷用 `--full-refresh` 人工触发（`FULL_REFRESH_START_DT = 2026-02-09`，`v_product_detail` 最早业务日）
- `decom.ads_daily_ai_fashion_product_detail_promotion`：脚本 `jobs/ads/ads_daily_ai_fashion_product_detail_promotion.py`，DDL `ddl/ads/ads_daily_ai_fashion_product_detail_promotion.sql`
  - `overall` 行必须单独按设备全局去重，不能对各素材 UV 直接求和（同一设备当日可能看过多个素材）
  - 素材明细行只出现实际有数据的 `(dt, ac_creative)` 组合，不做历史素材 `CROSS JOIN` 补零（否则行数随投放规模随时间爆炸）
  - 全量分支起始日取模块常量 `REPORT_START_DT = 2026-07-31`（看板上线日），不取 DWS 的 `MIN/MAX dt`：本主题 DWS 长期可能是空表，用 DWS 边界推日期会得到空列表，导致每轮都回全量分支
  - 每天固定补 `ac_creative = 'overall'` 全零行，保证目标表永远有行，Superset 才有行可渲染

### 规则

- `docs/rules/pipeline.md`：`## 主链路` 的「站外与投放」链路拓扑，以及 `## 跨主题约束` 中 `c_third_party_goods` 的域名隔离要求
- `docs/rules/data.md`：时间切天、去重、聚合规则

### Superset

- 物理 dataset：`ads_daily_ai_fashion_product_detail_promotion`（id=292）
- chart（自上而下即 dashboard 展示顺序）：
  1. `商详页投放趋势`（id=718，`echarts_timeseries_line`，x 轴 `dt`，4 个指标折线）
  2. `商详页投放明细`（id=717，`table`，列：日报日期、投放素材、商详页曝光UV、商详页曝光PV、点击跳转PV、点击跳转UV，按 `dt` 降序）
- dashboard：`【AI-Fashion】商详页投放`（id=129，slug `ai-fashion-product-detail-promotion`）
- native filter：
  1. `日报日期`：`filter_time`，默认「美东昨日」：`DATEADD(DATETRUNC(DATEADD(DATETIME("now"), -5, hour), day), -1, day) : DATETRUNC(DATEADD(DATETIME("now"), -5, hour), day)`，作用于两个 chart
  2. `投放素材`：`filter_select` on `ac_creative`，**默认值 `overall`**（若不设默认值，chart 会把 `overall` 行和素材明细行一起 `SUM`，PV/UV 翻倍），作用于两个 chart
- 图表展示列使用中文 label（`all_columns` 用 `{expressionType: SQL, label: 中文, sqlExpression: "列名 "}` 形式，`sqlExpression` 末尾补一个空格，这是当前 Superset 实例的已知实测修正方式）
- 已用 `chart/data` 真实查询验证：默认 filter（`ac_creative='overall'`）下两个 chart 均返回 1 行 `2026-07-31`，4 个指标全 0，与 BigQuery 源数据一致

### 当前状态

- 截至 `2026-07-31`：两张表 DDL 已建，两个脚本已实现并通过验证（正则单元测试、放宽口径回归基线、严格口径为 0、分区裁剪扫描量守门、补零幂等性），已注册进 `env/etl_config.json` 与 `docs/rules/pipeline.md`
- 截至 `2026-07-31`：Superset dataset / chart / dashboard / native filter 已创建并通过 `chart/data` 真实查询验证
- 埋点上线首日必做核查：① `prop_url` 的 origin 是否真是 `https://alvinclub.com`（不是 `www.` / `.ai`）② `prop_url` 是否非空（若投放落地页走 App WebView 且 `prop_app_type` 不是 `h5`，域名过滤会全部落空）；任一不满足需回到口径讨论，不得私自放宽过滤条件
