# 首页模块曝光与点击明细

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | 首页模块曝光与点击明细 | 首页模块曝光/点击明细（日×模块） |
| 指标 | 模块曝光 UV | 模块曝光人数 |
| 指标 | 模块点击 UV | 模块点击人数 |
| 指标 | 模块点击 PV | 模块点击次数 |
| 指标 | click_rate | 模块点击率 |

## 适用范围

- 业务线：`Alvin's Club App`
- 适合：按日、首页模块查询曝光 UV、点击 UV 和点击率。
- 不适合：新老用户拆分应使用[首页新老用户曝光点击](../首页新老用户曝光点击/fact.md)；月度模块内容表现应使用[模块追踪明细](../模块追踪明细/fact.md)。
- 需要澄清：需要确认首页模块、日期范围和曝光或点击指标。

## 报表

### ads_daily_home_module_performance

- 表：`my-project-8584-jetonai.decom.ads_daily_home_module_performance`
- 状态：`active`
- 粒度：`dt × module`
- 时区：`America/Toronto`
- 数据范围：首页各模块的曝光与点击日表现。
- 必要过滤：按目标 `module` 过滤。
- 不包含：新老用户拆分和自然月内容模块汇总。
- Dashboard：[【AI-Fashion】首页模块曝光/点击明细（日×模块）](https://bi.alvinclub.ca/superset/dashboard/92/)（id=92）

## 字段

| 字段 | 口径 |
|------|------|
| module_exposure_uv | 已登录用户模块曝光 UV；`COUNT(DISTINCT prop_user_id)`，且仅统计 `prop_user_id IS NOT NULL AND prop_user_id != ''` 的曝光事件 |
| module_click_uv | 已登录用户模块点击 UV；`COUNT(DISTINCT prop_user_id)`，且仅统计 `prop_user_id IS NOT NULL AND prop_user_id != ''` 的点击事件 |
| click_rate | 点击 UV / 曝光 UV |
| click_pv | 已登录用户模块点击 PV；仅统计 `prop_user_id IS NOT NULL AND prop_user_id != ''` 的点击事件，并按 `raw_event_id` 去重 |
| avg_click_count_per_user | 点击 PV / 点击 UV |

补充说明：
- 固定模块：`star`, `magazine`, `brand`, `feeds`
- 登录用户口径：所有指标仅纳入 `prop_user_id` 非空的事件
- 事件映射：`v_home_* / c_home_*`

## 依赖

- 上游：`dwd_event_log`
- 中层事实：`dws_home_module_daily`
- 中层脚本：`jobs/dws/dws_home_module_daily.py`
- 中层 DDL：`ddl/dws/dws_home_module_daily.sql`
- 终层脚本：`jobs/ads/ads_daily_home_module_performance.py`
- DDL：`ddl/ads/ads_daily_home_module_performance.sql`
- 编排：`env/etl_config.json`（`dws_home_module_daily`、`ads_daily_home_module_performance`）

## 临时取数

### 首页模块内容曝光 UV Top 30（2026-06）

- 类型：一次性临时取数，不属于正式报表，不新增 ADS 表，不修改或回填生产数据
- 时间：`2026-06-01 ~ 2026-06-07`，按 `America/Toronto` 分天
- 模块及事件：
  - `star`：`v_home_star`，内容 ID 来自 `args_star`
  - `magazine`：`v_home_magazine`，内容 ID 来自 `args_magazine`
  - `brand`：`v_home_brand`，内容 ID 来自 `args_brand`
- 指标：每个内容 ID 的 `COUNT(DISTINCT prop_user_id)`，仅统计已登录用户
- 排名：每天在每个模块内按曝光 UV 降序取前 30；UV 相同时按内容 ID 升序
- 输出：`exposure_dt / module / column_id / exposure_uv / uv_rank`
- SQL：`query_content_exposure_top30_20260601_20260607.sql`
- CSV：`outputs/首页模块内容曝光UV Top30（2026-06）/result.csv`
- 后续处理：若需求转为固定报表，应先评估下沉到 DWS 的内容 ID 粒度事实，不直接把本临时 SQL 作为 ADS 长期实现
