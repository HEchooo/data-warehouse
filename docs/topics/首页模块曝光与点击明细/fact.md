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

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 模块 | `module` | 首页模块 | 固定取值 `star` / `magazine` / `brand` / `feeds`，事件映射 `v_home_* / c_home_*` | 按目标模块过滤 | 不聚合 |
| 模块曝光 UV | `module_exposure_uv` | 已登录用户 | 曝光事件的 `COUNT(DISTINCT prop_user_id)` | `prop_user_id IS NOT NULL AND prop_user_id != ''` | 不可跨天或跨模块求和 |
| 模块点击 UV | `module_click_uv` | 已登录用户 | 点击事件的 `COUNT(DISTINCT prop_user_id)` | 同上 | 不可跨天或跨模块求和 |
| 点击率 | `click_rate` | 已登录用户 | `module_click_uv / module_exposure_uv`；分母为 0 时返回 `0` | 同上 | 不可 `AVG`；跨天、跨模块须用汇总分子分母重算 |
| 模块点击 PV | `click_pv` | 事件 | 点击事件按 `raw_event_id` 去重 | 同上 | 可跨天、跨模块求和 |
| 人均点击次数 | `avg_click_count_per_user` | 已登录用户 | `click_pv / module_click_uv`；分母为 0 时返回 `0` | 同上 | 不可 `AVG`；跨天、跨模块须用汇总分子分母重算 |

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
