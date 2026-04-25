# Handoff

## 1. Project Overview
- **Project**: `/Users/sinn/Documents/guyin/data-warehouse`
- **Tech Stack**: Python, BigQuery, Superset, JSON 配置驱动 ETL
- **Current Focus**: `WAREHOUSE_LAYERING_REFACTOR_PLAN.md` 的 `Batch 4` 已完成，平台内容与首页模块主题已切到独立 DWS

## 2. What This Session Finished
- 完成 `Batch 4` 的代码改造
  - 新增 [`jobs/dws/dws_platform_content_daily.py`](./jobs/dws/dws_platform_content_daily.py)
  - 新增 [`ddl/dws/dws_platform_content_daily.sql`](./ddl/dws/dws_platform_content_daily.sql)
  - 新增 [`jobs/dws/dws_home_module_daily.py`](./jobs/dws/dws_home_module_daily.py)
  - 新增 [`ddl/dws/dws_home_module_daily.sql`](./ddl/dws/dws_home_module_daily.sql)
  - 重写 [`jobs/ads/ads_daily_content_performance.py`](./jobs/ads/ads_daily_content_performance.py)
  - 重写 [`jobs/ads/ads_daily_home_module_performance.py`](./jobs/ads/ads_daily_home_module_performance.py)
- 调整编排
  - [`jobs/ads/ads_daily.py`](./jobs/ads/ads_daily.py) 已去掉 `ads_daily_content_performance` / `ads_daily_home_module_performance` 的共享入口调用
  - [`env/etl_config.json`](./env/etl_config.json) 已注册 4 个独立任务：
    - `dws_platform_content_daily`
    - `dws_home_module_daily`
    - `ads_daily_content_performance`
    - `ads_daily_home_module_performance`
- 更新文档
  - [`docs/rules/pipeline.md`](./docs/rules/pipeline.md)
  - [`docs/topics/平台整体内容表现/fact.md`](./docs/topics/平台整体内容表现/fact.md)
  - [`docs/topics/平台整体内容表现/patterns.md`](./docs/topics/平台整体内容表现/patterns.md)
  - [`docs/topics/首页模块曝光与点击明细/fact.md`](./docs/topics/首页模块曝光与点击明细/fact.md)
  - [`docs/topics/首页模块曝光与点击明细/patterns.md`](./docs/topics/首页模块曝光与点击明细/patterns.md)
- 完成 BigQuery 生产落地
  - 正式创建 `decom.dws_platform_content_daily`
  - 正式创建 `decom.dws_home_module_daily`
  - 全量初始化两张 DWS
  - 按 `staging -> changed -> swap` 安全流程切换：
    - `decom.ads_daily_content_performance`
    - `decom.ads_daily_home_module_performance`
  - 本次换表使用 run id：`20260425_163609`
  - 临时表已清理完成

## 3. Production Result
- DWS 正式表状态
  - `decom.dws_platform_content_daily`: `330` 行，`2025-12-29 ~ 2026-04-25`
  - `decom.dws_home_module_daily`: `308` 行，`2026-02-08 ~ 2026-04-25`
- ADS 正式表状态
  - `decom.ads_daily_content_performance`: `301` 行，`2026-02-01 ~ 2026-04-25`
  - `decom.ads_daily_home_module_performance`: `428` 行，`2026-01-09 ~ 2026-04-25`
- 特殊保留范围
  - `ads_daily_home_module_performance` 的 `2026-01-09 ~ 2026-02-07` 旧分区这次没有重算，直接保留旧生产行
  - 原因：新 `dws_home_module_daily` 的正式数据从 `2026-02-08` 开始，本次安全换表只替换 `2026-02-08 ~ 2026-04-25`

## 4. Reconciliation Summary
- `ads_daily_home_module_performance`
  - 替换范围 `2026-02-08 ~ 2026-04-25` 全量 `0 diff`
  - 校验项：`dt × module` 粒度行数、`module_exposure_uv`、`module_click_uv`、`click_pv`、`click_rate`
- `ads_daily_content_performance`
  - 截至 `2026-04-24` 全量 `0 diff`
  - 唯一差异在 `2026-04-25 / unknown`
    - 旧生产：`tryon_total_count = 3`，`tryon_uv = 2`
    - 最终新正式表：`tryon_total_count = 12`，`tryon_uv = 4`
    - `update_time = 2026-04-25 08:44:08 UTC`
  - 已确认这不是新口径错误，而是旧生产表刷新后，`v3_decom.common_ext_ai_task` 的当日 `TRY_ON / OUTFIT` 任务继续写入导致的新鲜度差异
  - 在正式 swap 前，已单独重刷 `dws_platform_content_daily` 的 `2026-04-25` 分区并重建 content staging

## 5. Key Decisions
- `ads_daily_content_performance` 现在只消费 `dws_platform_content_daily`
  - 平台曝光 / 内容曝光 / 商详曝光 / 商品点击 / 点赞 / 关注 / 试穿等基础事实全部下沉到 DWS
  - ADS 只保留终层展示率指标
- `ads_daily_home_module_performance` 现在只消费 `dws_home_module_daily`
  - 固定模块映射与补零骨架都沉到 DWS
  - ADS 只保留 `click_rate` 与 `avg_click_count_per_user`
- `jobs/ads/ads_daily.py` 现在不再触发这两张表
  - 这是有意设计，不是漏接
  - 后续应走 `env/etl_config.json` 中新增的独立任务

## 6. Validation Done
- 本地语法校验已通过
  - `python3 -m py_compile jobs/dws/dws_platform_content_daily.py`
  - `python3 -m py_compile jobs/dws/dws_home_module_daily.py`
  - `python3 -m py_compile jobs/ads/ads_daily_content_performance.py`
  - `python3 -m py_compile jobs/ads/ads_daily_home_module_performance.py`
  - `python3 -m py_compile jobs/ads/ads_daily.py`
- BigQuery 临时表已清理
  - `ads_daily_content_performance_tmp_20260425_163609`
  - `ads_daily_content_performance_changed_20260425_163609`
  - `ads_daily_home_module_performance_tmp_20260425_163609`
  - `ads_daily_home_module_performance_changed_20260425_163609`

## 7. Important Gotchas
- `ads_daily_content_performance` 对账不能直接把“今天”的 diff 当成口径问题
  - 要先验证“截至昨天”是否 `0 diff`
  - 再确认“今天”的差异是不是源表继续进数导致的新鲜度差异
- `ads_daily.py` 里已经切掉 `content/home_module`
  - 如果下个 session 只跑 `jobs/ads/ads_daily.py`，这两张表不会刷新
- `ads_daily_content_performance.py` 里仍保留了 `dates_to_sql_list / event_ts_expr / service_ts_expr / platform_expr`
  - 这是为了兼容当前仍在 import 这些函数的其他 ADS 脚本
  - 不是最终形态，后续如果做 `Batch 5+`，再统一抽公共模块

## 8. Current Workspace State
- 分支：`main`
- 代码和文档改动仍未 commit
- 本 `HANDOFF.md` 也是未提交状态

## 9. Next Recommended Step
1. 先提交本次 `Batch 4` 代码、文档和新的 `HANDOFF.md`
2. 然后直接进入 `Batch 5`
3. 如果不进 `Batch 5`，至少要记住：
   - `content/home_module` 已经正式切到独立 DWS
   - 后续任何问题先查：
     - `dws_platform_content_daily`
     - `dws_home_module_daily`
     - 独立 ADS 任务注册是否还在 `env/etl_config.json`
