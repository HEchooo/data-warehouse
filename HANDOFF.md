# Handoff

## 1. Project Overview
- **Project**: `/Users/sinn/Documents/guyin/data-warehouse`
- **Tech Stack**: Python, BigQuery, Superset, JSON 配置驱动 ETL
- **Brief Description**: 数据工程仓库，负责 ODS/DWD/DIM/DWS/ADS 分层 ETL 与 Superset 报表供数

## 2. Current Session Goals
- 从 `WAREHOUSE_LAYERING_REFACTOR_PLAN.md` 继续，完成 `Batch 5`
- 目标主题：试穿与商品
- 本次已完成代码重构、生产建表、DWS 初始化、ADS 并跑与正式切换
- **显式下一目标**：下个 session 优先复盘 `ads_daily_post_tryon_confirm` 的历史差异，确认 `diff_count = 586` 的根因

## 3. What Was Done
- 新增 4 张 DWS DDL
  - `ddl/dws/dws_tryon_confirm_daily.sql`
  - `ddl/dws/dws_tryon_confirm_post_daily.sql`
  - `ddl/dws/dws_tryon_user_daily.sql`
  - `ddl/dws/dws_product_behavior_daily.sql`
- 新增 4 张 DWS 脚本
  - `jobs/dws/dws_tryon_confirm_daily.py`
  - `jobs/dws/dws_tryon_confirm_post_daily.py`
  - `jobs/dws/dws_tryon_user_daily.py`
  - `jobs/dws/dws_product_behavior_daily.py`
- 重写 4 张 ADS 脚本
  - `jobs/ads/ads_daily_tryon_confirm.py`
  - `jobs/ads/ads_daily_post_tryon_confirm.py`
  - `jobs/ads/ads_daily_tryon_add_cart_conversion.py`
  - `jobs/ads/ads_daily_product_tryon_performance.py`
- 调整共享入口
  - `jobs/ads/ads_daily.py` 已移除 Batch 5 四张 ADS 的共享入口调用
- 更新调度元数据
  - `env/etl_config.json` 新增 8 个独立任务：
    - `dws_tryon_confirm_daily`
    - `dws_tryon_confirm_post_daily`
    - `dws_tryon_user_daily`
    - `dws_product_behavior_daily`
    - `ads_daily_tryon_confirm`
    - `ads_daily_post_tryon_confirm`
    - `ads_daily_tryon_add_cart_conversion`
    - `ads_daily_product_tryon_performance`
- 更新文档
  - `docs/rules/pipeline.md`
  - `docs/topics/穿搭工具点击使用/fact.md`
  - `docs/topics/穿搭工具点击使用/patterns.md`
  - `docs/topics/试穿结果转化/fact.md`
  - `docs/topics/试穿结果转化/patterns.md`
  - `docs/topics/商详页试穿维度/fact.md`
  - `docs/topics/商详页试穿维度/patterns.md`
- 完成生产落地
  - 正式创建并初始化 4 张 DWS
  - 4 张 ADS 已按 `staging -> changed -> swap` 正式切换
  - 临时 `tmp/changed` 表已清理

## 4. Key Decisions & Rationale
- Batch 5 的目标不是“把 SQL 搬家”，而是把重复扫描 `dwd_event_log` 的主题事实沉到可复用 DWS
- 四张 ADS 现在都只做终层展示，不再直接拼主题事实
  - `ads_daily_tryon_confirm` -> `dws_tryon_confirm_daily`
  - `ads_daily_post_tryon_confirm` -> `dws_tryon_confirm_post_daily + dim_post`
  - `ads_daily_tryon_add_cart_conversion` -> `dws_tryon_confirm_daily + dws_tryon_user_daily`
  - `ads_daily_product_tryon_performance` -> `dws_product_behavior_daily`
- `jobs/ads/ads_daily.py` 不再继续承接 Batch 5 的四张 ADS
  - 这是有意冻结旧入口，不是漏接
- 生产执行时没有直接依赖本机 ADC
  - 本机 Python BigQuery Client 的 ADC 刷新失败：`invalid_grant`
  - 实际通过去掉代理后的 `bq` CLI 完成 BigQuery 执行

## 5. Current State
- **Working / Stable**
  - Batch 5 四张 DWS 已正式落地
  - Batch 5 四张 ADS 已正式切到新链路
  - `ads_daily_tryon_confirm`、`ads_daily_tryon_add_cart_conversion`、`ads_daily_product_tryon_performance` 的差异模式基本符合“当天新鲜度差异”
- **In Progress**
  - `ads_daily_post_tryon_confirm` 的历史差异还未解释完
- **Broken / Failing**
  - 本机 Python 直接访问 BigQuery 仍会报 ADC 刷新错误：
    - `google.auth.exceptions.RefreshError: ('invalid_grant: Bad Request', {'error': 'invalid_grant', 'error_description': 'Bad Request'})`
  - 本机环境默认带失效代理：
    - `HTTP_PROXY=127.0.0.1:7890`
    - `HTTPS_PROXY=127.0.0.1:7890`

## 6. Unfinished Work & Next Steps
1. 先专项复盘 `ads_daily_post_tryon_confirm`
2. 重点查 `2026-02-27` 的差异样本，确认是旧生产脏数据、`dim_post` 映射变化，还是新链路去重/补维逻辑变化
3. 如有必要，再决定是否补一轮专项对账 SQL 或回刷策略说明
4. 评估是否需要修复本机 ADC，避免后续每次 BigQuery 落地都绕过 Python Client

## 7. Blockers & Open Questions
- `ads_daily_post_tryon_confirm` 并跑差异较大
  - `diff_count = 586`
  - 当前没有结论说明这些差异是“旧错新对”还是“新逻辑引入偏差”
- `dws_tryon_confirm_post_daily` 到 `dim_post` 的映射存在自然损耗
  - `dws_row_count = 7588`
  - `unmapped_rows = 81`
  - 最终 `ads_daily_post_tryon_confirm` 行数 `7507`

## 8. Important Context & Gotchas
- 不要把 `ads_daily_post_tryon_confirm` 的大 diff 和其他三张 ADS 的“当天新鲜度差异”混为一谈
  - 其他三张表的 diff 主要集中在 `2026-04-27`
  - `post_tryon` 是一批历史差异，不是单日波动
- `ads_daily_tryon_confirm` 与 `ads_daily_tryon_add_cart_conversion` 仍保留旧生产的 `2026-01-09 ~ 2026-02-07` 行
  - 因为新 DWS 从 `2026-02-08/09` 开始，安全换表时只替换新链路覆盖范围
- `ads_daily_post_tryon_confirm` 现在通过 `dim_post` 补齐 `post_id/post_name/creator`
  - 以后排查帖子维度问题时先查 `dim_post`，不要直接回头查 `community_post`
- 本次生产执行中曾临时写过 `.tmp_batch5_prod_run.py`
  - 已删除，不应提交

## 9. Environment & Configuration Notes
- 分支：`main`
- 最近提交：
  - `1697b42 refactor: complete batch4 platform and home layering`
  - `54293a1 refactor: complete batch3 column theme layering`
- 本机 BigQuery 可用方式
  - `bq` CLI 在去掉代理后可用
  - 示例：
    - `env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u http_proxy -u https_proxy -u all_proxy '/Users/sinn/Downloads/google-cloud-sdk 2/bin/bq' query ...`
- 本机 Python 环境
  - `/Users/sinn/Documents/work_py_venv/bin/python3.11`

## 10. Key Files & Locations
- 计划文档：`WAREHOUSE_LAYERING_REFACTOR_PLAN.md`
- 调度配置：`env/etl_config.json`
- 共享 ADS 入口：`jobs/ads/ads_daily.py`
- Batch 5 DWS：
  - `jobs/dws/dws_tryon_confirm_daily.py`
  - `jobs/dws/dws_tryon_confirm_post_daily.py`
  - `jobs/dws/dws_tryon_user_daily.py`
  - `jobs/dws/dws_product_behavior_daily.py`
- Batch 5 ADS：
  - `jobs/ads/ads_daily_tryon_confirm.py`
  - `jobs/ads/ads_daily_post_tryon_confirm.py`
  - `jobs/ads/ads_daily_tryon_add_cart_conversion.py`
  - `jobs/ads/ads_daily_product_tryon_performance.py`
- 主题文档：
  - `docs/topics/穿搭工具点击使用/fact.md`
  - `docs/topics/穿搭工具点击使用/patterns.md`
  - `docs/topics/试穿结果转化/fact.md`
  - `docs/topics/试穿结果转化/patterns.md`
  - `docs/topics/商详页试穿维度/fact.md`
  - `docs/topics/商详页试穿维度/patterns.md`

## 11. Quick-Start Summary
本 session 完成了 `WAREHOUSE_LAYERING_REFACTOR_PLAN.md` 的 `Batch 5`，把试穿与商品主题从轻量 ADS 直接扫事件的实现，重构成“DWS 承接主题事实、ADS 只保留终层展示”的结构。新增了四张 DWS：`dws_tryon_confirm_daily`、`dws_tryon_confirm_post_daily`、`dws_tryon_user_daily`、`dws_product_behavior_daily`；同时重写了四张 ADS：`ads_daily_tryon_confirm`、`ads_daily_post_tryon_confirm`、`ads_daily_tryon_add_cart_conversion`、`ads_daily_product_tryon_performance`，并把它们从 `jobs/ads/ads_daily.py` 的共享入口里摘掉，改为独立任务注册到 `env/etl_config.json`。生产侧已经正式建表并切换：DWS 状态分别是 `79/7588/5428/20845` 行，ADS 状态分别是 `109/7507/109/20845` 行，最大日期都到 `2026-04-27`。三张表的并跑差异基本符合当天新鲜度差异模式：`ads_daily_tryon_confirm diff_count=1`、`ads_daily_tryon_add_cart_conversion diff_count=1`、`ads_daily_product_tryon_performance diff_count=114` 且抽样都在 `2026-04-27`；但 `ads_daily_post_tryon_confirm diff_count=586`，属于需要下个 session 单独解释的历史差异。还要记住：本机 Python BigQuery Client 的 ADC 当前坏了，直接跑会报 `invalid_grant`，而默认代理 `127.0.0.1:7890` 也失效；本次 BigQuery 执行实际依赖去掉代理后的 `bq` CLI。
