# Handoff

## 1. Project Overview
- **Project**: `/Users/sinn/Documents/guyin/data-warehouse`
- **Tech Stack**: Python, BigQuery, Superset, JSON 配置驱动 ETL
- **Brief Description**: 数据工程仓库，负责 ODS/DWD/DIM/DWS/ADS ETL 和最终 Superset 看板数据产出

## 2. Current Session Goals
- 原始目标：开始重构 `WAREHOUSE_LAYERING_REFACTOR_PLAN.md` 中 Batch 2
- 中途澄清后的目标：先只落 Batch 2 的第一个子项，即新增 `dws_post_interaction_daily`
- 后续补充目标：
  - 不再把新 DWS 任务挂到 `jobs/dws/dws_daily.py`
  - 新任务要能被 `etl_run.py` 直接消费
  - 把“独立任务注册方式”和“增量更新策略”明确写进重构计划和增量规则文档
- 明确下一目标：继续完成 Batch 2，重写 `ads_daily_post_performance`，让其消费 `dim_post`、`dim_column`、`dws_post_interaction_daily`

## 3. What Was Done
- 新增 `ddl/dws/dws_post_interaction_daily.sql`
  - 定义帖子主题核心事实层 DDL，粒度为 `dt × post_code × module × column_id × platform`
- 新增 `jobs/dws/dws_post_interaction_daily.py`
  - 实现帖子曝光 / 阅读 / 点赞 / 关注归因 / 试穿核心事实
  - 增加独立日志入口
  - 增加 `__main__`，可被 `etl_run.py` 直接消费
  - 增加 `get_post_interaction_dates_to_process()`，按最近 `6` 小时多上游变更推导受影响 `dt`
- 更新 `env/etl_config.json`
  - 新增 `dws_post_interaction_daily` 独立任务占位
  - 当前已设为 `enabled: true`
- 更新 `docs/rules/window-incremental.md`
  - 明确禁止扩共享日期入口
  - 增加“分区重算型任务”规则
  - 明确多源任务可按单任务上游推 `dates`，不能共享整层 `dt`
- 更新 `WAREHOUSE_LAYERING_REFACTOR_PLAN.md`
  - 明确不再给共享旧入口挂新表
  - Batch 0 明确后续每批都要写“独立任务注册方式”和“增量更新策略”
  - Batch 1~8 的 `本批具体改法` 已补这两类约束
- 更新 `docs/topics/内容维度日报/fact.md`
  - 记录 `dws_post_interaction_daily` 已落地，但 `ads_daily_post_performance` 尚未切换消费

## 4. Key Decisions & Rationale
- `dws_post_interaction_daily` 不挂 `jobs/dws/dws_daily.py`
  - 用户明确要求后续废弃共享旧入口
  - 该表依赖事件表、服务端事实表、维表修正，不能复用整层共享 `dt`
- `dws_post_interaction_daily` 做成独立脚本并注册到 `env/etl_config.json`
  - 这样 `etl_run.py` 可直接执行该任务
  - 后续 DWS 拆分时可平移同样模式
- 增量策略采用“最近 6 小时多源变更映射受影响 `dt`”
  - 一开始误写成最近 7 天固定重刷，已被用户指出并改回规则默认值 `6` 小时
  - 当前不是全历史强一致方案；维表修正影响更早历史分区时仍需单独回填
- `env/etl_config.json` 中新任务已启用
  - 这是用户在最后明确要求的状态

## 5. Current State
- **Working / Stable**
  - `jobs/dws/dws_post_interaction_daily.py` 已可独立运行
  - `env/etl_config.json` 已存在独立任务配置
  - `py_compile` 已通过
  - `env/etl_config.json` JSON 校验已通过
- **In Progress**
  - Batch 2 只完成了第一个子项
  - `ads_daily_post_performance` 还未改成消费 DWS 中间层
- **Broken / Failing**
  - 无已知语法错误
  - 未实际跑 BigQuery 任务，因此尚未验证线上数据结果

## 6. Unfinished Work & Next Steps
1. 重写 `jobs/ads/ads_daily_post_performance.py`
2. 让 `ads_daily_post_performance` 只消费 `dim_post`、`dim_column`、`dws_post_interaction_daily`
3. 补对账 SQL 或对账方法，验证新旧 `ads_daily_post_performance`
4. 视情况更新 `ddl/ads/ads_daily_post_performance.sql` 或相关主题文档
5. 后续继续按同样模式拆 `dws_daily.py` 中其他存量任务

## 7. Blockers & Open Questions
- `dws_post_interaction_daily` 现在已在 `etl_config.json` 中启用，但本 session 未实际执行任务
- 多源增量只覆盖最近 `6` 小时内可见变更；若需要历史映射修正回刷，仍要单独确认范围
- `etl_run.py` 当前虽然保留了 `depends_on` / `schedule_group` 字段，但尚未真正消费这些字段做拓扑编排

## 8. Important Context & Gotchas
- 用户在多个 session 中反复强调：
  - 不要继续扩 `jobs/dws/dws_daily.py`
  - 每个新任务都应在 `env/etl_config.json` 有独立占位
  - 每个 Batch 都必须显式写明增量更新策略
- `dws_post_interaction_daily` 当前是分区重算语义：
  - 先找受影响 `dt`
  - 再对这些 `dt` 做 `DELETE + INSERT`
- 维表修正逻辑不是直接扫全历史：
  - 先找最近 `6` 小时变更的 `post_code / column_id`
  - 再回推目标表中已有这些业务键的历史 `dt`
- 本 session 已经把之前错误接入 `dws_daily.py` 的代码撤回；当前 `dws_daily.py` 没有本次遗留 diff

## 9. Environment & Configuration Notes
- 当前分支：`main`
- Python 环境：`/Users/sinn/Documents/work_py_venv/bin/python3.11`
- 本地运行入口：`etl_run.py`
- 关键配置文件：`env/etl_config.json`
- 新任务当前状态：`dws_post_interaction_daily.enabled = true`

## 10. Key Files & Locations
- `jobs/dws/dws_post_interaction_daily.py`
- `ddl/dws/dws_post_interaction_daily.sql`
- `env/etl_config.json`
- `docs/rules/window-incremental.md`
- `WAREHOUSE_LAYERING_REFACTOR_PLAN.md`
- `docs/topics/内容维度日报/fact.md`
- 下一步重点文件：`jobs/ads/ads_daily_post_performance.py`

## 11. Quick-Start Summary
本 session 主要完成了 Batch 2 的第一个子项：新增帖子主题核心事实层 `dws_post_interaction_daily`。实现上已经新增了 DDL 和独立脚本，其中脚本具备日志入口、`__main__`、以及自己的增量 `dt` 推导逻辑，不再依赖 `jobs/dws/dws_daily.py` 这种共享旧入口。用户明确要求以后所有新任务都要在 `env/etl_config.json` 有独立占位，并且每个 Batch 都必须显式写明增量更新策略，所以本次已经同步修改了 `WAREHOUSE_LAYERING_REFACTOR_PLAN.md` 和 `docs/rules/window-incremental.md`。最初曾把 `dws_post_interaction_daily` 写成“最近 7 天固定重刷”，但这与仓库默认 `6` 小时窗口规则不符，用户指出后已经改回：现在默认只看最近 `6` 小时内的多源变更，再映射出受影响的 `dt` 分区重算。`env/etl_config.json` 中已新增 `dws_post_interaction_daily` 独立任务，且当前已设为 `enabled: true`。这次没有实际运行 BigQuery 任务，所以代码层面是就绪的，数据结果层面尚未验证。下个 session 的直接目标是重写 `jobs/ads/ads_daily_post_performance.py`，让它只消费 `dim_post`、`dim_column`、`dws_post_interaction_daily`，然后补新旧结果对账。
