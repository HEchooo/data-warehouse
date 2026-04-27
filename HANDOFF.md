# Handoff

## 1. Project Overview
- **Project**: `/Users/sinn/Documents/guyin/data-warehouse`
- **Tech Stack**: Python, BigQuery, Superset, JSON 配置驱动 ETL
- **Brief Description**: 数据仓库与 BI 供数项目，负责 ODS/DWD/DIM/DWS/ADS 分层 ETL 以及 Superset 报表数据来源

## 2. Current Session Goals
- 从 `HANDOFF.md` 续接，继续执行 `WAREHOUSE_LAYERING_REFACTOR_PLAN.md` 的 `Batch 7`
- 完成 `视频与投资人` 主题的代码重构
- 做生产侧建表、初始化、正式 ADS 切换前验证
- 按用户确认，仅重算 `2026-04-01 ~ 2026-04-27` 的正式 ADS 分区，不回刷更早历史
- **显式下一目标**：
  1. 若继续按计划推进，进入 `Batch 8：收尾与收口`
  2. 若先做生产善后，评估是否还要补 `2026-04-01` 之前的历史切换与 Superset 侧联动

## 3. What Was Done
- 新增 1 张 DIM DDL
  - `ddl/dim/dim_auto_publish_video_task.sql`
- 新增 3 张 DWS DDL
  - `ddl/dws/dws_registered_app_device_daily.sql`
  - `ddl/dws/dws_registered_app_user_daily.sql`
  - `ddl/dws/dws_social_channel_daily.sql`
- 新增 1 张 DIM 脚本
  - `jobs/dim/dim_auto_publish_video_task.py`
- 新增 3 张 DWS 脚本
  - `jobs/dws/dws_registered_app_device_daily.py`
  - `jobs/dws/dws_registered_app_user_daily.py`
  - `jobs/dws/dws_social_channel_daily.py`
- 重写 2 张 ADS 脚本
  - `jobs/ads/ads_daily_investor.py`
  - `jobs/ads/ads_daily_social_content.py`
- 调整共享入口
  - `jobs/ads/ads_daily.py` 已移除 `ads_daily_investor` 的共享入口调用
- 更新调度元数据
  - `env/etl_config.json` 新增 4 个独立任务：
    - `dim_auto_publish_video_task`
    - `dws_registered_app_device_daily`
    - `dws_registered_app_user_daily`
    - `dws_social_channel_daily`
- 更新文档
  - `docs/rules/pipeline.md`
  - `docs/topics/投资人报表【核心业务报表】/fact.md`
  - `docs/topics/投资人报表【核心业务报表】/patterns.md`
  - `docs/topics/【Social Content】社媒表现数据看板/fact.md`
  - `docs/topics/【Social Content】社媒表现数据看板/patterns.md`
- 完成生产落地
  - 正式创建 4 张新中间表
  - 正式初始化 4 张新中间表
  - 正式重算 `ads_daily_investor` 与 `ads_daily_social_content` 的 `2026-04-01 ~ 2026-04-27` 分区
- 完成校验
  - `ads_daily_investor` 在 `2026-04-01 ~ 2026-04-27` 上新旧对账 `diff_count = 0`
  - `ads_daily_social_content` 在 `2026-04-01 ~ 2026-04-27` 上新旧对账 `diff_count = 0`
  - 4 张新中间表与按当前源数据即时重算的结果逐行一致
- 清理临时产物
  - 删除 `jobs/ads/__pycache__`
  - 删除 `jobs/dim/__pycache__`
  - 删除 `jobs/dws/__pycache__`
  - 删除临时对账脚本 `scripts/compare_batch7_ads.py`

## 4. Key Decisions & Rationale
- Batch 7 的目标不是在 ADS 内继续拼跨平台视频语义，而是把公共语义下沉到 `1 DIM + 3 DWS`
  - `dim_auto_publish_video_task`：统一三端自动发布视频与任务映射
  - `dws_registered_app_device_daily`：设备级 app 活跃 / 注册活动事实
  - `dws_registered_app_user_daily`：注册用户级活跃 / 留存 / 商详曝光事实
  - `dws_social_channel_daily`：社媒渠道级日事实
- `ads_daily_investor` 现在只保留终层展示逻辑，不再直接从上传表和事件表拼跨层语义
- `ads_daily_social_content` 现在只消费 `dws_social_channel_daily`
- `jobs/ads/ads_daily.py` 不再承接 `ads_daily_investor`
  - 这是有意拆出独立入口，不是漏接
- 用户明确要求临时执行态内容不要写进 `fact.md`
  - 因此运行清单、切换清单、对账执行细节都只保留在会话和交接里，不落主题事实文档
- 正式 ADS 只重算 `2026-04-01 ~ 2026-04-27`
  - 这是用户明确确认的生产边界，`2026-04-01` 之前历史保持不动

## 5. Current State
- **Working / Stable**
  - Batch 7 代码重构已完成
  - 4 张新中间表已正式建表并初始化
  - `ads_daily_investor` 已正式切到新链路并重算 `2026-04-01 ~ 2026-04-27`
  - `ads_daily_social_content` 已正式切到新链路并重算 `2026-04-01 ~ 2026-04-27`
  - 上述正式 ADS 分区与当前代码即时重算结果逐行一致
- **In Progress**
  - 无进行中的 Batch 7 阻塞项
- **Broken / Failing**
  - 本机 `bq` CLI 仍可能受代理环境影响报错，不能把它当成稳定执行路径

## 6. Unfinished Work & Next Steps
1. 若按主计划继续，开始 `Batch 8：收尾与收口`，目标对象是 `ads_daily_new`
2. 若要补全历史切换，再单独评估是否回刷 `2026-04-01` 之前的 `ads_daily_investor` / `ads_daily_social_content`
3. 若后续涉及 Superset 看板口径确认，复核 dataset 是否只读取已切换的日期范围，避免把“新链路已切换区间”和“旧链路未回刷历史”混在一起解释

## 7. Blockers & Open Questions
- 是否需要继续回刷 `2026-04-01` 之前的历史
  - 当前用户决定：**不回刷**
  - 但如果业务后续希望全历史口径统一，仍要单独再做生产确认
- 是否需要同步做 Superset 侧说明或变更
  - 本次没有改任何线上 Superset 对象

## 8. Important Context & Gotchas
- `bq query` 在本机直接跑 DDL 时，曾因代理环境报 BigQuery CLI bug
  - 报错核心是代理连接失败，不是 SQL 本身有问题
  - 本次稳定执行路径是：
    - 显式指定 `GOOGLE_APPLICATION_CREDENTIALS`
    - 使用本地 Python + `google.cloud.bigquery.Client`
- `ads_daily_investor` 在第一轮全历史对账时发现过真实回归
  - 问题是 `next_day_retention_rate` / `day_7_retention_rate` 的成熟判断早了一天
  - 已在 `jobs/ads/ads_daily_investor.py` 修正
  - 修正后全历史对账和本次生产切换前对账都为 `diff_count = 0`
- 主题文档里不要再写“当前状态 / 切换清单 / 执行命令”这类临时内容
  - 用户明确否定过这种写法
- `docs/topics/投资人报表【核心业务报表】/fact.md` 当前保留了一句稳定提示：
  - 历史日期是否切到 Batch 7 新链路，仍需单独确认回填与切换方案
  - 这句现在依然成立，因为本次只切了 `2026-04-01 ~ 2026-04-27`

## 9. Environment & Configuration Notes
- 分支：`main`
- 运行 Python：
  - `/Users/sinn/Documents/work_py_venv/bin/python3.11`
- 本次 BigQuery 稳定执行方式：
  - `GOOGLE_APPLICATION_CREDENTIALS=/Users/sinn/Documents/guyin/data-warehouse/jobs/ods/ods_android_download/my-project-8584-jetonai-2bafc5bf74da.json`
  - 之后直接运行本地 Python 脚本
- `bq` CLI 不建议作为本机默认方案
  - 代理环境下不稳定

## 10. Key Files & Locations
- 计划文档：`WAREHOUSE_LAYERING_REFACTOR_PLAN.md`
- 调度配置：`env/etl_config.json`
- 共享 ADS 入口：`jobs/ads/ads_daily.py`
- Batch 7 DIM：
  - `ddl/dim/dim_auto_publish_video_task.sql`
  - `jobs/dim/dim_auto_publish_video_task.py`
- Batch 7 DWS：
  - `ddl/dws/dws_registered_app_device_daily.sql`
  - `ddl/dws/dws_registered_app_user_daily.sql`
  - `ddl/dws/dws_social_channel_daily.sql`
  - `jobs/dws/dws_registered_app_device_daily.py`
  - `jobs/dws/dws_registered_app_user_daily.py`
  - `jobs/dws/dws_social_channel_daily.py`
- Batch 7 ADS：
  - `jobs/ads/ads_daily_investor.py`
  - `jobs/ads/ads_daily_social_content.py`
- 文档：
  - `docs/rules/pipeline.md`
  - `docs/topics/投资人报表【核心业务报表】/fact.md`
  - `docs/topics/投资人报表【核心业务报表】/patterns.md`
  - `docs/topics/【Social Content】社媒表现数据看板/fact.md`
  - `docs/topics/【Social Content】社媒表现数据看板/patterns.md`

## 11. Quick-Start Summary
本 session 完成了 `WAREHOUSE_LAYERING_REFACTOR_PLAN.md` 的 `Batch 7`，把“视频与投资人”相关的公共语义从 ADS 下沉成 `1 DIM + 3 DWS`，然后把两张正式 ADS 在用户确认范围内切到了新链路。新增对象包括 `dim_auto_publish_video_task`、`dws_registered_app_device_daily`、`dws_registered_app_user_daily`、`dws_social_channel_daily`，对应 DDL 和 Python 任务都已落库并注册到 `env/etl_config.json`。`ads_daily_investor` 与 `ads_daily_social_content` 都已改成只消费新中间层；`jobs/ads/ads_daily.py` 里也移除了 `ads_daily_investor` 的共享入口。生产侧已经完成 4 张新表建表与初始化，其中 `dim_auto_publish_video_task` 当前 `2927` 行；`dws_registered_app_device_daily` 覆盖 `2026-02-09 ~ 2026-04-27` 共 `11976` 行；`dws_registered_app_user_daily` 覆盖 `2026-02-09 ~ 2026-04-27` 共 `8814` 行；`dws_social_channel_daily` 覆盖 `2023-07-14 ~ 2026-04-27` 共 `4076` 行。正式 ADS 只重算了 `2026-04-01 ~ 2026-04-27`：`ads_daily_investor` 为 `27` 行，`ads_daily_social_content` 为 `108` 行。切换前后都做了校验：两张 ADS 在该区间的新旧逻辑对账 `diff_count = 0`，而且 4 张新中间表以及两张正式 ADS 都与“按当前源数据即时重算”的结果逐行一致。未做的事情只有两类：`2026-04-01` 之前历史未回刷，以及 Superset 线上对象未改。若继续主计划，下一步就是 `Batch 8：收尾与收口`，目标对象是 `ads_daily_new`。
