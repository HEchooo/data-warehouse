# 数据仓库项目交接文档

> 交接人：Sinn ｜ 最后更新：2026-10-10
> 本文只说明项目如何运作、在哪里维护。具体脚本实现看代码，业务口径看主题文档，不在这里重复盘点。

## 1. 项目是做什么的

本项目把 Alvin's Club App、Makepod 独立站、ECN、TikTok 等业务的数据汇入 BigQuery，加工成报表，最终通过 Superset 看板和飞书日 / 周报提供给业务方。

技术栈是 **Python + BigQuery + Superset**。生产机用 **crontab + `etl_run.py`** 调度 ETL，没有 Airflow。

```text
App / Web 埋点 → 服务端接收 → GCS → Eventarc → Cloud Run ─┐
服务端业务数据库 ─────────────────────────→ Datastream ──┤
外部平台 API / 报表文件 ─────────────────→ Python ODS ───┤
                                                       ↓
                                               BigQuery 源数据
                                                       │
                                         生产机 cron + etl_run.py
                                                       ↓
                                           DWD / DIM → DWS → ADS
                                                       ↓
                                              Superset / 飞书报表
```

先分清三个环节：**源数据同步、ETL 加工、报表展示**。源数据没进来，重跑下游不能补上；源数据已入库，也不代表报表已刷新。

## 2. 数据从哪里来

### 2.1 App / Web 埋点：GCS → Cloud Run

App / Web 上报埋点后，服务端把日志写入 GCS。对象写入事件经 Eventarc 触发 Cloud Run，由 Python 解析日志并写入 BigQuery，供下游清洗和汇总。

- 日志桶：`decom-prod-client-log`、`mall-prod-client-log`。
- 管理入口：[Eventarc 触发器](https://console.cloud.google.com/eventarc/triggers?project=my-project-8584-jetonai)、Cloud Run 的 [decom-track-log-runner](https://console.cloud.google.com/run/detail/us-east4/decom-track-log-runner/observability/logs?project=my-project-8584-jetonai) / [mall-track-log-runner](https://console.cloud.google.com/run/detail/us-east4/mall-track-log-runner/observability/logs?project=my-project-8584-jetonai)。
- 仓库代码：[jobs/ods/ods_event_log/](./jobs/ods/ods_event_log/)。当前 `main.py` 写入 `decom.ods_event_log`，并记录已处理文件，供下游 DWD 判断处理范围。

**这条链路由事件触发，不由生产机 cron 调度。** 按现有交接记录，代码在 Cloud Console 中编辑并部署；本地提交或生产机 `git pull` 不会更新 Cloud Run。实际部署代码及触发器绑定以控制台为准，旧 README 的表名和服务名不能代替部署配置。

### 2.2 服务端业务表：Datastream → BigQuery

服务端数据库通过 [Datastream](https://console.cloud.google.com/datastream/streams?project=my-project-8584-jetonai) 同步到 BigQuery，下游读取其中的用户、订单、社区、视频等业务数据。常用源数据集包括 `v3_decom` 和 `videos`。

**Datastream 也独立于 `etl_run.py`。** 源库、同步表和目标数据集在数据流配置中维护；只增加本地脚本或 DDL，不会让新业务表自动同步。同步异常先查数据流及对应表的状态，不要直接重跑下游。

### 2.3 外部平台：Python ODS 采集

下载、归因、广告和 ECN 数据由 Python ODS 脚本读取外部 API / 报表文件，包括 iOS、Android、AppsFlyer、TikTok Ads / GMV Max 和 ECN。

这些任务位于 [jobs/ods/](./jobs/ods/)，通过 [env/etl_config.json](./env/etl_config.json) 纳入生产机 ETL。凭据入口见第 5 节。

## 3. 数据如何加工和展示

### 3.1 分层职责

| 层 | 做什么 |
|---|---|
| ODS | 保留原始数据，便于追溯和重算 |
| DWD | 清洗、去重、统一类型、拼完整明细 |
| DIM | 维护共享的实体属性、首次日期和归属关系 |
| DWS | 固化可复用的业务口径 |
| ADS | 按报表粒度聚合，提供展示字段和比率等轻计算 |

不是每条链路都要经过所有层，**没有实际作用的空转层不要建**。按规范，ADS 只读 DWS / DIM，业务口径优先放在中间层；历史例外以对应主题文档为准。

具体依赖关系看 [pipeline.md](./docs/rules/pipeline.md)，分层和校验规则看 [layering.md](./docs/rules/layering.md)。业务日期默认按 `America/Toronto` 切天，例外看对应主题；它和生产 cron 的运行时区不是一回事。

### 3.2 增量更新与安全写入

为避免每轮全量扫描历史数据，各任务根据上游变更或固定时间窗口判断需要更新哪些实体、日期或月份。**处理范围由各任务自己决定，不是每小时重算全部历史。** 窗口外的历史修正可能不会自动补上，中断较久后也需确认漏刷范围。

这里要分清两件事：**增量模式决定“重算哪些数据”，写入方式决定“怎样替换旧数据”。**

| 写入方式 | 通俗理解 | 主要风险 |
|---|---|---|
| 分开执行 `DELETE + INSERT` | 先撤掉旧货，再把新货摆上去 | 删除成功、插入失败时，对应数据会缺失 |
| staging 校验后替换 | 先在旁边备好新货，检查通过后再替换 | 准备和校验失败时旧数据不动；最终删表、换名仍可能失败，并非原子切换 |

仓库中两种实现仍然并存，不能把规范当成所有脚本都已完成改造。**新增或修改写入逻辑时遵循 [安全表变更协议](./.agents/skills/bigquery-safe-table-change/SKILL.md)**：先准备、校验，再替换；替换失败时保留可恢复数据。具体实现看脚本，增量取舍看 [window-incremental.md](./docs/rules/window-incremental.md)。

### 3.3 Superset 与飞书

[Superset 看板](https://bi.alvinclub.ca/superset/) 按 **dataset → chart → dashboard** 组织：dataset 连接报表表或查询，chart 定义图表，dashboard 组合展示。

看板只负责查询和展示，不代替 ETL 更新数据。业务主题、报表表和看板链接统一从 [docs/topics/index.md](./docs/topics/index.md) 查找；每个主题的 `fact.md` 记录业务口径，`patterns.md` 记录真实踩坑。

飞书日 / 周报由独立 cron 任务推送，不在 `etl_run.py` 内执行。具体时间和脚本见 [env/cron.txt](./env/cron.txt)。

维护 Superset 使用 [superset skill](./.agents/skills/superset/SKILL.md) 的 REST API 协议，优先修改已有 dataset / chart / dashboard，不平行再建一套。

## 4. 生产上怎么运行

生产机项目目录：`/home/echooo/sinn_project/data-warehouse`。

按仓库的 [env/cron.txt](./env/cron.txt)，cron 每小时 `:40` 启动 `etl_run.py`；实际时间、时区和环境变量以生产机 `crontab -l` 及系统配置为准。

`etl_run.py` 读取 [env/etl_config.json](./env/etl_config.json)，按依赖就绪派发任务，默认最多同时执行 4 个任务。配置顺序仅用于就绪任务的优先级，不是整层屏障。

必须知道的运行特点（本次代码需部署后才生效）：

- **依赖校验。** 启动前校验任务名称唯一、`depends_on` 只引用排在前面的任务；未知依赖、前向引用和环均拒绝运行。依赖静态检查仍需在发布前通过。
- **按声明依赖隔离失败。** 失败、超时、待恢复、被阻断或禁用的任务只阻断其下游；独立任务继续，整轮返回非零。共享资源不属于数据依赖：普通失败后，资源释放，独立的资源使用者仍可运行。
- **共享资源互斥。** 每张目标表和同一脚本单写者（脚本路径解析为真实绝对路径，包含相对路径和符号链接别名）；两个 DWD 事件任务串行访问 `oss_key_process_log`，防止重建状态表相互覆盖。同一 ECN / TikTok 提供方的两个采集任务也不重叠，避免任务内并发再乘以任务并发。配置可增加 `resources` 字符串列表，内置约束不能由配置移除。
- **超时。** 默认单任务 900 秒；期限不包含依赖与队列等待。到期向整个进程组发 SIGTERM，默认等 5 秒后 SIGKILL，组长回收再等待最多 1 秒，仍无法确认结束则标记待恢复；无输出、无换行和大量输出都不阻塞超时检查。组长退出后若仍有后台子进程，不报告成功。
- **及时告警。** 失败及其阻断关系一经识别，异步发送飞书通知；通知请求不占任务执行槽，不阻塞其他任务的超时处理。最后补发尚未通知的失败状态，同一失败快照发送失败后退避 1 秒补试一次，收尾可提前补试；新汇总也保留一次补试机会。HTTP 发送在不继承 ETL 锁的独立进程内执行，socket 超时 15 秒、整个发送期限 20 秒，超期终止通知进程。只发送 run_id、任务名和状态，不发送原始异常、日志或凭据。
- **同项目目录互斥。** 保留 `.etl_run.lock`，重复启动记录 `run_skipped` 并告警，不执行任务，沿用退出码 0。不要删除锁文件。锁覆盖通过本入口运行的任务，不覆盖直接执行单脚本、其他机器或外部 Cloud Run。
- **中断处理。** SIGTERM / SIGINT 通知全部工作线程停止子进程组，回收后才释放锁。父进程被 SIGKILL 时，子进程继承的锁在其退出前仍保持。
- **未确认结束的任务不自动重入。** `.etl_task_state.json` 在启动任务前原子写入，正常完成后移除对应记录。超时、中断、外部信号终止（包括 OOM/SIGKILL）、后台子进程或上轮残留 `running` 均保留为 `recovery_required`。下一轮只阻断这些任务、共享资源使用者及其下游，独立链路仍可运行。状态文件损坏或启动时无法写入则整轮拒绝运行并告警；执行中的状态持久化失败停止派发、清理在途任务并告警，保留先前的启动记录用于下一轮保护。
- **恢复边界。** 终止本地 Python 不代表 BigQuery 作业已停止。本阶段不自动取消远端作业、不重试写任务、不修复表或回填。解除待恢复状态前，操作人员必须确认远端写作业已结束、目标表/临时表状态及后续恢复范围；仅在持有同一个 `.etl_run.lock` 的条件下移除已核对任务的状态记录，不能清空整个文件。正常业务失败仍可在下一轮依赖就绪后重试；恢复标记仅针对未确认结束的执行。
- **可选参数。** 顶层 `scheduler` 支持 `max_workers`（1–4，默认 4）、`task_timeout_seconds`（1–86400，默认 900）、`terminate_grace_seconds`（1–30，默认 5）。任务可单独指定 `timeout_seconds`。本次未修改现有环境配置；`schedule_group` 尚未用于控制频率。
- **首次切换。** 不能与旧版无锁进程混跑；应在旧进程全部退出后部署。4 并发的生产耗时、配额及数据一致性尚须部署后验证，不能由本地测试推断已经提速。

并行安全审查范围：已核对配置内目标表引用、临时表命名、辅助表写入和采集端共享资源。业务目标表依赖保持原样；发现的共享辅助表为 `oss_key_process_log`，两个 DWD 的保护不覆盖独立 Cloud Run 对该表的外部写入，后者仍是原有跨调度边界。临时表使用目标表前缀；DWD 的固定 ledger 备份/替换表受同一共享资源约束。新增动态 SQL、外部 SQL 或额外写入目标必须重新核对，静态扫描不能证明全部运行时血缘。

本地验证（不运行业务任务）：

```bash
PYTHONDONTWRITEBYTECODE=1 python3 scripts/check_etl_dependencies.py
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p '*_test.py'
PYTHONDONTWRITEBYTECODE=1 python3 scripts/check_knowledge.py
```

依赖静态检查覆盖配置内 143 个任务的 SQL 字符串、f-string 表名片段和表名常量，忽略文档及 SQL 注释；不导入任务模块。新增动态拼接表名或外部 SQL 时仍需人工核对。外部 Datastream / Cloud Run / 独立 cron 源表不属于本轮任务图，任务成功也不等于外部源已刷新。

2026-10-13 本地复审补齐 13 条直接依赖（含素材 `dim_column`、下载 iOS/Android 源、模块月表、试穿和社媒），移除 2 条无实际读取关系的依赖。iOS 下载日期异常、iOS/Android 临时表写入失败均以失败退出；Android 无文件或空报告也失败。iOS 404 暂无报告仍沿用原有跳过语义。当前告警和隔离根据任务退出状态判断，不替代业务日期完整性校验。

日志入口：

| 日志 | 位置 |
|---|---|
| 调度生命周期日志，按天追加 | `logs/etl_YYYYMMDD.log` |
| 本轮任务 stdout / stderr，不交错 | `logs/runs/<run_id>/<table_name>.log` |
| 结构化运行事件 | `logs/runs/<run_id>/events.jsonl` |
| 业务脚本原有日志 | `logs/<layer>/<table_name>/` |
| 飞书推送日志 | `logs/ads_daily_*_lark_push.log`、`logs/ads_weekly_*_lark_push.log` |

`events.jsonl` 使用 UTC 时间，记录 run_id、任务开始/结束、状态、退出码、耗时、依赖等待、就绪后排队、超时阈值、阻断原因、峰值并发、任务耗时合计、整轮耗时和通知结果。子进程获得 `ETL_RUN_ID` / `ETL_TASK_ID`。SIGKILL 无法保证写出结束事件，要结合持锁进程和持久状态判断，不能当作成功。本阶段指标只覆盖编排，不包含 BigQuery job_id、扫描量、slot_ms 或业务数据新鲜度；这些需要后续查询级埋点。任务原始输出仍可能含业务数据，仅留服务器日志，不放入通知/结构化指标。日志保留与清理应由运维按容量制定，本次没有修改生产轮转配置。

判断运行是否正常，要看**最新一轮**的开始、完成或失败记录，不能拿当天较早一次成功当作最新状态。只有开始记录时，还需区分正在运行和异常中断；跨午夜要查相邻日期日志。

## 5. 环境、权限和配置在哪里

### 5.1 访问入口

| 项目 | 入口 / 说明 |
|---|---|
| GCP | 项目 `my-project-8584-jetonai`；数仓主数据集 `decom`，源数据还涉及 `v3_decom` / `videos` |
| 生产机 | 在 **GuYin 公司网络 / VPN** 下运行 `env/connect.py`，经 `huxin@jump.valleysound.xyz:2222` 堡垒机选择 `172.30.0.13` |
| GitHub | `git@github-work:HEchooo/data-warehouse.git`；访问配置见 [env/github-auth.md](./env/github-auth.md) |
| Superset | [bi.alvinclub.ca/superset/](https://bi.alvinclub.ca/superset/)，账号配置在 `env/superset.json` |
| 本地仓库 | 原负责人路径 `/Users/sinn/Documents/guyin/data-warehouse`，接手人使用自己的目录 |

接手需具备相应的 GCP、SSH、GitHub 和 Superset 权限；GCP 范围包括 BigQuery、Cloud Run / Eventarc、Datastream，以及相关密钥的读取权限，由管理员按接手范围配置。

### 5.2 Python 与运行配置

- 本地使用安装了 [requirements.txt](./requirements.txt) 依赖的 Python 3.11+ 环境，无需照搬原负责人的虚拟环境目录。Cloud Run 的依赖在 `jobs/ods/ods_event_log/requirements.txt` 单独维护。
- [env/etl_config.json](./env/etl_config.json) 的 `paths.local` 要适配本机项目目录、Python、日志目录及凭据文件路径；`paths.remote` 保留生产配置，当前生产 Python 为 `/usr/local/bin/python3.12`。
- `PATH_MODE_OVERRIDE=local / remote` 选择运行路径和环境配置，优先于配置文件默认值。生产调度显式传 `remote`。
- **`local` 不是测试库或只读模式，现有脚本仍可能写生产 BigQuery。不要靠启动 ETL 来验证新机器环境。**
- 通过调度器运行时，它会注入所选配置中的环境变量；直接执行单脚本不会自动得到这些变量，需自行配置。

### 5.3 凭据位置

以下只记录位置，不记录密钥值。未跟踪文件仅 `git clone` 无法获得，需通过加密渠道单独移交。

| 用途 | 配置 / 凭据位置 |
|---|---|
| BigQuery 默认认证（ADC） | `GOOGLE_APPLICATION_CREDENTIALS` 或本机 ADC 登录；原交接 service account 文件为 `env/my-project-8584-jetonai-2bafc5bf74da.json` |
| Android GCS / BigQuery | `ANDROID_GCS_SERVICE_ACCOUNT`、`ANDROID_BQ_SERVICE_ACCOUNT`，由运行配置指向对应文件 |
| iOS App Store Connect | `IOS_KEY_FILE`，原文件位于 `jobs/ods/ods_ios_download/*.p8` |
| 堡垒机 | `env/id_rsa` |
| ECN | `env/ecn.json` |
| AppsFlyer | `env/appsflyer.json` |
| TikTok Business API | GCP Secret Manager：`prod-ads-runner-tiktok-access-token` |
| Superset | `env/superset.json` |
| 飞书机器人 | `env/lark_webhook.json`；Cloud Run 使用环境变量 `LARK_ALERT_WEBHOOK` |

多数脚本使用 ADC，Android 下载使用独立凭据。原交接记录中的生产 ADC 配置在 **crontab 顶部**，指向 `/home/echooo/sinn_project/my-project-8584-jetonai-2bafc5bf74da.json`；该配置不在仓库 `env/cron.txt` 中，接手时须核对，不能用仓库文件覆盖整个 crontab。

## 6. 改动怎样上线

| 改动对象 | 生效方式 |
|---|---|
| 生产机 Python ETL | 本地在 `main` 提交并推送，再到生产目录 `git pull`；没有自动部署 |
| 调度时间 / cron 环境变量 | 在生产机调整实际 crontab；只改 `env/cron.txt` 不会生效 |
| Cloud Run 日志处理 | 在 Cloud Console 部署服务新修订版本，独立于生产机仓库 |
| Datastream 同步范围 | 在数据流配置中修改；新增同步或历史回填先确认范围 |
| Superset 图表 / 看板 | 按 superset skill 通过 API 修改并回查验证 |

提交前运行 `python3 scripts/check_knowledge.py`，检查知识文档一致性。上线前记录生产版本和改动范围，核对正在运行的任务；上线后确认生产版本、配置和相关运行结果。

确需手工跑 ETL 时，先确认处理范围、上游和运行窗口，避免与 cron 重叠。生产入口如下，**该命令会写生产数据**：

```bash
cd /home/echooo/sinn_project/data-warehouse
PATH_MODE_OVERRIDE=remote /usr/local/bin/python3.12 etl_run.py
```

**历史回填必须先确认，代码回退也不会恢复已写入的数据。** 出现异常先保存日志、确认影响范围，再选择恢复方式；不能未经核验就假定临时表或历史版本一定能恢复数据。

## 7. 出问题先看哪里

| 现象 | 排查顺序 |
|---|---|
| 埋点没进来 | GCS 日志 → Eventarc → Cloud Run 日志 → BigQuery ODS → 下游 DWD |
| 服务端表没更新 | Datastream 数据流 / 表状态 → BigQuery 源表 → 下游 ETL |
| 某张报表没更新 | 最新一轮 ETL 是否中断 → 对应任务日志 → 上游数据和目标分区 → 看板筛选条件 |
| 指标不对 | 主题 `fact.md` 的统计主体、过滤条件和聚合限制 → 上游字段及关联 → 图表查询 |
| 长时间中断后缺数据 | 核对增量窗口外的漏刷范围，确认后再补刷，不能假定重跑一轮会补齐 |

注意：Cloud Run 返回成功不代表每条日志都写入成功；BigQuery 表修改时间也不能证明预期分区齐全。Superset 中还存在其他项目的数据集，排查应沿具体看板的 dataset 查询及依赖走，不要把所有数据集都当作本项目的表。

业务问题与已确认的排查经验以 [主题导航](./docs/topics/index.md) 中的 `fact.md` / `patterns.md` 为准，具体参数、重试和 SQL 逻辑直接看对应脚本。

## 8. 已知情况与阅读入口

### 8.1 已知情况

- **ECN 接口曾因超时 / 502 导致 ETL 中断**，目前已降低视频归属接口并发；失败仍会阻断后续任务，排查应看接口和任务日志，不要把失败跳过当成功。
- **部分历史分层和写入逻辑尚未按现行规范改造**；社媒 v1 / v2 链路仍并存，删除旧链路前须确认旧看板和其他消费者已停用。
- `jobs/ods/ods_xiaohongshu_cost/` 不在主调度中，但业务仍需保留，不要当成无用目录删除。
- **仓库存在已知明文凭据风险**：`env/superset.json`、`env/lark_webhook.json`、`env/appsflyer.json`，小红书历史脚本及 token 文件，以及 `jobs/ods/ods_event_log/BI数据交接.md`。不要复制或继续提交真实值；治理需轮换凭据并移出版本控制，仅删除当前文件不能清除 Git 历史。
- 原交接的 `env/id_rsa` 没有密码短语，应通过加密渠道移交并更换或加固。未跟踪不等于历史上从未泄露。

这些是需要知道的现状，不是本交接文档要求接手前全部完成的整改清单；具体未落地需求和口径边界查看对应主题文档。

### 8.2 需要细节时查哪里

| 想了解什么 | 入口 |
|---|---|
| 开发规则总索引 | [AGENTS.md](./AGENTS.md) |
| 分层、依赖和增量规则 | [layering.md](./docs/rules/layering.md)、[pipeline.md](./docs/rules/pipeline.md)、[window-incremental.md](./docs/rules/window-incremental.md) |
| 字段计算与业务范围 | [主题导航](./docs/topics/index.md)、[data.md](./docs/rules/data.md) |
| 脚本、表结构、调度配置 | `jobs/`、`ddl/`、[env/etl_config.json](./env/etl_config.json) |
| 埋点原始文档 | `docs/references/` |
| 环境与凭据规则 | [environment.md](./docs/rules/environment.md) |
| 写入和看板维护协议 | [安全表变更 skill](./.agents/skills/bigquery-safe-table-change/SKILL.md)、[superset skill](./.agents/skills/superset/SKILL.md) |

建议先看本文的数据流和调度方式，再从主题导航选一张看板，沿 **看板 → ADS → DWS / DIM → 源数据** 阅读对应文档和代码。
