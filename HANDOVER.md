# 数据仓库项目交接文档

> 交接人：Sinn ｜ 最后更新：2026-10-09
> 接手范围：全盘（ETL pipeline + Superset 看板 + 生产机运维）
> 读完本文 + [AGENTS.md](./AGENTS.md) + [docs/rules/](./docs/rules/) 即可独立接手。

---

## 0. 三十秒版本

| 项 | 内容 |
|---|---|
| 做什么 | Alvin's Club App / Makepod 独立站 / ECN / TikTok 橱窗号 等 7 条业务线的 ETL + Superset BI 看板 |
| 技术栈 | Python 3.11/3.12 + BigQuery（无 Airflow，用 crontab + 自研 `etl_run.py` 串行调度） |
| 数据规模 | 143 个调度任务（ODS 7 / DWD 3 / DIM 12 / DWS 57 / ADS 64），40 个业务主题 |
| 生产机 | `/home/echooo/sinn_project/data-warehouse`（Linux，crontab 每小时 :40 全量串行跑一轮） |
| 本地开发 | `/Users/sinn/Documents/guyin/data-warehouse`（当前仓库，macOS） |
| 看板入口 | https://bi.alvinclub.ca/superset/ （Superset 4.0.2） |
| 仓库 | `git@github-work:HEchooo/data-warehouse.git`，直接在 `main` 提交推送 |

**这个项目最大的特点：文档即规范。** 代码之外的规则全部沉淀在 `docs/rules/*.md`，主题口径沉淀在 `docs/topics/*/fact.md`。改动前先读规则，比读代码更省时间。

---

## 1. 接手第一天要拿到的东西

按顺序办，缺一项都会卡住：

1. **GCP 项目权限**：`my-project-8584-jetonai`（BigQuery 数据全在 `decom` dataset）。角色清单见 [jobs/ods/ods_event_log/BI数据交接.md](./jobs/ods/ods_event_log/BI数据交接.md) 末尾，实际列的是：Artifact Registry Writer、BigQuery Admin / Data Editor / Job User、Cloud Build Editor、Cloud Datastore Owner、Cloud Run Admin / Source Developer、Cloud SQL Admin、Datastream Admin、Eventarc Developer。
2. **生产机 SSH 权限**：`/home/echooo/sinn_project/data-warehouse`，以及该机的 `crontab -l` 编辑权限。走堡垒机 `huxin@jump.valleysound.xyz:2222`，私钥随文件夹提供（`env/id_rsa`），入口脚本 `env/connect.py`。
3. **GitHub 仓库权限**：`HEchooo/data-warehouse`（公司账号，SSH key 配置见 [env/github-auth.md](./env/github-auth.md)）。
4. **Superset 账号**：账号密码在 `env/superset.json`（**注意：此文件当前被 git 跟踪，含真实凭据，见第 8 节风险**）。
5. **随文件夹提供的凭据文件**：文件**都在项目文件夹内**、被 `.gitignore` 排除，拷文件夹即可带走（「仓库里没有」只指 git 历史里没有，不代表文件缺失）：
   - `env/ecn.json`（ECN API host + key）
   - `env/my-project-8584-jetonai-2bafc5bf74da.json`（BigQuery service account `bigquery-user@my-project-8584-jetonai.iam.gserviceaccount.com`，与生产机 `/home/echooo/sinn_project/my-project-8584-jetonai-2bafc5bf74da.json` 是同一文件）
   - `env/id_rsa`（堡垒机私钥，**无密码短语**）
   - `jobs/ods/ods_ios_download/AuthKey_ZPUB8KVQ8R.p8`（iOS App Store Connect 私钥）
   - `jobs/ods/ods_android_download/*.json`（Android GCS / BQ service account）
6. **飞书（Lark）机器人 webhook**：`env/lark_webhook.json`。用途只有两个：4 个日报推送脚本，以及埋点链路（Cloud Run 环境变量 `LARK_ALERT_WEBHOOK`）。**全链路 ETL 失败不推飞书**，别指望它告警。
7. **TikTok Business API 的 access token**：不在仓库，在 GCP Secret Manager 的 `prod-ads-runner-tiktok-access-token`。
8. **本地 Python 环境**：`pip install -r requirements.txt`（依赖清单见仓库根目录）。本地 venv 路径写在 `env/etl_config.json` 的 `paths.local`，**接手后第一件事就是把这 3 个路径改成自己的**（`project_dir` / `python` / `log_dir`），否则 `PATH_MODE_OVERRIDE=local` 跑不起来。
9. **BigQuery 认证（最容易漏的一项）**：所有脚本用 ADC 认证。生产机靠 crontab **顶部**的一行环境变量：
   ```
   GOOGLE_APPLICATION_CREDENTIALS="/home/echooo/sinn_project/my-project-8584-jetonai-2bafc5bf74da.json"
   ```
   这行不在仓库里。本地开发则在 `~/.config/gcloud` 里 `gcloud auth application-default login`，或用文件夹内的 service account：`export GOOGLE_APPLICATION_CREDENTIALS=<文件夹>/env/my-project-8584-jetonai-2bafc5bf74da.json`。**漏配不会报「认证失败」，而是脚本连不上 BigQuery 直接失败，然后拖停其后的全部任务。**

---

## 2. 架构总览

### 2.1 分层（SSOT：[docs/rules/layering.md](./docs/rules/layering.md)）

```
外部源 ──▶ ODS ──▶ DWD ──▶ DWS ──▶ ADS ──▶ Superset
                    │        ▲
                    └──▶ DIM ┘
```

核心原则（务必先读懂再改代码）：

- **层数不是目标**。内容等价于 `SELECT * FROM 上一层` 的空转层不要建。
- **ADS 只能读 DWS / DIM**，禁止直接读 ODS / DWD / 外部源表。越层要按「待收口架构债」处理并先和需求方确认。
- **DWS 是口径的唯一出处**，且只固化可加的分子分母；比率 / 成本 / 占比由下游 `SAFE_DIVIDE(SUM(分子), SUM(分母))` 现算，**禁止对派生列求和或求平均**。
- **每层写目标表前必须自校验**（staging 行数非 0、主键唯一非空），校验不过就失败，目标表一个字节都不碰。写法统一走 `staging 表 → 校验 → 换名`，见 `bigquery-safe-table-change` skill。
- **首次 / 首日类口径不在增量脚本里现算**，必须沉到 DIM（`dim_device_first_active` / `dim_user_first_active` 等）。原因：增量窗口内的 `MIN(dt)` 不等于全历史首次值。

### 2.2 主链路拓扑（SSOT：[docs/rules/pipeline.md](./docs/rules/pipeline.md)）

不要在这里背，直接读 [docs/rules/pipeline.md](./docs/rules/pipeline.md) 的「主链路」章节，它按主题列出了每条链路的上下游。这里只给骨架：

- **事件主链路**：`ods_event_log → dwd_event_log → dws_device_daily / dws_user_daily → ads_*`（覆盖 App 内绝大多数主题）
- **用增链路**：`dwd_event_log → dim_device_first_active + dim_user_first_active → dws_device_daily + dws_user_daily`；`ods_appsflyer_download → dws_appsflyer_download_daily`；三者 `→ ads_daily_new`
- **下载链路**：`ods_ios_download + ods_android_download → dwd_download → dws_download_daily`
- **社媒链路**：`videos.* → dim_video → dws_social_video_snapshot_daily_v2 → dws_social_video_state_daily_v2 → 各社媒 ADS`
- **Makepod 链路**：`ods_event_log → dwd_makepod_event_log → dws_makepod_traffic_daily / dws_makepod_order_daily → ads_daily_makepod`
- **投放链路**：`TikTok Business API → ods_tiktok_ad_daily / ods_tiktok_gmv_max_daily → dws_* → ads_*`

### 2.3 编排方式（重要，容易踩坑）

- **调度器是 crontab + `etl_run.py`，没有 Airflow。**
- `crontab` 每小时 `:40` 跑 `PATH_MODE_OVERRIDE=remote /usr/local/bin/python3.12 etl_run.py`（见 [env/cron.txt](./env/cron.txt)）。
- `etl_run.py` 按 `env/etl_config.json` 的 `tasks` **数组顺序串行**执行，每个 task 起一个子进程，读取脚本 stdout 写日志。
- ⚠️ **任一 task 失败，`etl_run.py` 立即 `return 1` 退出，后面的 task 全部不执行。** 排在前面的链路挂了会把后面全部拖停：ECN 三个任务在 tasks 数组下标 72–74（第 73–75 个），一旦失败，其后的 **68 个任务**（素材维表、周报等）当轮全部不执行。排查「某张表今天没更新」时，先看当天 `logs/etl_YYYYMMDD.log` 是不是中途退出了。
- ⚠️ **全链路失败没有自动告警。** `etl_run.py` 只写日志、不推送飞书；`env/lark_webhook.json` 只服务 4 个日报推送脚本和埋点链路（Cloud Run 环境变量 `LARK_ALERT_WEBHOOK`）。ETL 中断不会主动通知任何人，必须人工盯 `logs/etl_*.log`。
- ⚠️ **`etl_config.json` 里的 `depends_on` / `schedule_group` / `layer` / `table_name` 只是文档用途，`etl_run.py` 实际只读 `stage` / `name` / `script` / `enabled`。** 真正的执行顺序 = tasks 数组顺序，新增任务时**必须手动插到正确位置**，靠 `depends_on` 不会自动排序。
- 另有 4 个独立 crontab 任务，不属于 `etl_run.py`：`ads_daily_investor_lark_push.py`（每天 6:00）、`ads_daily_ecn_lark_push.py`（6:00）、`ads_daily_makepod_lark_push.py`（2:00）、`ads_weekly_makepod_lark_push.py`（周一 2:30）。

### 2.4 日期与窗口（SSOT：[docs/rules/data.md](./docs/rules/data.md)、[docs/rules/window-incremental.md](./docs/rules/window-incremental.md)）

- 所有按天统计的 `dt` 默认按 **`America/Toronto`** 切天。例外：TikTok 广告 ODS 是广告主时区 `Etc/GMT+5`（固定 UTC-5，不含夏令时）。
- 窗口增量有三种模式，动手前先选模式：
  - **模式 A 窗口增量 upsert**（DIM 默认，6 小时窗口）→ `jobs/dim/dim_column.py`、`jobs/dim/dim_post.py`
  - **模式 B 分区重算**（DWS/ADS 默认，先发现待刷分区再 `DELETE + INSERT`）→ `jobs/dws/dws_column_interaction_daily.py`
  - **模式 C 固定回刷窗口**（性能优先，日表近 3 天 / 月表当月+上月）→ `jobs/ads/ads_daily_column_performance.py`
- **每张 ADS 表自己按本表上游判定待处理范围**，不共享整层日期入口（旧的共享 ADS 入口已从仓库移除）。
- 回刷历史分区前**必须先和业务方确认**，不能直接动生产数据。

---

## 3. 代码与文档地图

### 3.1 目录结构

```
data-warehouse/
├── etl_run.py              # 唯一调度入口，串行执行 etl_config.json 里的 tasks
├── requirements.txt        # ★ Python 依赖清单（pip install -r requirements.txt）
├── env/
│   ├── etl_config.json     # ★ 任务清单（143 个 task）与运行环境路径，编排的 SSOT
│   ├── connect.py          # 堡垒机连接入口（用同目录 id_rsa）
│   ├── cron.txt            # 生产机 crontab 内容（文档用途，改它不会自动生效）
│   ├── superset.json       # Superset 地址与账号（含真实凭据）
│   ├── lark_webhook.json   # 飞书机器人 webhook（含真实凭据）
│   ├── appsflyer.json      # AppsFlyer API 配置
│   ├── ecn.json            # ECN API 配置（已 gitignore，随文件夹提供）
│   ├── id_rsa              # 堡垒机私钥（已 gitignore，随文件夹提供）
│   ├── my-project-8584-jetonai-2bafc5bf74da.json  # BQ service account（已 gitignore）
│   └── github-auth.md      # 多 GitHub 账号 SSH 配置说明
├── jobs/                   # 各层生成逻辑，按层分目录
│   ├── ods/  dwd/  dim/  dws/  ads/
│   └── event_activity_rules.py  # 唯一的跨脚本共享常量（被动触达事件排除名单）
├── ddl/                    # 各层建表语句（ods/dwd/dim/dws/ads）
├── docs/
│   ├── rules/              # ★ 跨主题规则，改代码前必读
│   ├── topics/             # ★ 业务主题口径（fact.md + patterns.md），40 个主题
│   └── references/         # 埋点原始文档（App / Makepod / Launch）
├── scripts/check_knowledge.py  # 知识一致性校验，提交前必跑
└── logs/                   # 运行日志（已 gitignore）
```

### 3.2 规则文档路由（改动前先查这里）

[AGENTS.md](./AGENTS.md) 的「SSOT 路由表」是权威索引，最常用的几条：

| 要改什么 | 先读 |
|---|---|
| 新建表 / 判断该不该建层 | [docs/rules/layering.md](./docs/rules/layering.md) |
| 写脚本 / 改字段 / 改 DDL | [docs/rules/code.md](./docs/rules/code.md) |
| 指标口径（UV、比率、留存、NULL vs 0） | [docs/rules/data.md](./docs/rules/data.md) |
| 调度、日期判定、回刷 | [docs/rules/pipeline.md](./docs/rules/pipeline.md) + [docs/rules/window-incremental.md](./docs/rules/window-incremental.md) |
| 写主题文档 | [docs/rules/topics.md](./docs/rules/topics.md) |
| 某主题的业务口径 | `docs/topics/<主题>/fact.md` |
| 某主题的踩坑史 | `docs/topics/<主题>/patterns.md` |

主题导航总表：[docs/topics/index.md](./docs/topics/index.md)（含每张表的 Dashboard 链接）。

### 3.3 两个 skill（`jobs` 之外的执行协议）

- **`.agents/skills/bigquery-safe-table-change/`** — 改 BigQuery 表数据 / 字段时**必须**用。核心是 `staging → 校验 → 换名`，避免对生产表直接 `DELETE + INSERT`（校验失败时数据已被删一半）。
- **`.agents/skills/superset/`** — 用 Superset REST API 维护 dataset / chart / dashboard / filter。要点：先读 OpenAPI 不要猜接口；改前先读现状；写完必须回查验证；凭据不要拼进 shell 命令字符串（密码含 `$`、反引号等会被 shell 吃掉）。

### 3.4 提交前必做

```bash
python3 scripts/check_knowledge.py
```

校验 topic 索引、ADS 归属、知识引用和字段声明的一致性。跑不过不要提交。

---

## 4. 日常运维

### 4.1 手动跑一轮全链路

```bash
cd /home/echooo/sinn_project/data-warehouse
PATH_MODE_OVERRIDE=remote /usr/local/bin/python3.12 etl_run.py
```

本地测试时把 `PATH_MODE_OVERRIDE` 改成 `local`。**`env/etl_config.json` 的 `paths.local` 里三个路径（`project_dir` / `python` / `log_dir`）是原负责人机器的绝对路径，接手人必须先改成自己的**，`python` 指向你自己的 venv（依赖见根目录 `requirements.txt`）。`paths.remote` 是生产机路径，不要动。

`PATH_MODE_OVERRIDE` 优先于 `etl_config.json` 的 `path_mode`；**生产 cron 必须显式传 `remote`**，不能依赖配置文件默认值（默认值是 `local`）。

### 4.2 跑单张表

每张 ADS/DWS 都是独立可执行脚本，直接跑即可：

```bash
PATH_MODE_OVERRIDE=remote python3 jobs/ads/ads_daily_closet_overview.py
```

部分 ODS 脚本支持 `--start-date` / `--end-date` 显式回刷（如 `ods_tiktok_ad_daily.py`），具体看脚本 `parse_args`。**回刷前先和业务方确认。**

### 4.2.1 本地改动怎么上生产

**没有 CI/CD，也没有部署脚本。** 流程是手工同步：

1. 本地改完，直接在 `main` 提交并 `git push`。
2. SSH 上生产机，`cd /home/echooo/sinn_project/data-warehouse && git pull`。
3. 生产机不会自动同步，**忘记 pull 就是代码没上线**。

改完代码在生产生效前，务必核对：

1. 生产机 `git log -1` 是否等于你推送的 commit。
2. 生产机 `crontab -l` 是否与 `env/cron.txt` 一致（含 `PATH_MODE_OVERRIDE=remote`）；`env/cron.txt` 只是文档，改它不会自动生效，要在生产机上手工改 crontab。
3. 新增任务是否已插进 `env/etl_config.json` 的 `tasks` 数组正确位置——**`depends_on` 不参与排序**（见 2.3 节）。

### 4.3 日志在哪

- 全链路日志：`logs/etl_YYYYMMDD.log`（每次运行追加）
- 单脚本日志：`logs/<layer>/<table_name>/YYYY-MM-DD_<table_name>.log`
- 飞书日报推送日志：`logs/ads_daily_*_lark_push.log`（只有这 4 个）

### 4.4 新增一张 ADS 表的标准流程

1. 按 [layering.md](./docs/rules/layering.md) 判断口径该不该沉到 DWS/DIM；不要先在 ADS 跑通当长期方案。
2. 写 `jobs/ads/<表名>.py`。骨架参考 `ads_daily_formal_video_increment.py` 或 `ads_daily_tiktok_ad.py`：`configure_logging` / `dates_to_sql_list` / 待处理日期判定 / `CREATE staging → 校验行数与日期数 → 删除目标表 → RENAME 换名`。⚠️ **不要照抄 `ads_daily_closet_overview.py`**：它写的是裸 `DELETE + INSERT`，没有 staging 也没有校验，不符合 2.1 节的规则（现有 68 个 ADS 脚本里只有 16 个走了 staging 协议，新表一律按 staging 写）。
3. 写 `ddl/ads/<表名>.sql`。
4. **在 `env/etl_config.json` 的 `tasks` 数组里插到正确位置**（依赖的上游之后、依赖它的下游之前）。
5. 建 / 更新主题文档 `docs/topics/<主题>/fact.md`，并在 `docs/topics/index.md` 加一行导航。
6. 跑 `python3 scripts/check_knowledge.py`。
7. 在 Superset 建 dataset / chart / dashboard（用 `superset` skill）。

---

## 5. 高频故障排查手册

### 5.1 「今天看板没数据」

按顺序查（不要一上来就怀疑数据）：

1. **ETL 是否跑完**：看 `logs/etl_YYYYMMDD.log` 有没有 `ETL 全部完成`。没有就是中途失败了，往上翻找到第一个失败任务。**若是接手后第一次跑、且报错集中在连接/认证，先查 `GOOGLE_APPLICATION_CREDENTIALS` 是否配好（见第 1 节第 9 项）。**
2. **是不是 T+0 展示问题**：脚本支持 T+0 ≠ 每张表都支持当天。检查子任务内部数据窗口和成熟度上限（如留存率要等 `dt+N`）。
3. **筛选器范围**：Superset 的时间筛选器是否覆盖了目标日期。
4. **区分链路**：总量 / 新增异常时，先确认自己走的是事件链路还是下载链路。

### 5.2 「某个指标数字突然不对」

- **率值类**（留存率 / 渗透率 / 点击率 / ROI）：确认没有跨天或跨维度 `AVG` / `SUM`。必须回明细汇总分子分母重算。
- **UV 类**：确认统计主体没搞混（设备 UV / 注册用户 UV / 账号 UV 不可互换），且没跨天相加。
- **首次 / 首日类**：确认读的是 DIM 首次口径表，不是窗口内 `MIN()`。
- **NULL vs 0**：`NULL` = 尚未到观察期 / 无有效分母；`0` = 观察完成且确实为零。两者不能互换。分母为 0 默认返回 `NULL`，除非主题文档明确规定返回 `0`。
- **视频播放增量**：下游统一读 `dws_social_video_state_daily_v2`（前向填充的日末全量状态），不要读 `snapshot`（只有当天真实采集到的视频）。

### 5.3 「ADS 里某列整段为空」

大概率是**上游新加字段没同步补值**。典型场景：新加的扩展列只对回填后的分区有效，历史分区全空，看起来像「那段时间没发生」，实际是缺 join key。

判断方法：先查上游同期有值的行数（如 `SUM(spend)`）。上游有值但下游 join 不上，就是缺回填，不是业务为空。修法是显式回填上游分区再重跑下游。

> 历史真实案例见 [docs/topics/【Affiliate】投放数据看板/patterns.md](./docs/topics/【Affiliate】投放数据看板/patterns.md)（`tiktok_item_id` 历史分区全空事件）。

### 5.4 「调度中断了 / 漏刷了」

- 窗口增量模式下，调度中断超过窗口（默认 6 小时）会漏更，需要人工确认补刷范围。
- **任何任务失败都会拖停其后的全部任务**（见 2.3 节），不只是 ECN。判断影响面：找到 `logs/etl_YYYYMMDD.log` 里第一个失败任务，它后面没执行的任务全都要确认是否需要补跑。
- ECN 相关链路（tasks 数组下标 72–74）中断超过 6 小时，**需要单独确认并补刷其后 68 个任务涉及的素材维表和事实分区**。
- 补刷前先和业务方确认。

### 5.5 「在线看板有没有数据没在更新的」

2026-10-09 做过一次全量审计，方法可复用：

1. 拉 Superset 全部已发布 dashboard → 各 dashboard 的 chart → 各 chart 的 `query_context.datasource.id` → dataset 的 `table_name`。
2. 只保留**物理** dataset（`sql` 为空）且 `schema = decom` 的，避免把虚拟 dataset 和别的项目的表算进来。
3. 用 `SELECT table_id, TIMESTAMP_MILLIS(last_modified_time) FROM decom.__TABLES__` 一次性取所有表的最后修改时间，比对今天。

审计结论（2026-10-09）：**在线看板引用的 58 张物理表里，只有 1 张陈旧**，即 `ads_daily_retention_user_new_follow_bucket`（已修复）。其余 57 张均为当日更新。

注意：Superset 里还有大量 **不属于本项目**的 dataset（销售报表、优惠券、利润报表、物流数据、搜索词等，指向别的 schema/项目），排查时会大量报「表不存在」，这是正常的，不要当成故障。

### 5.6 Superset 相关问题

- 先读 `env/superset.json`，确认 `url` 和身份。**当前实例的 API `base_url` 不一定是 `/superset/`**，出现 `Invalid login` 先核对凭据和站点根路径。
- 当前实例 dataset refresh 用 **`PUT`**，不是 `POST`。
- 维护看板优先修现有 dataset / chart / dashboard，**不要平行再造一套 v2**。
- 不要擅自给 dashboard 设置角色（role）。
- 详细协议见 `.agents/skills/superset/SKILL.md`。

---

## 6. 业务线枚举与主题分布

业务线固定值（`docs/topics/index.md` 中定义，主题文档只能用这些值）：

- `Alvin's Club App` — 主 App，主题最多
- `Alvin's Club 商详页 Launch 投放`
- `Makepod 独立站`
- `ECN 视频`
- `MyCCH` — LTA 看板
- `TikTok Makepod 小店`
- `TikTok 橱窗号`

40 个主题目录中，**4 个是临时取数**（`docs/topics/*临时取数*`，只有查询口径、没落 ADS 表），其余是活跃看板主题。`docs/topics/index.md` 有 40 行主题导航，其中 35 个有 Dashboard 链接、41 个唯一 dashboard URL（含 1 个 deprecated）。完整清单和链接见 [docs/topics/index.md](./docs/topics/index.md)。

各主题的踩坑记录量分布（接手时优先读踩坑多的，说明复杂度高）：
`【Social Content】Makepod 店铺 TK 广告投放数据`(143 行) > `投资人报表【核心业务报表】`(134) > `【Affiliate】投放数据看板`(110) > `【AI-Fashion】素材内容数据`(107) > `【Social Content】社媒表现数据看板`(89) > `专栏追踪明细`(86)。

---

## 7. 遗留事项与未完成项

### 7.1 已确认的架构债

| 项 | 现状 | 后续方向 |
|---|---|---|
| 视频团队边界过滤在 ADS 聚合层 | `ads_daily_investor` 等仍在展示层做团队边界过滤（过渡处理） | 下沉到稳定中层（DIM/DWS） |
| `c_third_party_goods` 被 LTA 与 AI-Fashion 商详页投放共用 | 靠域名过滤隔离（LTA 走 `mycch.me`，投放走 `https://alvinclub.com`） | 不得放宽过滤；长期考虑拆表 |
| 5 张留存分桶表已在生产运行 | `ads_daily_retention_user_{duration,follow,tryon,product_detail,new_follow}_bucket` 均已建表并在调度中（tasks 数组下标 110–114、`enabled=true`）。前 4 张自 2026-05 起每日更新；`new_follow_bucket` 原先未纳入调度、数据停在 2026-05-25，2026-10-09 已纳入调度并全量回刷至 2026-02-09 ~ 2026-10-09（2916 行，每日 12 行 = 3 user_group × 4 桶） | **不要重复建表**；如需重刷用 `--full-refresh`，并按第 4.2 节先和业务方确认 |
| ⚠️ `docs/topics/留存用户特征数据/fact.md` 含过时声明 | 该文件写「`duration` / `follow` / `tryon` / `product_detail` 四张分桶表生产表尚未创建」，**与实际不符**（4 张均已建表并每日更新）。另写 `new_follow_bucket`「已回刷 2026-05-01 ~ 05-25」，现也已过时（已回刷至 2026-10-09）。经确认暂不修改该文件 | 接手人读 fact.md 时以本表为准；后续择机修正 |
| TikTok GMV Max 只做到 campaign 级 | 商品级 / 素材级 / 小时级未接 | 见 `docs/topics/【Social Content】Makepod 店铺 TK 广告投放数据/fact.md` 的「未落地字段」 |
| `dws_social_*` 的 v1 表仍在调度中 | v1 / v2 并存。**v1 不是死链**：`dws_social_video_snapshot_daily → dws_social_video_state_daily → dws_social_channel_daily → ads_daily_social_content` 是一条完整链路，末端被 Superset **dashboard 102**「【Social Content】社媒表现数据看板」（slug `social-content`，**已下线** `published=False`，不在 `index.md` 导航）的 7 个 chart（575–580、586）消费；dashboard 116（`social-content-v2`，`published=True`）是它的完整替代 | 删除 v1 的真实代价 = 删掉 dashboard 102。确认 102 无人使用后，再连 4 个任务、脚本、DDL 一起清 |
| `jobs/ods/ods_xiaohongshu_cost/` 独立于主链路 | 不在 `etl_config.json` 中，是历史脚本，硬编码了另一台机器路径和明文密码 | **已知未治理风险**，见第 8 节；业务仍需保留，接手人不要误删 |

### 7.2 需要确认的口径 / 需求

- `docs/topics/*/fact.md` 里标「未落地」「待确认」的字段，各主题都有若干，逐个主题看 `fact.md`。
- 4 个临时取数主题（2026-05 / 06 / 07 / 08）是当时的临时需求，接手后确认是否还需要保留。

### 7.3 观察项

- **ECN 视频归属接口并发已从 32 降到 4**（2026-09-24 生产 32 并发导致大量连接超时和 502，熔断后阻断了排在后面的素材链路）。接手后盯几天成功数 / 失败数；若低并发仍失败，继续排查 ECN 网关和后端日志。**不要把失败当成功。**
- 两个 ECN ODS 的重试策略**不同，不要混记**：
  - `ods_ecn_video_publication_owner`：连接超时 5s，连接错误与 502/503/504 **各重试 1 次（固定 2s 间隔）**；另有熔断器 `CIRCUIT_FAILURE_LIMIT=3`——连续 3 次不可用即 `RuntimeError` 整批停止。这是有意的「宁可失败也不把失败当成功」。
  - `ods_ecn_account_affiliation`：502/503/504 重试 **5 次，2/4/8/16/32s 指数退避**。
  - 两者都不把 HTML 错误页当 JSON 解析（响应非 dict 时返回 `{}`）。
- ECN 熔断失败的真实代价：ECN 在 tasks 数组下标 72–74，其后 68 个任务当轮全部不执行，且**没有告警**（见 2.3 节）。

---

## 8. ⚠️ 安全风险（接手后优先处理）

仓库中存在**明文凭据**，需要尽快治理。以下只列位置和类型，不复制具体值：

| 位置 | 问题 | 建议 |
|---|---|---|
| `jobs/ods/ods_xiaohongshu_cost/xiaohongshu_cost_data.py` | 硬编码 Doris 数据库明文密码 + 小红书 API secret | **业务确认仍需保留**，暂不删除。治理方向：改为读环境变量或本地未跟踪配置 |
| `jobs/ods/ods_xiaohongshu_cost/bigquery_xiaohongshu_cost_data.py` | 同上，含明文 secret | 同上 |
| `jobs/ods/ods_xiaohongshu_cost/token.json` | 被 git 跟踪的 token 文件（当前内容已失效） | 从版本控制移除并加入 `.gitignore` |
| `jobs/ods/ods_event_log/BI数据交接.md` | 含 Superset `SUPERSET_SECRET_KEY` 明文和服务器地址 | 轮换 secret，文档中脱敏 |
| `env/superset.json` | 被 git 跟踪，含真实账号密码 | 加入 `.gitignore` 并 `git rm --cached`，密码改为环境变量 |
| `env/lark_webhook.json` | 被 git 跟踪，含飞书机器人 webhook（等同于凭据） | 同上 |
| `env/appsflyer.json` | 被 git 跟踪，含真实 AppsFlyer JWT token | 同样处理 |

已正确处理的（供参考）：`env/ecn.json`、`env/id_rsa`、`env/my-project-8584-jetonai-2bafc5bf74da.json`、`jobs/ods/ods_android_download/*.json`、`jobs/ods/ods_ios_download/*.p8` 都已在 `.gitignore` 中。`jobs/ods/ods_tiktok_ad_daily` 和 `ods_tiktok_gmv_max_daily` 的 token 走 GCP Secret Manager，是正确做法。

⚠️ **`env/` 下新增的未跟踪凭据文件**（`id_rsa`、`my-project-8584-jetonai-2bafc5bf74da.json`）随文件夹传递，**不要 `git add -A` 一把梭**，`.gitignore` 已覆盖，但换机器/换人后要重新确认 `git status` 里没有它们。

⚠️ **`env/id_rsa` 没有密码短语**，拿到文件即可登录堡垒机。传文件夹时走加密渠道（不是微信/邮件明文），接手人接管后建议给这把 key 加密码短语或换成他自己的 key。

**注意**：这些凭据已进入 git 历史，仅 `git rm` 不会从历史中移除。轮换密钥比清理历史更实际。

按 [docs/rules/environment.md](./docs/rules/environment.md)：脚本读凭据应优先用环境变量、本地未跟踪配置文件或外部密钥管理，仓库中只保留脱敏模板。

---

## 9. 外部资源与上下文

### 9.1 数据平台

- GCP 项目：`my-project-8584-jetonai`，BigQuery dataset：`decom`（数仓主库）、`videos`（社媒源）、`v3_decom`（服务端业务库）
- 埋点链路：App/Web 上报 → GCP Storage（`decom-prod-client-log` / `mall-prod-client-log`）→ Eventarc → Cloud Run（`decom-track-log-runner` / `mall-track-log-runner`）→ BigQuery。详见 [jobs/ods/ods_event_log/README.md](./jobs/ods/ods_event_log/README.md) 和 [BI数据交接.md](./jobs/ods/ods_event_log/BI数据交接.md)。
- 埋点字段文档：`docs/references/App 数据埋点.txt`、`Makepod 数据埋点.txt`、`Launch 数据埋点.txt`

### 9.2 看板

- Superset 4.0.2：https://bi.alvinclub.ca/superset/
- dashboard 的 id / slug 见 [docs/topics/index.md](./docs/topics/index.md)（41 个唯一 URL，含 1 个 deprecated）

### 9.3 外部 API

| 来源 | 用途 | 凭据位置 |
|---|---|---|
| AppsFlyer | 下载归因 | `env/appsflyer.json` |
| iOS App Store Connect | iOS 下载 | `.p8` 私钥（未跟踪） |
| Android GCS / BQ | Android 下载 | service account json（未跟踪） |
| TikTok Business API | 广告投放 / GMV Max | GCP Secret Manager |
| ECN API | 视频归属 / 账号联盟 | `env/ecn.json`（未跟踪） |
| 飞书机器人 | 4 个日报推送；埋点链路告警（Cloud Run 环境变量 `LARK_ALERT_WEBHOOK`） | `env/lark_webhook.json` |

---

## 10. 建议的接手路径

1. **第 1 天**：拿权限（第 1 节）→ 本地环境三件事：`pip install -r requirements.txt`、改 `env/etl_config.json` 的 `paths.local`、配好 BigQuery 认证（`GOOGLE_APPLICATION_CREDENTIALS` 指向 `env/my-project-8584-jetonai-2bafc5bf74da.json`）→ 和原负责人确认生产机代码同步方式（第 4.2.1 节）→ 通读本文件 + `AGENTS.md` + `docs/rules/layering.md` + `docs/rules/pipeline.md`。
2. **第 2 天**：本地跑通一轮（`PATH_MODE_OVERRIDE=local python3 etl_run.py`），对照 `logs/etl_*.log` 看每层在做什么。
3. **第 3 天**：挑一个主题（建议从 `用增报表` 入手，链路最清晰），从 `fact.md` 倒推 SQL，读完整链路。
4. **第 4 天起**：按第 7、8 节的清单处理遗留项和安全风险；遇到真实踩坑写回对应主题的 `patterns.md`。
5. **持续**：改动前查 SSOT 路由表，提交前跑 `check_knowledge.py`，回刷前问业务方。

---

## 附：常用命令速查

```bash
# 跑全链路（生产）
PATH_MODE_OVERRIDE=remote /usr/local/bin/python3.12 etl_run.py
```

```bash
# 跑单张表
PATH_MODE_OVERRIDE=remote python3 jobs/ads/ads_daily_closet_overview.py
```

```bash
# 提交前校验
python3 scripts/check_knowledge.py
```

```bash
# 列出所有 ADS 任务（编排 SSOT）
python3 -c "import json;[print(t['table_name'], t['script'], t['depends_on']) for t in json.load(open('env/etl_config.json'))['tasks'] if t['layer']=='ADS']"
```

```bash
# 看当天全链路日志是否跑完
grep "ETL 全部完成" logs/etl_$(date +%Y%m%d).log
```
