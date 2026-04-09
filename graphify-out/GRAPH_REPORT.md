# Graph Report - /Users/sinn/Documents/guyin/data-warehouse  (2026-04-09)

## Corpus Check
- Corpus is ~29,600 words - fits in a single context window. You may not need a graph.

## Summary
- 350 nodes · 400 edges · 45 communities detected
- Extraction: 90% EXTRACTED · 10% INFERRED · 0% AMBIGUOUS · INFERRED: 40 edges (avg confidence: 0.69)
- Token cost: 0 input · 0 output

## God Nodes (most connected - your core abstractions)
1. `jobs/ads/ads_daily.py` - 22 edges
2. `dwd_event_log 事件主链路` - 14 edges
3. `Topic: 穿搭工具点击使用` - 11 edges
4. `Topic: 试穿结果转化` - 10 edges
5. `Topic: 首页模块曝光与点击明细` - 10 edges
6. `BI 数据交接文档` - 10 edges
7. `jobs/ads/ads_daily_investor.py` - 9 edges
8. `BigQuery table decom.dwd_event_log` - 9 edges
9. `event_ts_expr()` - 9 edges
10. `run_dws_video_daily (视频播放快照日表: 补采/填充/日增)` - 9 edges

## Surprising Connections (you probably didn't know these)
- `Rationale: 先确认是不是固定四行预期` --rationale_for--> `jobs/ads/ads_daily_home_module_performance.py`  [INFERRED]
  docs/topics/首页模块曝光与点击明细/patterns.md → jobs/ads/ads_daily_home_module_performance.py
- `首页模块事件映射: v_home_* / c_home_*` --semantically_similar_to--> `MODULE_MAP_SQL: module -> exposure_event/click_event 显式映射`  [EXTRACTED] [semantically similar]
  docs/topics/首页模块曝光与点击明细/fact.md → jobs/ads/ads_daily_home_module_performance.py
- `run_etl(config, config_path)` --references--> `PATH_MODE_OVERRIDE（remote/local 运行模式覆盖）`  [INFERRED]
  etl_run.py → docs/rules/environment.md
- `Topic: 穿搭工具点击使用` --references--> `jobs/ads/ads_daily.py`  [EXTRACTED]
  docs/topics/穿搭工具点击使用/fact.md → jobs/ads/ads_daily.py
- `Rationale: 首次跑通前先确认表是否已经创建` --rationale_for--> `BigQuery table decom.ads_daily_tryon_add_cart_conversion`  [INFERRED]
  docs/topics/试穿结果转化/patterns.md → jobs/ads/ads_daily.py

## Hyperedges (group relationships)
- **Social Content 链路 (视频源表 -> dws_social_video_daily -> ads_daily_social_content)** — bq_videos_yt_videos, bq_videos_videos, bq_videos_ig_media, dws_social_video_daily_run_dws_social_video_daily, bq_decom_dws_social_video_daily, ads_daily_social_content_run_ads_daily_social_content, bq_decom_ads_daily_social_content [EXTRACTED 1.00]
- **iOS Sales Report 入仓与分区替换流** — ios_script_ios_download, ios_script_create_temp_table, ios_script_build_ios_changed_table, ios_script_drop_and_rename_table, ios_script_bigquery_table_decom_ods_ios_download, ios_script_apple_sales_reports_api, ios_readme_table_ods_ios_download [INFERRED 0.83]
- **小红书离线报表拉取与落库流（BigQuery/Doris 两版本）** — xhs_bq_refresh_token_flow, xhs_adapi_refresh_token_endpoint, xhs_bq_get_offline_data, xhs_adapi_offline_report_endpoint, xhs_bq_table_mall_xiaohongshu_ad, xhs_doris_get_offline_data, xhs_doris_insert_xiaohongshu_ad [INFERRED 0.84]
- **下载指标链路 (ODS -> DWD -> DWS -> ADS)** — bq_decom_ods_android_download, dwd_download_run_dwd_download, bq_decom_dwd_download, dws_daily_run_dws_download_daily, bq_decom_dws_download_daily_target, ads_daily_new_run_ads_daily_new [INFERRED 0.85]
- **Social Content 独立链路（DWS->ADS）** — table_decom_dws_social_video_daily, table_decom_ads_daily_social_content, script_jobs_dws_dws_social_video_daily_py, script_jobs_ads_ads_daily_social_content_py, concept_dense_daily_semantics, concept_interval_expand_generation [INFERRED 0.87]
- **试穿结果转化: c_tryon 与 c_add_cart -> ads_daily_tryon_add_cart_conversion -> dashboard 97** — 试穿结果转化_fact_topic, event_c_tryon, event_c_add_cart, dwd_event_log_table, ads_daily_tryon_add_cart_conversion_table, 试穿结果转化_fact_dashboard_97 [EXTRACTED 0.75]
- **首页模块曝光与点击明细: dwd_event_log -> ads_daily_home_module_performance -> dashboard 92** — 首页模块曝光与点击明细_fact_topic, dwd_event_log_table, ads_daily_home_module_performance_script, ads_daily_home_module_performance_table, ads_daily_home_module_performance_module_map_sql, concept_fixed_modules_star_magazine_brand_feeds, concept_home_module_event_mapping, 首页模块曝光与点击明细_fact_dashboard_92 [EXTRACTED 0.80]
- **穿搭工具点击使用: c_tryon_confirm + 维度映射 -> ads_daily_tryon_confirm / ads_daily_post_tryon_confirm -> dashboard 96** — 穿搭工具点击使用_fact_topic, event_c_tryon_confirm, dwd_event_log_table, v3_decom_community_post_table, ads_daily_tryon_confirm_table, ads_daily_post_tryon_confirm_table, 穿搭工具点击使用_fact_dashboard_96 [EXTRACTED 0.75]
- **Event Log Cloud Run 入仓流** — ods_event_log_readme_architecture_flow, main_process_log_file, main_get_log_entity_list_from_content, applog_decode_json, main_save_log_list, main_bigquery_table_decom_ods_event_log, main_save_oss_key_process_log, main_bigquery_table_decom_oss_key_process_log [INFERRED 0.85]
- **ADS 日报统一编排报表集合** — pipeline_ads_daily_orchestrator, pipeline_ads_daily_content_performance_table, pipeline_ads_daily_column_performance_table, pipeline_ads_daily_user_duration_frequency_table, pipeline_ads_daily_investor_table [EXTRACTED 1.00]
- **专栏追踪明细与帖子快照协同链路** — topic_column_perf, pipeline_dws_column_post_daily, post_mapping_kol_rel, post_mapping_community_post, post_mapping_tryon_task [INFERRED 0.84]
- **投资人报表中间层指标流** — topic_investor, topic_investor_dws_video_daily, topic_investor_dws_appsflyer_download_daily, topic_investor_dws_content_item_device_daily, topic_investor_dws_product_detail_daily, topic_investor_lark_push [EXTRACTED 1.00]

## Communities

### Community 0 - "主题与编排规则"
Cohesion: 0.05
Nodes (47): Graph Report Community: ADS 内容表现, Graph Report God Node: jobs/ads/ads_daily.py, ODS Event Log Cloud Functions 依赖, 专栏日报异常先回看 dwd_event_log 事件明细, 在线帖子快照与试穿日指标不能混表且快照应独立日调度, 统一内容曝光分母后再补 read_rate, INSERT 列顺序必须与 SELECT 列顺序对齐, Superset 新增指标列需同步重建 query_context (+39 more)

### Community 1 - "ADS 报表实现"
Cohesion: 0.07
Nodes (44): jobs/ads/ads_daily_content_performance.py, BigQuery table decom.ads_daily_content_performance, ddl/ads/ads_daily_home_module_performance.sql, MODULE_MAP_SQL: module -> exposure_event/click_event 显式映射, jobs/ads/ads_daily_home_module_performance.py, BigQuery table decom.ads_daily_home_module_performance, Superset Dashboard 101: 核心业务报表 - 明细表, Lark Wiki: 投资人报表数据口径文档 (+36 more)

### Community 2 - "社媒与全量报表"
Cohesion: 0.07
Nodes (33): 代码规则（DDL/更新数据/路径/清理/可读性）, 停留时长口径：仅 app_launch 的 args_session_duration 去重求和, ID 清洗：column_id 双引号/占位值导致映射为空的排查点, 去重优先级：raw_event_id 优先，缺失回退 hash_id, Dense daily 语义：从 published_dt 起每天一行，未采集日沿用最近累计值, dt 切天规则：America/Toronto, 帖子日报关注归因：同日详情曝光 last-touch + raw_event_id 去重, 关注归因规则：c_follow 帖子级需按同日详情曝光 last-touch (+25 more)

### Community 3 - "试穿与时长指标"
Cohesion: 0.11
Nodes (24): 关注事件 last-touch 归因到帖子详情曝光 (c_follow -> 最后一条 exposure), run_ads_daily_post_performance (内容日帖表现日报), run_ads_daily_post_tryon_confirm (穿搭工具点击使用: 日×帖子), clean_sku_expr (清洗 args_sku 多层引号/反斜杠与 'null'), run_ads_daily_product_tryon_performance (商详页试穿: dt×SPU×SKU), run_ads_daily_tryon_add_cart_conversion (试穿结果转化日报: tryon_uv/add_cart_uv), run_ads_daily_tryon_confirm (穿搭工具点击使用: 每日一行), run_ads_daily_user_duration_frequency (用户时长与访问频次/留存) (+16 more)

### Community 4 - "DWS 日聚合主链路"
Cohesion: 0.13
Nodes (20): event_ts_expr(), get_dates_to_process(), get_download_dates_to_process(), merge_dim_device_first_active(), merge_dim_user_first_active(), 找出下载 DWD 层有新数据但 DWS 下载层尚未处理的日期。     使用多伦多时间（America/Toronto），按 T+1 处理。, 增量维护 dim_device_first_active 维度表。     只扫描本批次日期分区，对新设备 INSERT，对已有设备取 LEAST。     使, 增量维护 dim_user_first_active 维度表。     只扫描本批次日期分区，对新用户 INSERT，对已有用户取 LEAST。     使用多 (+12 more)

### Community 5 - "ODS Event Log 落盘"
Cohesion: 0.11
Nodes (20): AppTrackLogEntity（埋点日志实体）, decode_json(json_str, file_path)（JSON -> AppTrackLogEntity）, BigQuery 表：my-project-8584-jetonai.decom.ods_event_log（代码中写入目标）, BigQuery 表：my-project-8584-jetonai.decom.oss_key_process_log（代码中写入目标）, get_log_entity_list_from_content(file_content, gcs_key)（逐行解析日志）, normalize_numeric_for_bigquery(value)（NUMERIC 规范化，最多 9 位小数）, parse_log_day_from_gcs_key(gcs_key)（从路径提取 YYYY-MM-DD）, process_log_file(request)（Cloud Run HTTP 入口） (+12 more)

### Community 6 - "BI 交接与 iOS 下载"
Cohesion: 0.12
Nodes (18): BigQuery 使用经验（计费按扫描量，建议 Partition/Cluster）, BigQuery 字段变更流程（临时表 -> 快照 -> drop 原表 -> rename）, Cloud Run：decom-track-log-runner（Decom 埋点日志函数）, Cloud Run：mall-track-log-runner（AC 埋点日志函数）, GCP Datastream 同步与回填操作（Initiate backfill, 配置同步表）, BI 数据交接文档, GCP 角色清单（Artifact Registry/BigQuery/Cloud Run/Eventarc/Datastream 等）, GCS Bucket：decom-prod-client-log（Decom 埋点日志） (+10 more)

### Community 7 - "社媒视频链路"
Cohesion: 0.17
Nodes (16): get_social_content_full_dates_to_process (按最早发布时间生成全量日期序列), run_ads_daily_social_content (社媒主题日报: dt×展示渠道), ADS 分区替换: staging 表 + changed 表 + drop+rename + 行数校验, BigQuery Table: decom.ads_daily_social_content, BigQuery Table: decom.dws_social_video_daily, BigQuery Table: decom.dws_video_daily, BigQuery Table: videos.ig_media, BigQuery Table: videos.ig_media_stats_log (+8 more)

### Community 8 - "试穿确认报表"
Cohesion: 0.15
Nodes (14): ddl/ads/ads_daily_post_tryon_confirm.sql, jobs/ads/ads_daily_post_tryon_confirm.py, BigQuery table decom.ads_daily_post_tryon_confirm, ddl/ads/ads_daily_tryon_confirm.sql, jobs/ads/ads_daily_tryon_confirm.py, BigQuery table decom.ads_daily_tryon_confirm, 新增字段影响 BI 时需回填历史分区, 看板异常先检查默认时间范围/筛选器/数据源 (+6 more)

### Community 9 - "下载归因链路"
Cohesion: 0.17
Nodes (13): BigQuery Table: decom.dws_appsflyer_download_daily, BigQuery Table: decom.ods_android_download, BigQuery Table: decom.ods_appsflyer_download, BigQuery Table: decom.ods_ios_download, get_dates_to_process (下载待刷日期: 近 N 天 T+1), run_dwd_download (统一下载明细口径: iOS+Android), run_dws_appsflyer_download_daily (AppsFlyer 下载按天汇总), android_download (GCS 读取 UTF-16 CSV 并分区替换写入 ODS) (+5 more)

### Community 10 - "专栏日报实现"
Cohesion: 0.27
Nodes (11): clean_id_expr(), column_id_expr(), dates_to_sql_list(), event_ts_expr(), module_expr(), 将 logAt_timestamp 先按 prop_timezone 解释为真实时间，再转成 UTC TIMESTAMP。     logAt_timestam, v3 服务表的 gmt_create 是 UTC DATETIME，这里统一转成 TIMESTAMP。, dwd_event_log 的 args_* 有时来自 JSON_EXTRACT，可能携带首尾双引号：     例如：'"2602276189664"'。这里统 (+3 more)

### Community 11 - "ETL 运行环境"
Cohesion: 0.22
Nodes (9): PATH_MODE_OVERRIDE（remote/local 运行模式覆盖）, Superset 维护方式：优先用 REST API 修现有 dataset/chart/dashboard, 环境规则（Python 环境/etl_run.py/PATH_MODE_OVERRIDE/Superset）, ETL 配置文件（env/etl_config.json）, load_config(config_path), main(), resolve_script_path(project_dir, script), run_etl(config, config_path) (+1 more)

### Community 12 - "新增与全量日报"
Cohesion: 0.28
Nodes (9): run_ads_daily_new (ADS 新增报表构建), event_ts_expr (按 prop_timezone 解释 logAt_timestamp 再转 UTC), run_ads_daily_total (ADS 总量报表构建), BigQuery Table: decom.ads_daily_new, BigQuery Table: decom.ads_daily_total, BigQuery Table: decom.dws_device_daily, BigQuery Table: decom.dws_user_daily, 概念: 指标按 America/Toronto 切天 (+1 more)

### Community 13 - "Graphify 与专栏映射"
Cohesion: 0.25
Nodes (8): Graphify 知识图规则, SSOT 路由表, Graph Report Community: Post Column 映射, Graph Report Community: 规则与主题文档, column_id 双引号需先清洗再映射 column_name, v3_decom.community_post 帖子表, v3_decom.kol_rel 专栏映射表, v3_decom.common_ext_ai_task 试穿任务表

### Community 14 - "iOS 下载口径"
Cohesion: 0.29
Nodes (7): 引用：Apple Developer - Financial report fields, 引用：Apple Developer - Product type identifiers, 引用：Apple Developer - Sales and Trends metrics, 指标口径：新增下载量（仅 product_type_identifier=1）, 枚举：product_type_identifier（下载/重装/更新/IAP 等分类）, BigQuery 表 ods_ios_download（字段与含义说明）, 时区说明：报告基于太平洋时间 PT

### Community 15 - "专栏帖子快照"
Cohesion: 0.4
Nodes (5): dates_to_sql_list(), get_column_post_dates_to_process(), 只补昨日快照。     主 ETL 可能每小时触发，但帖子快照语义是“昨日固化一次”，     因此只在目标表缺少昨日分区时返回该日期。, 产出专栏维度的在线帖子日快照。      粒度：     - dt × module × column_id      说明：     - 该表只承载“昨日 0, run_dws_column_post_daily()

### Community 16 - "DWD Event 清洗"
Cohesion: 0.33
Nodes (6): BigQuery Table: decom.ods_event_log, args_spu/args_post 独立展开策略 (spu 行与 post 行分开), generate_raw_event_id (基于 row payload 的 md5 事件键), invite_code_to_user_id (邀请码解码为 invite_user_id), transform_data (展开 args_spu/args_post 并生成 dwd_event_log 行), invite_code_to_user_id (邀请码解码逻辑抽离版)

### Community 17 - "小红书投放"
Cohesion: 0.5
Nodes (5): 小红书广告 API：离线报表 /api/open/jg/data/report/offline/account, get_offline_data(filter_date)（离线报表拉取并写入 BigQuery）, BigQuery 表：my-project-8584-jetonai.mall.xiaohongshu_ad, get_offline_data(filter_date)（离线报表拉取并写入 Doris）, Doris 表写入：INSERT INTO xiaohongshu_ad VALUES (...)

### Community 18 - "Android 下载文档"
Cohesion: 0.5
Nodes (4): 概念: Device vs User 统计口径区别, README: Android 下载量数据 (Google Play Console CSV), 字段定义: date/country/daily_device_installs 等, 数据源: GCS installs CSV (UTF-16, installs_{package}_{YYYYMM}_country.csv)

### Community 19 - "OSS 处理日志"
Cohesion: 0.67
Nodes (3): BigQuery Table: decom.oss_key_process_log, safe_update_oss_key_process_log (通过重建表更新 analyze_state), Rationale: 用重建表方式更新 oss_key_process_log 以规避 Streaming Buffer 限制

### Community 20 - "投资人 Lark 推送"
Cohesion: 1.0
Nodes (3): Lark 日报不能挂在 ads_daily.py 高频成功回调上, 投资人报表 Lark 独立推送脚本, Lark 推送按 America/Toronto 昨日取数

### Community 21 - "专栏字段表达式"
Cohesion: 1.0
Nodes (2): clean_id_expr (清洗 args_* ID 字符串引号与 'null'), module_expr/column_id_expr (按事件与 args_* 推断 module/column_id)

### Community 22 - "小红书 Token 刷新"
Cohesion: 1.0
Nodes (2): 小红书广告 API：/api/open/oauth2/refresh_token, OAuth2 refresh_token 刷新并回写 token.json（脚本顶层流程）

### Community 23 - "项目架构原则"
Cohesion: 1.0
Nodes (2): 架构换性能原则, 数据工程项目（Python + BigQuery + Superset）

### Community 24 - "用户时长表"
Cohesion: 1.0
Nodes (1): BigQuery table decom.ads_daily_user_duration_frequency

### Community 25 - "商详试穿表"
Cohesion: 1.0
Nodes (1): BigQuery table decom.ads_daily_product_tryon_performance

### Community 26 - "帖子表现表"
Cohesion: 1.0
Nodes (1): BigQuery table decom.ads_daily_post_performance

### Community 27 - "用户时长时间字段"
Cohesion: 1.0
Nodes (1): event_ts_expr (复用 ads_daily_content_performance 的时区口径)

### Community 28 - "社媒内容补采日期"
Cohesion: 1.0
Nodes (1): get_social_content_dates_to_process (独立重刷最近 N 天分区)

### Community 29 - "iOS 环境变量"
Cohesion: 1.0
Nodes (1): require_env(name)（读取必需环境变量）

### Community 30 - "iOS 表查询"
Cohesion: 1.0
Nodes (1): query_table(dataset_id, query)（BigQuery 查询封装）

### Community 31 - "Applog 属性"
Cohesion: 1.0
Nodes (1): Properties（埋点 properties 子结构）

### Community 32 - "Unix 时间解析"
Cohesion: 1.0
Nodes (1): get_unix_timestamp(file_path)（从 archived/YYYY-MM-DD/HH_MM... 推导时间戳）

### Community 33 - "BigQuery 插入"
Cohesion: 1.0
Nodes (1): insert_into_bigquery(client, rows_data, dataset_id, table_id)（通用批量写入）

### Community 34 - "日志路径校验"
Cohesion: 1.0
Nodes (1): validate_log_file_path(gcs_key)（校验 archived 路径格式）

### Community 35 - "GCS 时间解析"
Cohesion: 1.0
Nodes (1): parse_timestamp_from_gcs_key(gcs_key)（从路径解析毫秒时间戳）

### Community 36 - "小红书 Doris"
Cohesion: 1.0
Nodes (1): 小红书投放金额脚本（Doris/MySQL 写入版本）

### Community 37 - "小红书 BigQuery"
Cohesion: 1.0
Nodes (1): 小红书投放金额脚本（BigQuery 落表版本）

### Community 38 - "社媒视频日期"
Cohesion: 1.0
Nodes (1): get_social_video_dates_to_process (独立重刷最近 N 天分区)

### Community 39 - "社媒全量日期"
Cohesion: 1.0
Nodes (1): get_social_video_full_dates_to_process (全量回刷: 从最早发布时间生成日期序列)

### Community 40 - "视频日期计算"
Cohesion: 1.0
Nodes (1): get_video_dates_to_process (固定重刷最近 N 天分区)

### Community 41 - "Appsflyer 日期"
Cohesion: 1.0
Nodes (1): get_appsflyer_dates_to_process (AppsFlyer 重刷最近 N 天)

### Community 42 - "Hash ID 生成"
Cohesion: 1.0
Nodes (1): generate_hash_id (基于 oss_key+user+session+时间等的 md5 行键)

### Community 43 - "主题文档规范"
Cohesion: 1.0
Nodes (1): 主题文档规则（fact.md/patterns.md 结构与职责）

### Community 44 - "埋点参考文档"
Cohesion: 1.0
Nodes (1): 参考文件（数据埋点 CSV 版本）

## Ambiguous Edges - Review These
- `process_log_file(request)（Cloud Run HTTP 入口）` → `文件过滤规则（archived/ + .log + 路径晚于某个 key）`  [AMBIGUOUS]
  jobs/ods/ods_event_log/README.md · relation: conceptually_related_to
- `BigQuery 表：my-project-8584-jetonai.decom.ods_event_log（代码中写入目标）` → `BigQuery 表：decom.decom_track_log（README 中描述）`  [AMBIGUOUS]
  jobs/ods/ods_event_log/README.md · relation: conceptually_related_to

## Knowledge Gaps
- **166 isolated node(s):** `ETL 运行入口脚本（etl_run.py）`, `resolve_script_path(project_dir, script)`, `ETL 配置文件（env/etl_config.json）`, `BigQuery table decom.dws_download_daily`, `BigQuery table decom.ads_daily_total` (+161 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `专栏字段表达式`** (2 nodes): `clean_id_expr (清洗 args_* ID 字符串引号与 'null')`, `module_expr/column_id_expr (按事件与 args_* 推断 module/column_id)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `小红书 Token 刷新`** (2 nodes): `小红书广告 API：/api/open/oauth2/refresh_token`, `OAuth2 refresh_token 刷新并回写 token.json（脚本顶层流程）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `项目架构原则`** (2 nodes): `架构换性能原则`, `数据工程项目（Python + BigQuery + Superset）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `用户时长表`** (1 nodes): `BigQuery table decom.ads_daily_user_duration_frequency`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `商详试穿表`** (1 nodes): `BigQuery table decom.ads_daily_product_tryon_performance`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `帖子表现表`** (1 nodes): `BigQuery table decom.ads_daily_post_performance`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `用户时长时间字段`** (1 nodes): `event_ts_expr (复用 ads_daily_content_performance 的时区口径)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `社媒内容补采日期`** (1 nodes): `get_social_content_dates_to_process (独立重刷最近 N 天分区)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `iOS 环境变量`** (1 nodes): `require_env(name)（读取必需环境变量）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `iOS 表查询`** (1 nodes): `query_table(dataset_id, query)（BigQuery 查询封装）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Applog 属性`** (1 nodes): `Properties（埋点 properties 子结构）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Unix 时间解析`** (1 nodes): `get_unix_timestamp(file_path)（从 archived/YYYY-MM-DD/HH_MM... 推导时间戳）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `BigQuery 插入`** (1 nodes): `insert_into_bigquery(client, rows_data, dataset_id, table_id)（通用批量写入）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `日志路径校验`** (1 nodes): `validate_log_file_path(gcs_key)（校验 archived 路径格式）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `GCS 时间解析`** (1 nodes): `parse_timestamp_from_gcs_key(gcs_key)（从路径解析毫秒时间戳）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `小红书 Doris`** (1 nodes): `小红书投放金额脚本（Doris/MySQL 写入版本）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `小红书 BigQuery`** (1 nodes): `小红书投放金额脚本（BigQuery 落表版本）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `社媒视频日期`** (1 nodes): `get_social_video_dates_to_process (独立重刷最近 N 天分区)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `社媒全量日期`** (1 nodes): `get_social_video_full_dates_to_process (全量回刷: 从最早发布时间生成日期序列)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `视频日期计算`** (1 nodes): `get_video_dates_to_process (固定重刷最近 N 天分区)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Appsflyer 日期`** (1 nodes): `get_appsflyer_dates_to_process (AppsFlyer 重刷最近 N 天)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Hash ID 生成`** (1 nodes): `generate_hash_id (基于 oss_key+user+session+时间等的 md5 行键)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `主题文档规范`** (1 nodes): `主题文档规则（fact.md/patterns.md 结构与职责）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `埋点参考文档`** (1 nodes): `参考文件（数据埋点 CSV 版本）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `process_log_file(request)（Cloud Run HTTP 入口）` and `文件过滤规则（archived/ + .log + 路径晚于某个 key）`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `BigQuery 表：my-project-8584-jetonai.decom.ods_event_log（代码中写入目标）` and `BigQuery 表：decom.decom_track_log（README 中描述）`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **Why does `jobs/ads/ads_daily.py` connect `ADS 报表实现` to `试穿确认报表`?**
  _High betweenness centrality (0.036) - this node is a cross-community bridge._
- **Why does `Topic: 试穿结果转化` connect `ADS 报表实现` to `试穿与时长指标`?**
  _High betweenness centrality (0.036) - this node is a cross-community bridge._
- **Why does `run_ads_daily_user_duration_frequency (用户时长与访问频次/留存)` connect `试穿与时长指标` to `新增与全量日报`?**
  _High betweenness centrality (0.035) - this node is a cross-community bridge._
- **Are the 3 inferred relationships involving `dwd_event_log 事件主链路` (e.g. with `ODS Event Log Cloud Functions 依赖` and `dws_product_detail_daily 商详曝光中间层`) actually correct?**
  _`dwd_event_log 事件主链路` has 3 INFERRED edges - model-reasoned connections that need verification._
- **What connects `ETL 运行入口脚本（etl_run.py）`, `resolve_script_path(project_dir, script)`, `ETL 配置文件（env/etl_config.json）` to the rest of the system?**
  _166 weakly-connected nodes found - possible documentation gaps or missing edges._