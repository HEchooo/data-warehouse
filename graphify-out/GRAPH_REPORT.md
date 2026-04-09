# Graph Report - .  (2026-04-09)

## Corpus Check
- Corpus is ~27,739 words - fits in a single context window. You may not need a graph.

## Summary
- 509 nodes · 722 edges · 33 communities detected
- Extraction: 81% EXTRACTED · 18% INFERRED · 0% AMBIGUOUS · INFERRED: 132 edges (avg confidence: 0.55)
- Token cost: 0 input · 0 output

## God Nodes (most connected - your core abstractions)
1. `jobs/ads/ads_daily.py` - 24 edges
2. `run_ads_daily_post_performance()` - 14 edges
3. `run_dws_video_daily()` - 14 edges
4. `Topic: 穿搭工具点击使用` - 12 edges
5. `BigQuery Table: decom.dwd_event_log` - 12 edges
6. `run_dws_column_post_daily()` - 10 edges
7. `BigQuery table decom.dwd_event_log` - 10 edges
8. `Topic: 试穿结果转化` - 10 edges
9. `Topic: 首页模块曝光与点击明细` - 10 edges
10. `dws_daily 主编排 (__main__: 维表增量 + 多个 DWS 产出)` - 10 edges

## Surprising Connections (you probably didn't know these)
- `Rationale: 先确认是不是固定四行预期` --rationale_for--> `jobs/ads/ads_daily_home_module_performance.py`  [INFERRED]
  docs/topics/首页模块曝光与点击明细/patterns.md → jobs/ads/ads_daily_home_module_performance.py
- `首页模块事件映射: v_home_* / c_home_*` --semantically_similar_to--> `MODULE_MAP_SQL: module -> exposure_event/click_event 显式映射`  [EXTRACTED] [semantically similar]
  docs/topics/首页模块曝光与点击明细/fact.md → jobs/ads/ads_daily_home_module_performance.py
- `run_ads_daily_post_performance()` --references--> `Event: c_tryon`  [EXTRACTED]
  jobs/ads/ads_daily_post_performance.py → docs/topics/试穿结果转化/fact.md
- `run_ads_daily_tryon_confirm()` --references--> `Event: c_tryon_confirm`  [EXTRACTED]
  jobs/ads/ads_daily_tryon_confirm.py → docs/topics/穿搭工具点击使用/fact.md
- `文件过滤规则（archived/ + .log + 路径晚于某个 key）` --conceptually_related_to--> `process_log_file()`  [AMBIGUOUS]
  jobs/ods/ods_event_log/README.md → jobs/ods/ods_event_log/main.py

## Hyperedges (group relationships)
- **SSOT 路由与规范文档集合** — AGENTS_ssot_routing_table, CLAUDE_ssot_routing_table, code_rules_code_md, data_rules_data_md, env_rules_environment_md, pipeline_rules_pipeline_md, post_mapping_rules_post_mapping_md, references_rules_references_md, topics_rules_topics_md, video_mapping_rules_video_mapping_md, topics_index_topics_md [EXTRACTED 1.00]
- **Social Content 独立链路（DWS->ADS）** — table_decom_dws_social_video_daily, table_decom_ads_daily_social_content, script_jobs_dws_dws_social_video_daily_py, script_jobs_ads_ads_daily_social_content_py, concept_dense_daily_semantics, concept_interval_expand_generation [INFERRED 0.87]
- **首页模块曝光与点击明细: dwd_event_log -> ads_daily_home_module_performance -> dashboard 92** — 首页模块曝光与点击明细_fact_topic, dwd_event_log_table, ads_daily_home_module_performance_script, ads_daily_home_module_performance_table, ads_daily_home_module_performance_module_map_sql, concept_fixed_modules_star_magazine_brand_feeds, concept_home_module_event_mapping, 首页模块曝光与点击明细_fact_dashboard_92 [EXTRACTED 0.80]
- **穿搭工具点击使用: c_tryon_confirm + 维度映射 -> ads_daily_tryon_confirm / ads_daily_post_tryon_confirm -> dashboard 96** — 穿搭工具点击使用_fact_topic, event_c_tryon_confirm, dwd_event_log_table, v3_decom_community_post_table, ads_daily_tryon_confirm_table, ads_daily_post_tryon_confirm_table, 穿搭工具点击使用_fact_dashboard_96 [EXTRACTED 0.75]
- **试穿结果转化: c_tryon 与 c_add_cart -> ads_daily_tryon_add_cart_conversion -> dashboard 97** — 试穿结果转化_fact_topic, event_c_tryon, event_c_add_cart, dwd_event_log_table, ads_daily_tryon_add_cart_conversion_table, 试穿结果转化_fact_dashboard_97 [EXTRACTED 0.75]
- **下载指标链路 (ODS -> DWD -> DWS -> ADS)** — bq_decom_ods_android_download, dwd_download_run_dwd_download, bq_decom_dwd_download, dws_daily_run_dws_download_daily, bq_decom_dws_download_daily_target, ads_daily_new_run_ads_daily_new [INFERRED 0.85]
- **Social Content 链路 (视频源表 -> dws_social_video_daily -> ads_daily_social_content)** — bq_videos_yt_videos, bq_videos_videos, bq_videos_ig_media, dws_social_video_daily_run_dws_social_video_daily, bq_decom_dws_social_video_daily, ads_daily_social_content_run_ads_daily_social_content, bq_decom_ads_daily_social_content [EXTRACTED 1.00]
- **统一按 America/Toronto 切天 + 事件时间归一化模式** — concept_toronto_timezone_cutover, ads_daily_post_performance_event_ts_expr, ads_daily_user_duration_frequency_event_ts_expr, dws_daily_event_ts_expr [INFERRED 0.80]
- **Event Log Cloud Run 入仓流** — ods_event_log_readme_architecture_flow, main_process_log_file, main_get_log_entity_list_from_content, applog_decode_json, main_save_log_list, main_bigquery_table_decom_ods_event_log, main_save_oss_key_process_log, main_bigquery_table_decom_oss_key_process_log [INFERRED 0.85]
- **iOS Sales Report 入仓与分区替换流** — ios_script_ios_download, ios_script_create_temp_table, ios_script_build_ios_changed_table, ios_script_drop_and_rename_table, ios_script_bigquery_table_decom_ods_ios_download, ios_script_apple_sales_reports_api, ios_readme_table_ods_ios_download [INFERRED 0.83]
- **小红书离线报表拉取与落库流（BigQuery/Doris 两版本）** — xhs_bq_refresh_token_flow, xhs_adapi_refresh_token_endpoint, xhs_bq_get_offline_data, xhs_adapi_offline_report_endpoint, xhs_bq_table_mall_xiaohongshu_ad, xhs_doris_get_offline_data, xhs_doris_insert_xiaohongshu_ad [INFERRED 0.84]

## Communities

### Community 0 - "ADS 内容表现"
Cohesion: 0.05
Nodes (65): jobs/ads/ads_daily_column_performance.py, BigQuery table decom.ads_daily_column_performance, jobs/ads/ads_daily_content_performance.py, BigQuery table decom.ads_daily_content_performance, ddl/ads/ads_daily_home_module_performance.sql, MODULE_MAP_SQL: module -> exposure_event/click_event 显式映射, jobs/ads/ads_daily_home_module_performance.py, BigQuery table decom.ads_daily_home_module_performance (+57 more)

### Community 1 - "规则与主题文档"
Cohesion: 0.06
Nodes (50): SSOT 路由表（项目知识入口）, SSOT 路由表（CLAUDE 版）, 代码规则（DDL/更新数据/路径/清理/可读性）, ADS 日报统一编排入口：jobs/ads/ads_daily.py, 停留时长口径：仅 app_launch 的 args_session_duration 去重求和, ID 清洗：column_id 双引号/占位值导致映射为空的排查点, 去重优先级：raw_event_id 优先，缺失回退 hash_id, Dense daily 语义：从 published_dt 起每天一行，未采集日沿用最近累计值 (+42 more)

### Community 2 - "埋点日志解析"
Cohesion: 0.05
Nodes (36): AppTrackLogEntity, decode_json(), Properties, 解析JSON字符串为AppTrackLogEntity对象, BigQuery 表：my-project-8584-jetonai.decom.ods_event_log（代码中写入目标）, BigQuery 表：my-project-8584-jetonai.decom.oss_key_process_log（代码中写入目标）, get_log_entity_list_from_content(), MockRequest (+28 more)

### Community 3 - "内容与试穿报表"
Cohesion: 0.07
Nodes (36): build_ads_daily_content_performance_select_query(), dates_to_sql_list(), event_ts_expr(), platform_expr(), 平台整体内容表现日报（每日每平台一行）:     - platform: 设备类型（iOS / Android / H5 / unknown）     - pl, 将 logAt_timestamp 先按 prop_timezone 解释为真实时间，再转成 UTC TIMESTAMP。     logAt_timestam, run_ads_daily_content_performance(), string_values_to_sql_list() (+28 more)

### Community 4 - "社媒视频日报"
Cohesion: 0.07
Nodes (38): build_ads_daily_social_content_select_sql(), dates_to_array_sql(), dates_to_sql_list(), get_social_content_dates_to_process(), get_social_content_full_dates_to_process(), Social Content 主题日报（dt × 展示渠道）:     - 先刷新 dws_social_video_daily，统一承接视频补日期、补累计、算, Social Content 只依赖视频源表，独立重刷最近 N 天分区。     这样不会受事件 ADS 主链路调度与失败状态影响。, 全量回刷时，从三端视频最早发布时间开始生成完整日期序列。 (+30 more)

### Community 5 - "总量与总览日报"
Cohesion: 0.08
Nodes (38): dates_to_sql_list(), 总量报表:     - device_count: 活跃设备数     - user_count: 活跃注册用户数     - avg_duration_sec, run_ads_daily_total(), 穿搭工具“点击使用”日汇总（每日一行）:     - click_use_pv: 点击使用次数（PV，按 raw_event_id 去重）     - clic, run_ads_daily_tryon_confirm(), BigQuery Table: decom.ads_daily_total, BigQuery Table: decom.ads_daily_tryon_confirm, BigQuery Table: decom.dim_device_first_active (+30 more)

### Community 6 - "事件明细建模"
Cohesion: 0.09
Nodes (25): BigQuery Table: decom.ods_event_log, BigQuery Table: decom.oss_key_process_log, args_spu/args_post 独立展开策略 (spu 行与 post 行分开), _base62_decode(), generate_hash_id(), generate_raw_event_id(), get_dataset_for_table(), invite_code_to_user_id() (+17 more)

### Community 7 - "新增与帖子日报"
Cohesion: 0.12
Nodes (20): dates_to_sql_list(), 新增报表:     - device_count: 新增设备数 (is_new_device = TRUE)     - user_count: 新增注册用户数, run_ads_daily_new(), clean_id_expr(), column_id_expr(), dates_to_sql_list(), event_ts_expr(), 关注事件 last-touch 归因到帖子详情曝光 (c_follow -> 最后一条 exposure) (+12 more)

### Community 8 - "AppsFlyer 下载"
Cohesion: 0.16
Nodes (18): BigQuery Table: decom.dws_appsflyer_download_daily, BigQuery Table: decom.ods_appsflyer_download, dates_to_sql_list(), 聚合投资人报表使用的 AppsFlyer 新增下载。     当前仅保留按天总量，不拆平台。, run_dws_appsflyer_download_daily(), build_changed_table(), 配置: env/appsflyer.json (token + app ids), create_temp_table() (+10 more)

### Community 9 - "下载明细建模"
Cohesion: 0.14
Nodes (16): BigQuery Table: decom.ods_android_download, BigQuery Table: decom.ods_ios_download, get_dates_to_process(), 获取需要刷新的下载日期（近 N 天，T+1）。     下载来源:     - ods_ios_download.begin_date     - ods_an, 统一下载明细口径到 dwd_download:     - iOS: product_type_identifier = 1 的 units     - And, run_dwd_download(), android_download(), build_android_changed_table() (+8 more)

### Community 10 - "BI 交接运维"
Cohesion: 0.12
Nodes (18): BigQuery 使用经验（计费按扫描量，建议 Partition/Cluster）, BigQuery 字段变更流程（临时表 -> 快照 -> drop 原表 -> rename）, Cloud Run：decom-track-log-runner（Decom 埋点日志函数）, Cloud Run：mall-track-log-runner（AC 埋点日志函数）, GCP Datastream 同步与回填操作（Initiate backfill, 配置同步表）, BI 数据交接文档, GCP 角色清单（Artifact Registry/BigQuery/Cloud Run/Eventarc/Datastream 等）, GCS Bucket：decom-prod-client-log（Decom 埋点日志） (+10 more)

### Community 11 - "投资人飞书推送"
Cohesion: 0.25
Nodes (15): build_card_payload(), fetch_investor_row(), format_decimal(), format_duration_seconds(), format_int(), format_percent(), format_rounded_int(), get_runtime_mode() (+7 more)

### Community 12 - "ETL 运行器"
Cohesion: 0.38
Nodes (11): fail(), load_config(), log_line(), log_section(), main(), must_non_empty_str(), now_clock(), now_text() (+3 more)

### Community 13 - "专栏内容日报"
Cohesion: 0.27
Nodes (11): clean_id_expr(), column_id_expr(), dates_to_sql_list(), event_ts_expr(), module_expr(), 将 logAt_timestamp 先按 prop_timezone 解释为真实时间，再转成 UTC TIMESTAMP。     logAt_timestam, v3 服务表的 gmt_create 是 UTC DATETIME，这里统一转成 TIMESTAMP。, dwd_event_log 的 args_* 有时来自 JSON_EXTRACT，可能携带首尾双引号：     例如：'"2602276189664"'。这里统 (+3 more)

### Community 14 - "专栏帖子映射"
Cohesion: 0.2
Nodes (11): BigQuery Table: decom.dws_column_post_daily, BigQuery Table: v3_decom.common_ext_ai_task, BigQuery Table: v3_decom.kol_rel, dates_to_sql_list(), get_column_post_dates_to_process(), v3 服务表的 gmt_create 是 UTC DATETIME，这里统一转成 TIMESTAMP。, 只补昨日快照。     主 ETL 可能每小时触发，但帖子快照语义是“昨日固化一次”，     因此只在目标表缺少昨日分区时返回该日期。, 产出专栏维度的服务端帖子日事实表。      粒度：     - dt × module × column_id      指标：     - post_tot (+3 more)

### Community 15 - "用户时长频次"
Cohesion: 0.31
Nodes (8): dates_to_array_sql(), dates_to_sql_list(), event_ts_expr(), 将 logAt_timestamp 先按 prop_timezone 解释为真实时间，再转成 UTC TIMESTAMP。     logAt_timestam, 用户时长与访问频次（日粒度，仅全量一行）:     - dau_uv: 日活 UV（visitor_id）     - total_duration_min:, run_ads_daily_user_duration_frequency(), BigQuery Table: decom.ads_daily_user_duration_frequency, 概念: action_event_id=COALESCE(raw_event_id,hash_id) 去重以避免 args 展开放大

### Community 16 - "iOS 下载入仓"
Cohesion: 0.36
Nodes (7): build_ios_changed_table(), create_temp_table(), drop_and_rename_table(), drop_table_if_exists(), ios_download(), query_table(), 执行查询，支持不同数据集     :param dataset_id: 数据集ID (mall 或 decom)     :param query: SQL 查

### Community 17 - "iOS 下载文档"
Cohesion: 0.29
Nodes (7): 引用：Apple Developer - Financial report fields, 引用：Apple Developer - Product type identifiers, 引用：Apple Developer - Sales and Trends metrics, 指标口径：新增下载量（仅 product_type_identifier=1）, 枚举：product_type_identifier（下载/重装/更新/IAP 等分类）, BigQuery 表 ods_ios_download（字段与含义说明）, 时区说明：报告基于太平洋时间 PT

### Community 18 - "运行环境配置"
Cohesion: 0.33
Nodes (6): PATH_MODE_OVERRIDE（remote/local 运行模式覆盖）, ETL 配置文件（env/etl_config.json）, load_config(config_path), main(), resolve_script_path(project_dir, script), run_etl(config, config_path)

### Community 19 - "小红书成本同步"
Cohesion: 0.5
Nodes (5): 小红书广告 API：离线报表 /api/open/jg/data/report/offline/account, get_offline_data(filter_date)（离线报表拉取并写入 BigQuery）, BigQuery 表：my-project-8584-jetonai.mall.xiaohongshu_ad, get_offline_data(filter_date)（离线报表拉取并写入 Doris）, Doris 表写入：INSERT INTO xiaohongshu_ad VALUES (...)

### Community 20 - "小红书查询工具"
Cohesion: 0.5
Nodes (2): query_table(), 执行查询，支持不同数据集     :param dataset_id: 数据集ID (mall 或 decom)     :param query: SQL 查

### Community 21 - "Android 下载文档"
Cohesion: 0.5
Nodes (4): 概念: Device vs User 统计口径区别, README: Android 下载量数据 (Google Play Console CSV), 字段定义: date/country/daily_device_installs 等, 数据源: GCS installs CSV (UTF-16, installs_{package}_{YYYYMM}_country.csv)

### Community 22 - "小红书 Doris 同步"
Cohesion: 1.0
Nodes (0): 

### Community 23 - "小红书令牌刷新"
Cohesion: 1.0
Nodes (2): 小红书广告 API：/api/open/oauth2/refresh_token, OAuth2 refresh_token 刷新并回写 token.json（脚本顶层流程）

### Community 24 - "Post Column 映射"
Cohesion: 1.0
Nodes (1): magazine/star/brand 与 post/column 映射关系

### Community 25 - "埋点参考文档"
Cohesion: 1.0
Nodes (1): 参考文件（数据埋点 CSV 版本）

### Community 26 - "用户时长表引用"
Cohesion: 1.0
Nodes (1): BigQuery table decom.ads_daily_user_duration_frequency

### Community 27 - "试穿表现表引用"
Cohesion: 1.0
Nodes (1): BigQuery table decom.ads_daily_product_tryon_performance

### Community 28 - "帖子表现表引用"
Cohesion: 1.0
Nodes (1): BigQuery table decom.ads_daily_post_performance

### Community 29 - "环境变量读取"
Cohesion: 1.0
Nodes (1): require_env(name)（读取必需环境变量）

### Community 30 - "BigQuery 查询封装"
Cohesion: 1.0
Nodes (1): query_table(dataset_id, query)（BigQuery 查询封装）

### Community 31 - "小红书 BigQuery 入口"
Cohesion: 1.0
Nodes (1): 小红书投放金额脚本（BigQuery 落表版本）

### Community 32 - "小红书 Doris 入口"
Cohesion: 1.0
Nodes (1): 小红书投放金额脚本（Doris/MySQL 写入版本）

## Ambiguous Edges - Review These
- `process_log_file()` → `文件过滤规则（archived/ + .log + 路径晚于某个 key）`  [AMBIGUOUS]
  jobs/ods/ods_event_log/README.md · relation: conceptually_related_to
- `BigQuery 表：decom.decom_track_log（README 中描述）` → `BigQuery 表：my-project-8584-jetonai.decom.ods_event_log（代码中写入目标）`  [AMBIGUOUS]
  jobs/ods/ods_event_log/README.md · relation: conceptually_related_to

## Knowledge Gaps
- **181 isolated node(s):** `找出 DWS 层有新数据但 ADS 层尚未处理的日期。     同时包含前一天用于回刷次日留存率。     同时纳入下载 DWS 与内容表现日报的增量日期。`, `新增报表:     - device_count: 新增设备数 (is_new_device = TRUE)     - user_count: 新增注册用户数`, `将 logAt_timestamp 先按 prop_timezone 解释为真实时间，再转成 UTC TIMESTAMP。     logAt_timestam`, `dwd_event_log 的 args_* 有时来自 JSON_EXTRACT，可能携带首尾双引号：     例如：'"2602276189664"'。这里统`, `内容维度日报（日×帖子）:     粒度：dt × post × module × column      指标：     - post_exposure_uv` (+176 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `小红书 Doris 同步`** (2 nodes): `xiaohongshu_cost_data.py`, `get_offline_data()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `小红书令牌刷新`** (2 nodes): `小红书广告 API：/api/open/oauth2/refresh_token`, `OAuth2 refresh_token 刷新并回写 token.json（脚本顶层流程）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Post Column 映射`** (1 nodes): `magazine/star/brand 与 post/column 映射关系`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `埋点参考文档`** (1 nodes): `参考文件（数据埋点 CSV 版本）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `用户时长表引用`** (1 nodes): `BigQuery table decom.ads_daily_user_duration_frequency`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `试穿表现表引用`** (1 nodes): `BigQuery table decom.ads_daily_product_tryon_performance`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `帖子表现表引用`** (1 nodes): `BigQuery table decom.ads_daily_post_performance`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `环境变量读取`** (1 nodes): `require_env(name)（读取必需环境变量）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `BigQuery 查询封装`** (1 nodes): `query_table(dataset_id, query)（BigQuery 查询封装）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `小红书 BigQuery 入口`** (1 nodes): `小红书投放金额脚本（BigQuery 落表版本）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `小红书 Doris 入口`** (1 nodes): `小红书投放金额脚本（Doris/MySQL 写入版本）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `process_log_file()` and `文件过滤规则（archived/ + .log + 路径晚于某个 key）`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `BigQuery 表：decom.decom_track_log（README 中描述）` and `BigQuery 表：my-project-8584-jetonai.decom.ods_event_log（代码中写入目标）`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **Why does `BigQuery Table: decom.dwd_event_log` connect `总量与总览日报` to `内容与试穿报表`, `用户时长频次`, `新增与帖子日报`?**
  _High betweenness centrality (0.098) - this node is a cross-community bridge._
- **Why does `dws_daily 主编排 (__main__: 维表增量 + 多个 DWS 产出)` connect `总量与总览日报` to `AppsFlyer 下载`, `社媒视频日报`, `专栏帖子映射`?**
  _High betweenness centrality (0.085) - this node is a cross-community bridge._
- **Why does `run_dws_video_daily()` connect `社媒视频日报` to `总量与总览日报`, `新增与帖子日报`?**
  _High betweenness centrality (0.082) - this node is a cross-community bridge._
- **Are the 4 inferred relationships involving `run_ads_daily_post_performance()` (e.g. with `dates_to_sql_list()` and `event_ts_expr()`) actually correct?**
  _`run_ads_daily_post_performance()` has 4 INFERRED edges - model-reasoned connections that need verification._
- **Are the 2 inferred relationships involving `run_dws_video_daily()` (e.g. with `dates_to_sql_list()` and `strings_to_sql_list()`) actually correct?**
  _`run_dws_video_daily()` has 2 INFERRED edges - model-reasoned connections that need verification._