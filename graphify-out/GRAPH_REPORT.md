# Graph Report - .  (2026-04-10)

## Corpus Check
- 32 files · ~30,647 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 200 nodes · 220 edges · 38 communities detected
- Extraction: 89% EXTRACTED · 11% INFERRED · 0% AMBIGUOUS · INFERRED: 24 edges (avg confidence: 0.64)
- Token cost: 0 input · 0 output

## God Nodes (most connected - your core abstractions)
1. `jobs/ads/ads_daily.py` - 13 edges
2. `event_ts_expr()` - 9 edges
3. `run_dws_video_daily (视频播放快照日表: 补采/填充/日增)` - 9 edges
4. `run_ads_daily_post_performance (内容日帖表现日报)` - 8 edges
5. `run_ads_daily_column_performance()` - 7 edges
6. `build_card_payload()` - 7 edges
7. `main()` - 7 edges
8. `BigQuery Table: decom.dwd_event_log` - 6 edges
9. `run_ads_daily_product_tryon_performance (商详页试穿: dt×SPU×SKU)` - 6 edges
10. `ios_download(start_timestamp, end_timestamp)（下载并写入销售报告）` - 6 edges

## Surprising Connections (you probably didn't know these)
- `save_log_list(log_list, bigquery_client, log_day, ctime, gcs_key)（写入 BigQuery）` --semantically_similar_to--> `create_bigquery_row(log_entity, log_day, ctime, gcs_key)（构建 BigQuery 行数据）`  [INFERRED] [semantically similar]
  jobs/ods/ods_event_log/main.py → jobs/ods/ods_event_log/utils.py
- `event_ts_expr (按 prop_timezone 解释 logAt_timestamp 再转 UTC)` --conceptually_related_to--> `概念: 指标按 America/Toronto 切天`  [INFERRED]
  jobs/ads/ads_daily_post_performance.py → jobs/ads/ads_daily_total.py
- `ODS 分区替换: staging 表 + changed 表 + drop+rename (按 min_date~max_date 替换)` --semantically_similar_to--> `ODS 分区替换: staging 表 + changed 表 + drop+rename (dt 分区, cluster by platform/app_id/media_source)`  [INFERRED] [semantically similar]
  jobs/ods/ods_android_download/ods_android_download.py → jobs/ods/ods_appsflyer_download/ods_appsflyer_download.py
- `save_log_list(log_list, bigquery_client, log_day, ctime, gcs_key)（写入 BigQuery）` --shares_data_with--> `AppTrackLogEntity（埋点日志实体）`  [INFERRED]
  jobs/ods/ods_event_log/main.py → jobs/ods/ods_event_log/applog.py
- `get_offline_data(filter_date)（离线报表拉取并写入 BigQuery）` --semantically_similar_to--> `get_offline_data(filter_date)（离线报表拉取并写入 Doris）`  [INFERRED] [semantically similar]
  jobs/ods/ods_xiaohongshu_cost/bigquery_xiaohongshu_cost_data.py → jobs/ods/ods_xiaohongshu_cost/xiaohongshu_cost_data.py

## Communities

### Community 0 - "Community 0"
Cohesion: 0.11
Nodes (24): 关注事件 last-touch 归因到帖子详情曝光 (c_follow -> 最后一条 exposure), run_ads_daily_post_performance (内容日帖表现日报), run_ads_daily_post_tryon_confirm (穿搭工具点击使用: 日×帖子), clean_sku_expr (清洗 args_sku 多层引号/反斜杠与 'null'), run_ads_daily_product_tryon_performance (商详页试穿: dt×SPU×SKU), run_ads_daily_tryon_add_cart_conversion (试穿结果转化日报: tryon_uv/add_cart_uv), run_ads_daily_tryon_confirm (穿搭工具点击使用: 每日一行), run_ads_daily_user_duration_frequency (用户时长与访问频次/留存) (+16 more)

### Community 1 - "Community 1"
Cohesion: 0.13
Nodes (20): event_ts_expr(), get_dates_to_process(), get_download_dates_to_process(), merge_dim_device_first_active(), merge_dim_user_first_active(), 找出下载 DWD 层有新数据但 DWS 下载层尚未处理的日期。     使用多伦多时间（America/Toronto），按 T+1 处理。, 增量维护 dim_device_first_active 维度表。     只扫描本批次日期分区，对新设备 INSERT，对已有设备取 LEAST。     使, 增量维护 dim_user_first_active 维度表。     只扫描本批次日期分区，对新用户 INSERT，对已有用户取 LEAST。     使用多 (+12 more)

### Community 2 - "Community 2"
Cohesion: 0.17
Nodes (16): jobs/ads/ads_daily_content_performance.py, BigQuery table decom.ads_daily_content_performance, MODULE_MAP_SQL: module -> exposure_event/click_event 显式映射, jobs/ads/ads_daily_home_module_performance.py, BigQuery table decom.ads_daily_home_module_performance, BigQuery table decom.ads_daily_new, BigQuery table decom.ads_daily_post_tryon_confirm, jobs/ads/ads_daily.py (+8 more)

### Community 3 - "Community 3"
Cohesion: 0.25
Nodes (15): build_card_payload(), fetch_investor_row(), format_decimal(), format_duration_seconds(), format_int(), format_percent(), format_rounded_int(), get_runtime_mode() (+7 more)

### Community 4 - "Community 4"
Cohesion: 0.17
Nodes (16): get_social_content_full_dates_to_process (按最早发布时间生成全量日期序列), run_ads_daily_social_content (社媒主题日报: dt×展示渠道), ADS 分区替换: staging 表 + changed 表 + drop+rename + 行数校验, BigQuery Table: decom.ads_daily_social_content, BigQuery Table: decom.dws_social_video_daily, BigQuery Table: decom.dws_video_daily, BigQuery Table: videos.ig_media, BigQuery Table: videos.ig_media_stats_log (+8 more)

### Community 5 - "Community 5"
Cohesion: 0.17
Nodes (13): BigQuery Table: decom.dws_appsflyer_download_daily, BigQuery Table: decom.ods_android_download, BigQuery Table: decom.ods_appsflyer_download, BigQuery Table: decom.ods_ios_download, get_dates_to_process (下载待刷日期: 近 N 天 T+1), run_dwd_download (统一下载明细口径: iOS+Android), run_dws_appsflyer_download_daily (AppsFlyer 下载按天汇总), android_download (GCS 读取 UTF-16 CSV 并分区替换写入 ODS) (+5 more)

### Community 6 - "Community 6"
Cohesion: 0.27
Nodes (11): clean_id_expr(), column_id_expr(), dates_to_sql_list(), event_ts_expr(), module_expr(), 将 logAt_timestamp 先按 prop_timezone 解释为真实时间，再转成 UTC TIMESTAMP。     logAt_timestam, v3 服务表的 gmt_create 是 UTC DATETIME，这里统一转成 TIMESTAMP。, dwd_event_log 的 args_* 有时来自 JSON_EXTRACT，可能携带首尾双引号：     例如：'"2602276189664"'。这里统 (+3 more)

### Community 7 - "Community 7"
Cohesion: 0.2
Nodes (11): AppTrackLogEntity（埋点日志实体）, decode_json(json_str, file_path)（JSON -> AppTrackLogEntity）, BigQuery 表：my-project-8584-jetonai.decom.ods_event_log（代码中写入目标）, BigQuery 表：my-project-8584-jetonai.decom.oss_key_process_log（代码中写入目标）, get_log_entity_list_from_content(file_content, gcs_key)（逐行解析日志）, normalize_numeric_for_bigquery(value)（NUMERIC 规范化，最多 9 位小数）, parse_log_day_from_gcs_key(gcs_key)（从路径提取 YYYY-MM-DD）, process_log_file(request)（Cloud Run HTTP 入口） (+3 more)

### Community 8 - "Community 8"
Cohesion: 0.28
Nodes (9): run_ads_daily_new (ADS 新增报表构建), event_ts_expr (按 prop_timezone 解释 logAt_timestamp 再转 UTC), run_ads_daily_total (ADS 总量报表构建), BigQuery Table: decom.ads_daily_new, BigQuery Table: decom.ads_daily_total, BigQuery Table: decom.dws_device_daily, BigQuery Table: decom.dws_user_daily, 概念: 指标按 America/Toronto 切天 (+1 more)

### Community 9 - "Community 9"
Cohesion: 0.33
Nodes (7): Apple App Store Connect Sales Reports API（/v1/salesReports）, BigQuery 表：my-project-8584-jetonai.decom.ods_ios_download（代码写入目标）, build_ios_changed_table(target, temp, changed, begin_date_str)（分区替换：UNION ALL）, create_temp_table(target_table_id, temp_table_id)（构建空结构临时表）, drop_and_rename_table(source_table_id, target_table_id)（drop 目标后 rename）, drop_table_if_exists(table_id)（DROP TABLE IF EXISTS）, ios_download(start_timestamp, end_timestamp)（下载并写入销售报告）

### Community 10 - "Community 10"
Cohesion: 0.53
Nodes (5): dates_to_array_sql(), dates_to_sql_list(), 投资人主题日报（dt 一行）:     - 视频侧指标基于 dws_video_daily，只保留 jhonson 团队视频     - 新增视频平均播放数按新, run_ads_daily_investor(), strings_to_sql_list()

### Community 11 - "Community 11"
Cohesion: 0.4
Nodes (5): dates_to_sql_list(), get_column_post_dates_to_process(), 只补昨日快照。     主 ETL 可能每小时触发，但帖子快照语义是“昨日固化一次”，     因此只在目标表缺少昨日分区时返回该日期。, 产出专栏维度的在线帖子日快照。      粒度：     - dt × module × column_id      说明：     - 该表只承载“昨日 0, run_dws_column_post_daily()

### Community 12 - "Community 12"
Cohesion: 0.33
Nodes (6): BigQuery Table: decom.ods_event_log, args_spu/args_post 独立展开策略 (spu 行与 post 行分开), generate_raw_event_id (基于 row payload 的 md5 事件键), invite_code_to_user_id (邀请码解码为 invite_user_id), transform_data (展开 args_spu/args_post 并生成 dwd_event_log 行), invite_code_to_user_id (邀请码解码逻辑抽离版)

### Community 13 - "Community 13"
Cohesion: 0.4
Nodes (5): ETL 配置文件（env/etl_config.json）, load_config(config_path), main(), resolve_script_path(project_dir, script), run_etl(config, config_path)

### Community 14 - "Community 14"
Cohesion: 0.5
Nodes (5): 小红书广告 API：离线报表 /api/open/jg/data/report/offline/account, get_offline_data(filter_date)（离线报表拉取并写入 BigQuery）, BigQuery 表：my-project-8584-jetonai.mall.xiaohongshu_ad, get_offline_data(filter_date)（离线报表拉取并写入 Doris）, Doris 表写入：INSERT INTO xiaohongshu_ad VALUES (...)

### Community 15 - "Community 15"
Cohesion: 0.67
Nodes (3): BigQuery Table: decom.oss_key_process_log, safe_update_oss_key_process_log (通过重建表更新 analyze_state), Rationale: 用重建表方式更新 oss_key_process_log 以规避 Streaming Buffer 限制

### Community 16 - "Community 16"
Cohesion: 1.0
Nodes (2): clean_id_expr (清洗 args_* ID 字符串引号与 'null'), module_expr/column_id_expr (按事件与 args_* 推断 module/column_id)

### Community 17 - "Community 17"
Cohesion: 1.0
Nodes (2): 小红书广告 API：/api/open/oauth2/refresh_token, OAuth2 refresh_token 刷新并回写 token.json（脚本顶层流程）

### Community 18 - "Community 18"
Cohesion: 1.0
Nodes (1): ETL 运行入口脚本（etl_run.py）

### Community 19 - "Community 19"
Cohesion: 1.0
Nodes (1): BigQuery table decom.ads_daily_user_duration_frequency

### Community 20 - "Community 20"
Cohesion: 1.0
Nodes (1): BigQuery table decom.ads_daily_product_tryon_performance

### Community 21 - "Community 21"
Cohesion: 1.0
Nodes (1): BigQuery table decom.ads_daily_post_performance

### Community 22 - "Community 22"
Cohesion: 1.0
Nodes (1): event_ts_expr (复用 ads_daily_content_performance 的时区口径)

### Community 23 - "Community 23"
Cohesion: 1.0
Nodes (1): get_social_content_dates_to_process (独立重刷最近 N 天分区)

### Community 24 - "Community 24"
Cohesion: 1.0
Nodes (1): require_env(name)（读取必需环境变量）

### Community 25 - "Community 25"
Cohesion: 1.0
Nodes (1): query_table(dataset_id, query)（BigQuery 查询封装）

### Community 26 - "Community 26"
Cohesion: 1.0
Nodes (1): Properties（埋点 properties 子结构）

### Community 27 - "Community 27"
Cohesion: 1.0
Nodes (1): get_unix_timestamp(file_path)（从 archived/YYYY-MM-DD/HH_MM... 推导时间戳）

### Community 28 - "Community 28"
Cohesion: 1.0
Nodes (1): insert_into_bigquery(client, rows_data, dataset_id, table_id)（通用批量写入）

### Community 29 - "Community 29"
Cohesion: 1.0
Nodes (1): validate_log_file_path(gcs_key)（校验 archived 路径格式）

### Community 30 - "Community 30"
Cohesion: 1.0
Nodes (1): parse_timestamp_from_gcs_key(gcs_key)（从路径解析毫秒时间戳）

### Community 31 - "Community 31"
Cohesion: 1.0
Nodes (1): 小红书投放金额脚本（Doris/MySQL 写入版本）

### Community 32 - "Community 32"
Cohesion: 1.0
Nodes (1): 小红书投放金额脚本（BigQuery 落表版本）

### Community 33 - "Community 33"
Cohesion: 1.0
Nodes (1): get_social_video_dates_to_process (独立重刷最近 N 天分区)

### Community 34 - "Community 34"
Cohesion: 1.0
Nodes (1): get_social_video_full_dates_to_process (全量回刷: 从最早发布时间生成日期序列)

### Community 35 - "Community 35"
Cohesion: 1.0
Nodes (1): get_video_dates_to_process (固定重刷最近 N 天分区)

### Community 36 - "Community 36"
Cohesion: 1.0
Nodes (1): get_appsflyer_dates_to_process (AppsFlyer 重刷最近 N 天)

### Community 37 - "Community 37"
Cohesion: 1.0
Nodes (1): generate_hash_id (基于 oss_key+user+session+时间等的 md5 行键)

## Knowledge Gaps
- **93 isolated node(s):** `ETL 运行入口脚本（etl_run.py）`, `resolve_script_path(project_dir, script)`, `ETL 配置文件（env/etl_config.json）`, `BigQuery table decom.dws_device_daily`, `BigQuery table decom.dws_download_daily` (+88 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 16`** (2 nodes): `clean_id_expr (清洗 args_* ID 字符串引号与 'null')`, `module_expr/column_id_expr (按事件与 args_* 推断 module/column_id)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 17`** (2 nodes): `小红书广告 API：/api/open/oauth2/refresh_token`, `OAuth2 refresh_token 刷新并回写 token.json（脚本顶层流程）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 18`** (1 nodes): `ETL 运行入口脚本（etl_run.py）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 19`** (1 nodes): `BigQuery table decom.ads_daily_user_duration_frequency`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 20`** (1 nodes): `BigQuery table decom.ads_daily_product_tryon_performance`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 21`** (1 nodes): `BigQuery table decom.ads_daily_post_performance`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 22`** (1 nodes): `event_ts_expr (复用 ads_daily_content_performance 的时区口径)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 23`** (1 nodes): `get_social_content_dates_to_process (独立重刷最近 N 天分区)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 24`** (1 nodes): `require_env(name)（读取必需环境变量）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 25`** (1 nodes): `query_table(dataset_id, query)（BigQuery 查询封装）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 26`** (1 nodes): `Properties（埋点 properties 子结构）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 27`** (1 nodes): `get_unix_timestamp(file_path)（从 archived/YYYY-MM-DD/HH_MM... 推导时间戳）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 28`** (1 nodes): `insert_into_bigquery(client, rows_data, dataset_id, table_id)（通用批量写入）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 29`** (1 nodes): `validate_log_file_path(gcs_key)（校验 archived 路径格式）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 30`** (1 nodes): `parse_timestamp_from_gcs_key(gcs_key)（从路径解析毫秒时间戳）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 31`** (1 nodes): `小红书投放金额脚本（Doris/MySQL 写入版本）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 32`** (1 nodes): `小红书投放金额脚本（BigQuery 落表版本）`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 33`** (1 nodes): `get_social_video_dates_to_process (独立重刷最近 N 天分区)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 34`** (1 nodes): `get_social_video_full_dates_to_process (全量回刷: 从最早发布时间生成日期序列)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 35`** (1 nodes): `get_video_dates_to_process (固定重刷最近 N 天分区)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 36`** (1 nodes): `get_appsflyer_dates_to_process (AppsFlyer 重刷最近 N 天)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 37`** (1 nodes): `generate_hash_id (基于 oss_key+user+session+时间等的 md5 行键)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `概念: 指标按 America/Toronto 切天` connect `Community 8` to `Community 0`, `Community 4`?**
  _High betweenness centrality (0.036) - this node is a cross-community bridge._
- **Why does `run_ads_daily_user_duration_frequency (用户时长与访问频次/留存)` connect `Community 0` to `Community 8`?**
  _High betweenness centrality (0.031) - this node is a cross-community bridge._
- **Are the 7 inferred relationships involving `event_ts_expr()` (e.g. with `get_dates_to_process()` and `merge_dim_device_first_active()`) actually correct?**
  _`event_ts_expr()` has 7 INFERRED edges - model-reasoned connections that need verification._
- **Are the 5 inferred relationships involving `run_ads_daily_column_performance()` (e.g. with `dates_to_sql_list()` and `event_ts_expr()`) actually correct?**
  _`run_ads_daily_column_performance()` has 5 INFERRED edges - model-reasoned connections that need verification._
- **What connects `ETL 运行入口脚本（etl_run.py）`, `resolve_script_path(project_dir, script)`, `ETL 配置文件（env/etl_config.json）` to the rest of the system?**
  _93 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Community 0` be split into smaller, more focused modules?**
  _Cohesion score 0.11 - nodes in this community are weakly interconnected._
- **Should `Community 1` be split into smaller, more focused modules?**
  _Cohesion score 0.13 - nodes in this community are weakly interconnected._