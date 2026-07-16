# Pipeline 规则

## ADS 日报编排

### 目标

- 全部 ADS 日报都走独立表级入口
- 共享 `ADS` 旧入口已退出主链路，不再作为编排入口
- 每张 `ADS` 表自己按本表上游判定待处理范围，不共享整层日期入口

独立表级入口包括：
- `ads_daily_new`
- `ads_daily_total`
- `ads_daily_investor`
- `ads_daily_post_performance`
- `ads_daily_column_performance`
- `ads_monthly_column_performance`
- `ads_monthly_module_performance`
- `ads_daily_content_performance`
- `ads_daily_home_module_performance`
- `ads_daily_thirdapp_channel_uv`
- `ads_daily_tryon_confirm`
- `ads_daily_post_tryon_confirm`
- `ads_daily_tryon_add_cart_conversion`
- `ads_daily_product_tryon_performance`
- `ads_daily_user_duration_frequency`
- `ads_daily_retention_user_feature`
- `ads_daily_retention_user_duration_bucket`
- `ads_daily_retention_user_follow_bucket`
- `ads_daily_retention_user_new_follow_bucket`
- `ads_daily_retention_user_tryon_bucket`
- `ads_daily_retention_user_product_detail_bucket`
- `ads_daily_social_content_v2`
- `ads_daily_social_content_attribute_v2`
- `ads_daily_social_video_monitor_rankings_v2`
- `ads_daily_social_creator_suspected_throttle_rankings`
- `ads_daily_formal_video_increment`
- `ads_weekly_formal_video_increment_growth`
- `ads_weekly_ai_fashion_tryon_okr`
- `ads_daily_ai_fashion_tryon_okr_v2`
- `ads_daily_closet_overview`
- `ads_daily_video_code_conversion`
- `ads_daily_push`
- `ads_daily_push_recall`
- `ads_daily_makepod`
- `ads_daily_ai_fashion_styleme`

### 执行流程

独立表级入口：
- `jobs/ads/ads_daily_new.py`
- `jobs/ads/ads_daily_total.py`
- `jobs/ads/ads_daily_investor.py`
- `jobs/ads/ads_daily_post_performance.py`
- `jobs/ads/ads_daily_column_performance.py`
- `jobs/ads/ads_daily_content_performance.py`
- `jobs/ads/ads_monthly_column_performance.py`
- `jobs/ads/ads_monthly_module_performance.py`
- `jobs/ads/ads_daily_home_module_performance.py`
- `jobs/ads/ads_daily_thirdapp_channel_uv.py`
- `jobs/ads/ads_daily_tryon_confirm.py`
- `jobs/ads/ads_daily_post_tryon_confirm.py`
- `jobs/ads/ads_daily_tryon_add_cart_conversion.py`
- `jobs/ads/ads_daily_product_tryon_performance.py`
- `jobs/ads/ads_daily_user_duration_frequency.py`
- `jobs/ads/ads_daily_retention_user_feature.py`
- `jobs/ads/ads_daily_retention_user_duration_bucket.py`
- `jobs/ads/ads_daily_retention_user_follow_bucket.py`
- `jobs/ads/ads_daily_retention_user_new_follow_bucket.py`
- `jobs/ads/ads_daily_retention_user_tryon_bucket.py`
- `jobs/ads/ads_daily_retention_user_product_detail_bucket.py`
- `jobs/ads/ads_daily_social_content_v2.py`
- `jobs/ads/ads_daily_social_content_attribute_v2.py`
- `jobs/ads/ads_daily_social_video_monitor_rankings_v2.py`
- `jobs/ads/ads_daily_social_creator_suspected_throttle_rankings.py`
- `jobs/ads/ads_daily_formal_video_increment.py`
- `jobs/ads/ads_weekly_formal_video_increment_growth.py`
- `jobs/ads/ads_weekly_ai_fashion_tryon_okr.py`
- `jobs/ads/ads_daily_ai_fashion_tryon_okr_v2.py`
- `jobs/ads/ads_daily_closet_overview.py`
- `jobs/ads/ads_daily_video_code_conversion.py`
- `jobs/ads/ads_daily_push.py`
- `jobs/ads/ads_daily_push_recall.py`
- `jobs/ads/ads_daily_makepod.py`
- `jobs/ads/ads_daily_ai_fashion_styleme.py`

日期规则：
1. `ads_daily_new.py` 自己按 `dws_device_daily + dws_user_daily + dws_appsflyer_download_daily` 的并集判定待处理 `dt`
2. `ads_daily_total.py` 自己按 `dws_device_daily + dws_user_daily` 的并集判定待处理 `dt`
3. 专题型 `ADS` 只按自己的直接依赖判定待处理范围，例如专栏、帖子、试穿、社媒、投资人主题都不应复用其他主题的共享 `dt`
4. 涉及次留的 `ADS`，需要把设备事实变更对应的 `DATE_SUB(dt, INTERVAL 1 DAY)` 一并补刷，保证成熟口径及时更新
5. 如果某张 `ADS` 同时依赖事实表修正和维表修正，要同时评估：
   - 当前分区是否需要重刷
   - 历史受影响分区是否需要补刷

### 刷新模式

- 按上游事实 `update_time` / 目标表水位判定 `dt`：
  - `ads_daily_new`
  - `ads_daily_total`
  - `ads_daily_post_performance`
  - `ads_daily_column_performance`
  - `ads_monthly_column_performance`
  - `ads_monthly_module_performance`
  - `ads_daily_content_performance`
  - `ads_daily_home_module_performance`
  - `ads_daily_thirdapp_channel_uv`
  - `ads_daily_tryon_confirm`
  - `ads_daily_post_tryon_confirm`
  - `ads_daily_tryon_add_cart_conversion`
  - `ads_daily_product_tryon_performance`
  - `ads_daily_user_duration_frequency`
  - `ads_daily_retention_user_feature`
  - `ads_daily_retention_user_duration_bucket`
  - `ads_daily_retention_user_follow_bucket`
  - `ads_daily_retention_user_new_follow_bucket`
  - `ads_daily_retention_user_tryon_bucket`
  - `ads_daily_retention_user_product_detail_bucket`
  - `ads_daily_social_content_v2`
  - `ads_daily_social_content_attribute_v2`
  - `ads_daily_social_video_monitor_rankings_v2`
  - `ads_daily_social_creator_suspected_throttle_rankings`
  - `ads_daily_formal_video_increment`
  - `ads_weekly_formal_video_increment_growth`
  - `ads_weekly_ai_fashion_tryon_okr`
  - `ads_daily_ai_fashion_tryon_okr_v2`
  - `ads_daily_closet_overview`
  - `ads_daily_investor`
  - `ads_daily_video_code_conversion`
  - `ads_daily_push`
  - `ads_daily_push_recall`
  - `ads_daily_makepod`
  - `ads_daily_ai_fashion_styleme`
- 固定窗口重刷或全量回刷能力，优先留在 `DWS` / `DIM` 层处理；不要重新引入共享 `ADS` 总入口来兜底


## 主链路

- 事件链路：
  - `ods_event_log -> dwd_event_log -> dws_device_daily / dws_user_daily / dws_content_item_device_daily -> ads_*`
- Makepod 事件链路：
  - `ods_event_log + oss_key_process_log.makepod_analyze_state -> dwd_makepod_event_log`
- Makepod 独立站日报链路：
  - `dwd_makepod_event_log -> dim_makepod_device_first_visit -> dws_makepod_traffic_daily`
  - `v3_decom.user_info -> dim_makepod_user_channel`
  - `dim_makepod_user_channel -> dws_makepod_traffic_daily`
  - `dim_makepod_user_channel + v3_decom.user_order + v3_decom.pay_order + v3_decom.user_order_item + decom.currency_conversion_dict -> dws_makepod_order_daily`
  - `dws_makepod_traffic_daily + dws_makepod_order_daily -> ads_daily_makepod`
- 平台内容链路：
  - `dwd_event_log + v3 服务端行为表 -> dws_platform_content_daily -> ads_daily_content_performance`
- 首页模块链路：
  - `dwd_event_log -> dws_home_module_daily -> ads_daily_home_module_performance`
- 第三方渠道曝光链路：
  - `dwd_event_log -> dws_thirdapp_channel_device_daily -> ads_daily_thirdapp_channel_uv`
- 试穿点击汇总链路：
  - `dwd_event_log -> dws_tryon_confirm_daily -> ads_daily_tryon_confirm`
- 帖子级试穿点击链路：
  - `dwd_event_log -> dws_tryon_confirm_post_daily + dim_post -> ads_daily_post_tryon_confirm`
- 试穿转化链路：
  - `dwd_event_log -> dws_tryon_confirm_daily + dws_tryon_user_daily -> ads_daily_tryon_add_cart_conversion`
- 商品试穿链路：
  - `dwd_event_log -> dws_product_behavior_daily -> ads_daily_product_tryon_performance`
- 用户时长频次链路：
  - `dwd_event_log -> dws_device_activity_daily -> ads_daily_user_duration_frequency`
- 用增链路：
  - `dwd_event_log -> dim_device_first_active + dim_user_first_active -> dws_device_daily + dws_user_daily`
  - `ods_appsflyer_download -> dws_appsflyer_download_daily`
  - `dws_device_daily + dws_user_daily + dws_appsflyer_download_daily -> ads_daily_new`
- 全量链路：
  - `dws_device_daily + dws_user_daily -> ads_daily_total`
- 留存特征链路：
  - `dws_user_daily + dwd_event_log + v3 服务端行为 -> dws_user_activity_daily -> dws_user_retention_feature_daily -> ads_daily_retention_user_feature`
- 留存时长分桶链路：
  - `dws_user_daily + dwd_event_log + v3 服务端行为 -> dws_user_activity_daily -> ads_daily_retention_user_duration_bucket`
- 留存累计关注分桶链路：
  - `dws_user_daily + dwd_event_log + v3 服务端行为 -> dws_user_activity_daily -> ads_daily_retention_user_follow_bucket`
- 留存新增关注分桶链路：
  - `dws_user_daily + dwd_event_log + v3 服务端行为 -> dws_user_activity_daily -> ads_daily_retention_user_new_follow_bucket`
- 留存 AI 试穿次数分桶链路：
  - `dws_user_daily + dwd_event_log + v3 服务端行为 -> dws_user_activity_daily -> ads_daily_retention_user_tryon_bucket`
- 留存浏览商详次数分桶链路：
  - `dws_user_daily + dwd_event_log + v3 服务端行为 -> dws_user_activity_daily -> ads_daily_retention_user_product_detail_bucket`
- 投资人主题链路：
  - `videos.* -> dim_auto_publish_video_task + dws_video_daily`
  - `dwd_event_log -> dws_registered_app_device_daily + dws_registered_app_user_daily`
  - `ods_appsflyer_download -> dws_appsflyer_download_daily`
  - `dim_auto_publish_video_task + dws_video_daily + dws_appsflyer_download_daily + dws_registered_app_device_daily + dws_registered_app_user_daily -> ads_daily_investor`
  - 当前现状：`ads_daily_investor` 已按独立入口运行，日期判断主要基于中层事实；但视频团队边界过滤仍在 `ADS` 聚合层做过渡性处理，后续应继续下沉到稳定中层
- 专栏帖子快照链路：
  - `v3_decom.community_post(status=1) -> dws_column_post_daily -> ads_daily_column_performance`
- 专栏月级行为链路：
  - `dwd_event_log + v3 服务端行为表 -> dws_column_interaction_monthly -> ads_monthly_column_performance`
- 专栏月级帖子快照链路：
  - `v3_decom.community_post(status=1) -> dws_column_post_daily -> dws_column_post_monthly -> ads_monthly_column_performance`
- 模块月级行为链路：
  - `dwd_event_log + v3 服务端行为表 -> dws_module_interaction_monthly -> ads_monthly_module_performance`
- 模块月级帖子快照链路：
  - `v3_decom.community_post(status=1) -> dws_column_post_daily -> dws_module_post_monthly -> ads_monthly_module_performance`
- 下载链路：
  - `ods_ios_download + ods_android_download -> dwd_download -> dws_download_daily`
- AppsFlyer 下载链路：
  - `ods_appsflyer_download -> dws_appsflyer_download_daily`
- 社媒内容链路：
  - `videos.yt_video_stats_log + videos.video_stats_log + videos.ig_media_stats_log + dim_video -> dws_social_video_snapshot_daily_v2 -> dws_social_video_state_daily_v2 + dim_auto_publish_video_task -> dws_social_channel_daily_v2 -> ads_daily_social_content_v2`
  - YouTube 累计快照源为 `yt_video_stats_log.view_count / like_count`
- 社媒属性维度链路：
  - `videos.video_upload_tasks + videos.video_upload_task_channels + dim_video -> dim_social_video_attribute`
  - `dim_social_video_attribute + dws_social_video_state_daily_v2 -> dws_social_attribute_daily_v2 -> ads_daily_social_content_attribute_v2`
  - `dim_social_video_attribute` 只保留 `ext_info` 顶层存在 `video_task` key 的任务，并一次性解析视频属性、品牌与博主属性
  - DWS 保留平均值和比率的分子分母，ADS 只做报表粒度轻派生
- 社媒视频监控日榜链路：
  - `videos.* -> dim_video`
  - `dim_video + dws_social_video_state_daily_v2 -> ads_daily_social_video_monitor_rankings_v2`
  - 日榜 `dt=D` 固定读取 D 与 `target_dt=D-1` 两个完整视频日末状态分区，分别产出 `D-1` 发布和 `D-1` 前发布视频在 D 当日的增量 Views Top 10
  - `dim_video + dws_social_video_snapshot_daily_v2 -> dws_social_creator_published_t1_daily -> ads_daily_social_creator_suspected_throttle_rankings`
  - 疑似限流账号榜 `dt=D` 只使用 T+1 当天存在真实快照的视频；在 `D-28 ~ D-1` 监控池内按发布时间取最近 10 个有效样本，前 7 个为历史基准、后 3 个为近期观察；`dim_video.is_formal_video` 不参与该主题准入或排除判断
- 正式号视频发布增量链路：
  - `videos.platform_video_tags -> dim_video`
  - `videos.yt_video_stats_log + videos.video_stats_log + videos.ig_media_stats_log + dim_video -> dws_social_video_snapshot_daily_v2 -> dws_social_video_state_daily_v2`
  - `dim_video + dws_social_video_state_daily_v2 -> ads_daily_formal_video_increment -> ads_weekly_formal_video_increment_growth`
  - YouTube 累计播放与点赞复用 Social Content v2 的 `yt_video_stats_log.view_count / like_count` 口径
- AI-Fashion 试穿 OKR 链路：
  - `dwd_event_log + dim_user_first_active + v3_decom.common_ext_ai_task -> dws_ai_fashion_tryon_okr_daily`
  - `dws_user_daily + dwd_event_log + v3_decom.common_ext_ai_task -> dws_ai_fashion_tryon_okr_user_daily`
  - `dwd_event_log + dws_user_daily + dws_user_activity_daily + dws_user_retention_feature_daily + dws_ai_fashion_tryon_okr_daily -> ads_daily_ai_fashion_tryon_okr_v2`
  - `dws_ai_fashion_tryon_okr_daily -> ads_weekly_ai_fashion_tryon_okr`
  - `dws_ai_fashion_tryon_okr_daily + dws_ai_fashion_tryon_okr_user_daily + dws_user_activity_daily + dws_user_daily + dws_user_retention_feature_daily -> ads_weekly_ai_fashion_tryon_okr_v2`
- 视频编码转化链路：
  - `dwd_event_log + dim_user_first_active -> dws_video_code_search_user_daily`
  - `videos.video_upload_tasks + dim_auto_publish_video_task + dws_video_daily + dws_video_code_search_user_daily -> dws_video_code_conversion_daily`
  - `dws_video_code_conversion_daily + dws_video_code_search_user_daily -> ads_daily_video_code_conversion`
- 衣橱概览链路：
  - `v3_decom.user_fashion_closet -> dim_user_closet_first_use`
  - `dwd_event_log + dws_user_daily + dim_user_closet_first_use + v3_decom.user_fashion_closet -> dws_closet_daily -> ads_daily_closet_overview`
- push 链路：
  - `v3_decom.user_notice -> dws_push_daily -> ads_daily_push`
- push 召回链路：
  - `dwd_event_log -> dws_push_recall_daily -> ads_daily_push_recall`
- AI-Fashion Style Me 链路：
  - `dwd_event_log + v3_decom.common_ext_ai_task + dws_user_daily -> dws_ai_fashion_styleme_daily -> ads_daily_ai_fashion_styleme`
- 默认要求：`ADS` 只消费本主题所需的 `DIM` / `DWS`；如果发现 `ADS` 直接读取 `ODS` / `DWD` / 外部源表，应优先视为待收口架构债，而不是常规模式
- 总量和新增异常时，先确认自己走的是事件链路还是下载链路。
- 非下载类 ADS 默认不要直接消费下载链路回刷日期；优先按自身依赖链路分发日期，避免全链路无效回刷。
- Batch 5 之后，试穿与商品 4 张 ADS 不再挂共享 `ADS` 旧入口的共享设备水位。
- Batch 6 之后，用户时长与留存 2 张 ADS 也不再挂共享 `ADS` 旧入口。
- Batch 7 之后，投资人报表也不再挂共享 `ADS` 旧入口。
- Batch 8 之后，`ads_daily_new`、`ads_daily_total` 也已拆成独立入口，仓库内不再保留 ADS 共享旧入口。
