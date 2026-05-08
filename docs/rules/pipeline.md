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
- `ads_daily_tryon_confirm`
- `ads_daily_post_tryon_confirm`
- `ads_daily_tryon_add_cart_conversion`
- `ads_daily_product_tryon_performance`
- `ads_daily_user_duration_frequency`
- `ads_daily_retention_user_feature`
- `ads_daily_retention_user_duration_bucket`
- `ads_daily_social_content`
- `ads_daily_closet_overview`
- `ads_daily_video_code_conversion`
- `ads_daily_push`

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
- `jobs/ads/ads_daily_tryon_confirm.py`
- `jobs/ads/ads_daily_post_tryon_confirm.py`
- `jobs/ads/ads_daily_tryon_add_cart_conversion.py`
- `jobs/ads/ads_daily_product_tryon_performance.py`
- `jobs/ads/ads_daily_user_duration_frequency.py`
- `jobs/ads/ads_daily_retention_user_feature.py`
- `jobs/ads/ads_daily_retention_user_duration_bucket.py`
- `jobs/ads/ads_daily_social_content.py`
- `jobs/ads/ads_daily_closet_overview.py`
- `jobs/ads/ads_daily_video_code_conversion.py`
- `jobs/ads/ads_daily_push.py`

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
  - `ads_daily_tryon_confirm`
  - `ads_daily_post_tryon_confirm`
  - `ads_daily_tryon_add_cart_conversion`
  - `ads_daily_product_tryon_performance`
  - `ads_daily_user_duration_frequency`
  - `ads_daily_retention_user_feature`
  - `ads_daily_retention_user_duration_bucket`
  - `ads_daily_social_content`
  - `ads_daily_closet_overview`
  - `ads_daily_investor`
  - `ads_daily_video_code_conversion`
  - `ads_daily_push`
- 固定窗口重刷或全量回刷能力，优先留在 `DWS` / `DIM` 层处理；不要重新引入共享 `ADS` 总入口来兜底


## 主链路

- 事件链路：
  - `ods_event_log -> dwd_event_log -> dws_device_daily / dws_user_daily / dws_content_item_device_daily -> ads_*`
- 平台内容链路：
  - `dwd_event_log + v3 服务端行为表 -> dws_platform_content_daily -> ads_daily_content_performance`
- 首页模块链路：
  - `dwd_event_log -> dws_home_module_daily -> ads_daily_home_module_performance`
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
  - `videos.* -> dim_video -> dws_social_video_snapshot_daily -> dws_social_video_state_daily + dim_auto_publish_video_task -> dws_social_channel_daily -> ads_daily_social_content`
- 视频编码转化链路：
  - `videos.video_upload_tasks + dim_auto_publish_video_task + dws_video_daily + dwd_event_log + dim_user_first_active -> dws_video_code_conversion_daily -> ads_daily_video_code_conversion`
- 衣橱概览链路：
  - `v3_decom.user_fashion_closet -> dim_user_closet_first_use`
  - `dwd_event_log + dws_user_daily + dim_user_closet_first_use + v3_decom.user_fashion_closet -> dws_closet_daily -> ads_daily_closet_overview`
- push 链路：
  - `v3_decom.user_notice -> dws_push_daily -> ads_daily_push`
- 默认要求：`ADS` 只消费本主题所需的 `DIM` / `DWS`；如果发现 `ADS` 直接读取 `ODS` / `DWD` / 外部源表，应优先视为待收口架构债，而不是常规模式
- 总量和新增异常时，先确认自己走的是事件链路还是下载链路。
- 非下载类 ADS 默认不要直接消费下载链路回刷日期；优先按自身依赖链路分发日期，避免全链路无效回刷。
- Batch 5 之后，试穿与商品 4 张 ADS 不再挂共享 `ADS` 旧入口的共享设备水位。
- Batch 6 之后，用户时长与留存 2 张 ADS 也不再挂共享 `ADS` 旧入口。
- Batch 7 之后，投资人报表也不再挂共享 `ADS` 旧入口。
- Batch 8 之后，`ads_daily_new`、`ads_daily_total` 也已拆成独立入口，仓库内不再保留 ADS 共享旧入口。
