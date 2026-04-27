# Pipeline 规则

## ADS 日报编排

### 目标

- 全部 ADS 日报都走独立表级入口
- 共享 `ADS` 旧入口已退出主链路，不再作为编排入口

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
- `ads_daily_social_content`

### 执行流程

独立表级入口：
- `jobs/ads/ads_daily_new.py`
- `jobs/ads/ads_daily_total.py`
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

日期规则：
1. `ads_daily_new.py` 自己按 `dws_device_daily + dws_user_daily + dws_download_daily` 的并集判定待处理 `dt`
2. `ads_daily_total.py` 自己按 `dws_device_daily + dws_user_daily` 的并集判定待处理 `dt`
3. 涉及次留的 ADS，需要把设备事实变更对应的 `DATE_SUB(dt, INTERVAL 1 DAY)` 一并补刷，保证成熟口径及时更新


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
  - `dwd_download -> dws_download_daily`
  - `dws_device_daily + dws_user_daily + dws_download_daily -> ads_daily_new`
- 全量链路：
  - `dws_device_daily + dws_user_daily -> ads_daily_total`
- 留存特征链路：
  - `dws_user_daily + dwd_event_log + v3 服务端行为 -> dws_user_activity_daily -> dws_user_retention_feature_daily -> ads_daily_retention_user_feature`
- 投资人主题链路：
  - `videos.* + dws_video_daily + dws_appsflyer_download_daily + dws_device_daily + dws_user_daily + dwd_event_log -> dim_auto_publish_video_task + dws_registered_app_device_daily + dws_registered_app_user_daily -> ads_daily_investor`
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
  - `videos.* -> dws_social_video_daily + dim_auto_publish_video_task -> dws_social_channel_daily -> ads_daily_social_content`
- 总量和新增异常时，先确认自己走的是事件链路还是下载链路。
- 非下载类 ADS 默认不要直接消费下载链路回刷日期；优先按自身依赖链路分发日期，避免全链路无效回刷。
- Batch 5 之后，试穿与商品 4 张 ADS 不再挂共享 `ADS` 旧入口的共享设备水位。
- Batch 6 之后，用户时长与留存 2 张 ADS 也不再挂共享 `ADS` 旧入口。
- Batch 7 之后，投资人报表也不再挂共享 `ADS` 旧入口。
- Batch 8 之后，`ads_daily_new`、`ads_daily_total` 也已拆成独立入口，仓库内不再保留 ADS 共享旧入口。
