# Pipeline 规则

## ADS 日报编排

### 目标

统一编排 ADS 日报任务，按天增量刷新以下报表：
- `ads_daily_new`
- `ads_daily_total`
- `ads_daily_content_performance`
- `ads_daily_tryon_confirm`
- `ads_daily_tryon_add_cart_conversion`
- `ads_daily_post_tryon_confirm`
- `ads_daily_post_performance`
- `ads_daily_column_performance`
- `ads_daily_product_tryon_performance`
- `ads_daily_home_module_performance`
- `ads_daily_user_duration_frequency`

### 执行流程

入口脚本：`jobs/ads/ads_daily.py`

执行顺序：
1. 在 `ads_daily.py` 内识别待处理日期
2. 执行 `jobs/ads/ads_daily_new.py`
3. 执行 `jobs/ads/ads_daily_total.py`
4. 执行 `jobs/ads/ads_daily_content_performance.py`
5. 执行 `jobs/ads/ads_daily_tryon_confirm.py`
6. 执行 `jobs/ads/ads_daily_tryon_add_cart_conversion.py`
7. 执行 `jobs/ads/ads_daily_post_tryon_confirm.py`
8. 执行 `jobs/ads/ads_daily_post_performance.py`
9. 执行 `jobs/ads/ads_daily_column_performance.py`
10. 执行 `jobs/ads/ads_daily_product_tryon_performance.py`
11. 执行 `jobs/ads/ads_daily_home_module_performance.py`
12. 执行 `jobs/ads/ads_daily_user_duration_frequency.py`


## 主链路

- 事件链路：
  - `ods_event_log -> dwd_event_log -> dws_device_daily / dws_user_daily -> ads_*`
- 下载链路：
  - `ods_ios_download + ods_android_download -> dws_download_daily -> ads_*`
- AppsFlyer 下载链路：
  - `ods_appsflyer_download -> dws_appsflyer_download_daily`
- 投资人视频链路：
  - `videos.* -> dws_video_daily`
- 总量和新增异常时，先确认自己走的是事件链路还是下载链路。
