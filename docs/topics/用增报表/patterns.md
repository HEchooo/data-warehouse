# 用增报表经验

## 先分清自己走的是事件链路还是下载链路

- `ads_daily_new` 除了事件链路，还会承接 `ods_appsflyer_download -> dws_appsflyer_download_daily -> ads_daily_new`
- 看"新增"异常时，不要只查事件链路

## 排查刷新异常时先看本表待处理日期判定，不要只盯单个指标

- `ads_daily_new` 现在由 `jobs/ads/ads_daily_new.py` 独立判定待处理日期
- 先确认 `dws_device_daily`、`dws_user_daily`、`dws_appsflyer_download_daily` 有没有把目标日期推进来，再继续往下查单表逻辑

## 匿名设备口径已经下沉到 `dws_device_daily`

- `ads_daily_new` 不再直扫 `dwd_event_log` 判定匿名设备
- 先看 `dws_device_daily.has_anonymous_event` 是否已按设备日正确落地，再判断 ADS 聚合是否异常

## 新增用户留存要用用户事实回推 cohort 日期

- `next_day_retention_rate` 和 `day_7_retention_rate` 的 cohort 是新增 `prop_user_id`，不是新增设备
- 当 `dws_user_daily` 的活跃日期发生变化时，`ads_daily_new` 除了刷新当天，也要分别回推 `dt-1` 和 `dt-7`，否则成熟后的留存不会更新到 cohort 日期

## DWD 回刷后总量异常，先怀疑补数策略

- `dwd_event_log` 历史补数在 `hash_id` 逻辑未变化时，不要先清表
- 优先重置 `oss_key_process_log.analyze_state` 后重跑覆盖
- 如果 `hash_id` 生成逻辑变过，优先按 `oss_key` 整体替换，避免旧行残留影响总量和时长
