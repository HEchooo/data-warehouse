# 用增报表经验

## 先分清自己走的是事件链路还是下载链路

- `ads_daily_new` 除了事件链路，还会承接 `ods_ios_download + ods_android_download -> dws_download_daily -> ads_daily_new`
- 看"新增"异常时，不要只查事件链路

## 排查刷新异常时先看总编排，不要只盯单表

- `ads_daily_new` 由 `jobs/ads/ads_daily.py` 统一编排
- 先确认目标日期有没有被带进总任务，再继续往下查单表逻辑

## DWD 回刷后总量异常，先怀疑补数策略

- `dwd_event_log` 历史补数在 `hash_id` 逻辑未变化时，不要先清表
- 优先重置 `oss_key_process_log.analyze_state` 后重跑覆盖
- 如果 `hash_id` 生成逻辑变过，优先按 `oss_key` 整体替换，避免旧行残留影响总量和时长
