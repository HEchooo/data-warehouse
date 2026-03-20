# 数据与口径规则

## 时间

- 所有按天统计和 ADS 日报的 `dt` 默认按 `America/Toronto` 切天。
- ADS 日报统一由 `jobs/ads/ads_daily/ads_daily.py` 编排。

## 去重

- 原始事件级统计优先按 `raw_event_id` 去重。
- `hash_id` 只表示拆行后的明细行唯一键，不是原始事件唯一键。

## 主链路

- 事件链路：
  - `ods_event_log -> dwd_event_log -> dws_device_daily / dws_user_daily -> ads_*`
- 下载链路：
  - `ods_ios_download + ods_android_download -> dws_download_daily -> ads_*`

## 停留时长

- 停留时长口径只统计 `event_name = 'app_launch'` 的 `args_session_duration`。
- 优先按 `raw_event_id` 去重，缺失时才回退 `hash_id`。

## 内容消费

- `dws_device_daily` / `dws_user_daily` 的 `content_consume_count` 必须先按 `raw_event_id` 去重，再按设备 / 用户聚合。
- 同时保留内容消费为 `0` 的活跃主体行。
