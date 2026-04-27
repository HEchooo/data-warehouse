# 用户时长与访问频次经验

## `全量设备` 不能偷用 `COALESCE(prop_user_id, prop_device_id)`

- 当需求明确写的是“全量设备”时，主体必须始终是 `prop_device_id`
- 如果写成 `COALESCE(NULLIF(prop_user_id,''), NULLIF(prop_device_id,''))`，已登录场景会退化成按 `user_id` 去重，口径会从“设备”变成“访客”
- 这类分群报表要先逐档确认主体：
  - `全量设备` / `游客` 可以按 `device_id`
  - `已登录用户` 可能要求按 `user_id`
- 主体不一致时，要在 `fact.md` 明确写清，避免把三档结果直接相加

## 先确认回看的是 DWS 聚合层，不是原始明细层

- 这张表的上游核心是 `dws_device_daily` / `dws_user_daily`
- 留存、时长、频次异常时，先回查 DWS 聚合逻辑，再回溯 `dwd_event_log`

## `mau_uv` 口径已经改成自然月 MAU

- 现在不是滚动 30 天窗口
- 回查旧结果时，先确认看的是旧版还是自然月版
- Batch 6 下沉到 `dws_device_activity_daily` 后，同月任一活跃日变化都会影响该月已落地日期的 `mau_uv`
- 排查时不要只盯当天分区，要连同同月已重算分区一起看

## 字段名已经从 `launch_session_count` 改成 `app_launch_count`

- 为了避免和 `session_id` 概念混淆
- 如果线上表还是旧字段，回查时记得同步确认 DDL 是否已执行 rename

## `content_consume_count` 要先去重，再保留 0 行

- `dws_device_daily` / `dws_user_daily` 的 `content_consume_count` 必须先按 `raw_event_id` 去重
- 同时要保留内容消费为 0 的活跃主体行，否则下游频次和人均指标会被带偏
