# 用户时长与访问频次经验

## 先确认回看的是 DWS 聚合层，不是原始明细层

- 这张表的上游核心是 `dws_device_daily` / `dws_user_daily`
- 留存、时长、频次异常时，先回查 DWS 聚合逻辑，再回溯 `dwd_event_log`

## `mau_uv` 口径已经改成自然月 MAU

- 现在不是滚动 30 天窗口
- 回查旧结果时，先确认看的是旧版还是自然月版

## 字段名已经从 `launch_session_count` 改成 `app_launch_count`

- 为了避免和 `session_id` 概念混淆
- 如果线上表还是旧字段，回查时记得同步确认 DDL 是否已执行 rename

## `content_consume_count` 要先去重，再保留 0 行

- `dws_device_daily` / `dws_user_daily` 的 `content_consume_count` 必须先按 `raw_event_id` 去重
- 同时要保留内容消费为 0 的活跃主体行，否则下游频次和人均指标会被带偏
