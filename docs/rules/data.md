# 数据与口径规则

## 时间

- 所有按天统计和 ADS 日报的 `dt` 默认按 `America/Toronto` 切天。
- ADS 日报统一由 `jobs/ads/ads_daily.py` 编排。
- 排查“今天没数据”时，先区分 T+0 展示、筛选器范围和 ETL 未跑。
- 入口编排支持 T+0，不等于每张 ADS 子任务都已经支持当天；还要同时检查子任务内部数据窗口和成熟度上限。

## 去重

- 原始事件级统计优先按 `raw_event_id` 去重。
- `hash_id` 只表示拆行后的明细行唯一键，不是原始事件唯一键。

## 聚合

- `like_rate`、`read_rate`、`follow_rate` 这类率指标跨天或跨维度聚合时，不能直接 `AVG`。
- 正确做法是按曝光 UV 加权，或等价地先聚合分子分母再计算整体率。

## 主链路

- 事件链路：
  - `ods_event_log -> dwd_event_log -> dws_device_daily / dws_user_daily -> ads_*`
- 下载链路：
  - `ods_ios_download + ods_android_download -> dws_download_daily -> ads_*`
- 总量和新增异常时，先确认自己走的是事件链路还是下载链路。

## 停留时长

- 停留时长口径只统计 `event_name = 'app_launch'` 的 `args_session_duration`。
- 优先按 `raw_event_id` 去重，缺失时才回退 `hash_id`。

## 内容消费

- `dws_device_daily` / `dws_user_daily` 的 `content_consume_count` 必须先按 `raw_event_id` 去重，再按设备 / 用户聚合。
- 同时保留内容消费为 `0` 的活跃主体行。

## 表结构变更

- 给已有 ADS 表新增维度字段时，不能只改未来增量；只要表已经被 dashboard 使用，就要评估历史分区是否需要回填。
- 给已有 ADS 表升级粒度时，先校验旧粒度总量不漂移，再继续扩字段或改看板。

## 清洗与归因规则

- `column_name` 为空时，先检查 `column_id` 是否带首尾双引号，或是否落成 `'null'` 这类占位值。
- `args_star`、`args_magazine`、`args_brand` 这类专栏 ID，要在模块识别、`column_id` 生成和 join 前统一清洗，不要只在最后一步临时补丁。
- `c_follow` 本身没有帖子主键，帖子级关注必须按同一天内的详情曝光做 last-touch 归因。
- `c_follow` 这类事件要区分“归因成功”和“归因结果维度完整”两件事；如果 `module / column_id` 仍为空，不能直接进入最终统计。