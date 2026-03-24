# 全量报表经验

## 排查刷新异常时先看总编排，不要只盯单表

- `ads_daily_total` 由 `jobs/ads/ads_daily.py` 统一编排
- 先确认目标日期有没有被带进总任务，再继续往下查单表逻辑

## DWD 回刷后总量异常，先怀疑补数策略

- `dwd_event_log` 历史补数在 `hash_id` 逻辑未变化时，不要先清表
- 优先重置 `oss_key_process_log.analyze_state` 后重跑覆盖
- 如果 `hash_id` 生成逻辑变过，优先按 `oss_key` 整体替换，避免旧行残留影响总量和时长

## 不要把模块类日报的补零模板套到总量日报

- `ads_daily_total` 更接近 `DELETE + INSERT + 多个 CTE 合流`
- 首页模块类日报里的 `CROSS JOIN + LEFT JOIN` 是固定维度补零模板，不是总量日报默认写法
