# 投资人报表踩坑记录

- 视频播放相关源表的 `views / view_count` 是累计值，不是日增值；日报不能直接按当天采集值汇总。
- 正确口径是先按 `America/Toronto` 切天，再取每个视频当天 `max(collected_at)` 对应的日末累计值，最后与前一日日末累计值做差。
- 如果某视频次日没有采集记录，次日的日末累计值要沿用前一日，否则会把“无采集”误算成负增量或漏算。
- AppsFlyer `daily_report/v5` 同一天会返回多行聚合结果，不能把 `dt + platform + app_id` 误当成唯一键。
- AppsFlyer 实际返回的分组列可能与预期列名不完全一致，ODS 不能只保留猜测字段，必须额外保留完整原始行，如 `raw_row_json`，否则后续无法回查分组维度。
- AppsFlyer 常见字段名不是固定的 `Media Source` / `Campaign`，实际可能是 `Media Source (pid)`、`Campaign (c)` 这类变体；解析时不能只做精确列名匹配。
- AppsFlyer 某些 `campaign` 值可能出现编码异常，例如中文 campaign 在返回结果里显示成乱码；当前“新增下载总数”口径不受影响，但如果后续要按 campaign 分析，需优先回看 `raw_row_json` 并单独确认编码处理方案。

## 复用 `dws_device_daily` / `dws_user_daily` 时不能先按 `platform` 汇总再相加

- 投资人主题最终粒度是 `dt`，而 `dws_device_daily` / `dws_user_daily` 的现有粒度包含 `platform`
- 设备数、注册用户数、留存 cohort 都必须回到 `prop_device_id` / `prop_user_id` 明细去重后再做 `dt` 聚合
- 直接相加各端聚合值，会在同一 ID 跨端活跃时重复计数

## 注册用户留存不是新增注册用户留存

- 需求里的“注册用户”指 `prop_user_id` 非空且当日活跃的用户
- 留存分母不是 `is_new_user = TRUE`
- 如果误用新增注册用户 cohort，结果会更接近“注册次留”，不是当前投资人报表要的注册用户留存

## 内容展示量 / 点击量必须按 item 级去重

- `content_consume_count` 只覆盖详情类内容消费，不含 feeds 曝光，也不是 `post / spu` item 数
- `dwd_event_log` 已把 `args_spu` 落成 `product_code`、把 `args_post` 落成 `post_code`，同一原始事件会共享同一个 `raw_event_id`
- 因此内容展示量 / 点击量要按 `raw_event_id + item_key` 去重，其中 `item_key = product_code` 或 `post_code`
- 只按 `raw_event_id` 统计会把同一次曝光里的多个 item 压成 1，导致人均内容展示量和内容点击率都偏低

## 新增视频口径不能依赖“发布当天有播放增量”

- 现有视频源的采集时间与发布时间存在明显滞后，很多视频在 `published_dt` 当天没有任何播放快照
- 已实际验证：`published_dt = dt` 的行存在，但绝大多数 `has_snapshot = FALSE`，因此 `daily_view_increment` 会被算成 `0`
- 如果把新增视频定义为 `published_dt = dt AND daily_view_increment > 0`，则 `is_new_video` 可能长期全为 `FALSE`
- 排查这类问题时，先逐个渠道核对：发布时间、多伦多时区下的首个采集时间、首个采集日与发布日期的天数差，不要直接怀疑 ADS 聚合

## 新增视频播放数不能错误绑定 `is_new_video`

- `daily_view_increment` 表示视频在 `dt` 当天的播放增量，属于全量视频的日增量指标
- 投资人报表里的“新增视频播放数”实际要的是“当天新增播放数”，即 `dt` 当天全量视频 `daily_view_increment` 之和
- 如果写成 `SUM(IF(is_new_video, daily_view_increment, 0))`，会把分母错误收缩到“新增视频”，在 `is_new_video` 异常时进一步把播放增量也算成 `0`
- 正确做法是按 `SUM(daily_view_increment)` 汇总，再基于该值计算平均播放数和播放下载转化率
