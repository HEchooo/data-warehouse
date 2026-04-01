# 投资人报表踩坑记录

## 视频相关指标要先排除 YouTube 测试账号

- 投资人报表里的 `新增视频`、`活跃视频数`、`新增视频播放数` 都直接复用 `dws_video_daily`
- `docs/rules/video-mapping.md` 已明确 YouTube 需要排除测试账号 `channel_id IN ('UCynAg7WHdiy8SuQpESO__yw','UCgBJozWjiE0cqwv7UNalyog')`
- 这类过滤不能只加在 `yt_videos` 或只加在 `yt_video_analytics_log` 单边；要同时过滤视频维表和播放快照源表
- 如果只过滤单边，会出现维表被排除但快照仍参与计算，或快照被排除但维表仍进入 spine，最终让 `dws_video_daily` 和投资人报表的视频指标口径不一致

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
- 正确口径应改为：按 `video_key` 全历史实际采集日中，最早满足 `raw_day_end_views > 0` 的 `dt` 记为新增日
- 这个“首次正播放采集日”是全历史属性；即使日常只重刷最近 30 天，上线新口径后也要补历史回填，不能指望旧分区自然修正
- 排查这类问题时，先逐个渠道核对：发布时间、多伦多时区下的首个采集时间、首个正播放采集日与发布日期的天数差，不要直接怀疑 ADS 聚合

## 新增视频播放数不能错误绑定 `is_new_video`

- `daily_view_increment` 表示视频在 `dt` 当天的播放增量，属于全量视频的日增量指标
- 投资人报表里的“新增视频播放数”实际要的是“当天新增播放数”，即 `dt` 当天全量视频 `daily_view_increment` 之和
- 如果写成 `SUM(IF(is_new_video, daily_view_increment, 0))`，会把分母错误收缩到“新增视频”，在 `is_new_video` 异常时进一步把播放增量也算成 `0`
- 正确做法是按 `SUM(daily_view_increment)` 汇总，再基于该值计算平均播放数和播放下载转化率

## 人均停留时长分母不能包含 `h5`

- `dws_device_daily` 会把活跃设备按 `platform` 落成 `iOS / Android / h5 / unknown`
- 只有 App 端 `app_launch` 才有 `args_session_duration`，`h5` 没有停留时长埋点
- 如果分子仍取 `SUM(session_duration_sec)`，但分母用全量 `COUNT(DISTINCT prop_device_id)`，就会把 `h5` 设备算进来，系统性压低“人均停留时长”
- 正确口径应为：`SUM(session_duration_sec) / COUNT(DISTINCT IF(platform != 'h5', prop_device_id, NULL))`
- 这类问题要先区分“展示层实时修正”和“ADS 历史分区修正”：chart 可直接按 `dws_device_daily` 重算口径；`ads_daily_investor` 的历史数据若要同步，必须先确认回填后再改生产数据

## Lark 日报推送不能直接挂在 `ads_daily.py` 的高频成功回调上

- 当前远程服务器会高频执行主 ETL；如果把机器人推送直接挂在 `jobs/ads/ads_daily.py` 成功后，会在同一天重复发多次
- 投资人报表机器人需求是“每天只推一次”，应拆成独立脚本和独立 cron，而不是复用主链路触发频率
- 推送查询的 `dt` 也不能按上海自然日昨天理解，必须按 `America/Toronto` 的昨天取数

## 上海固定 12 点不一定等于多伦多昨天已经结束

- `America/Toronto` 有夏令时，和上海的时差不是全年固定 12 小时
- 冬令时下，上海 `12:00` 时多伦多前一自然日可能还没有结束，直接推会拿到未成熟数据
- 如果用固定上海时刻发“多伦多昨天”日报，建议把 cron 放到上海 `13:00` 之后，再叠加你们实际 ETL 完成时间留缓冲
