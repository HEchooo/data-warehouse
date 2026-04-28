# 投资人报表踩坑记录

## 视频相关指标要先按 jhonson 团队过滤 YouTube 渠道

- 投资人报表里的 `新增视频播放数` 直接复用 `dws_video_daily`，`新增视频`、`活跃视频数` 则在 ADS 聚合层额外做投资人口径修正
- `docs/rules/video-mapping.md` 已明确 YouTube 的 bob 团队包含测试渠道和内部渠道；投资人报表要的 jhonson 团队口径，需要把这两组渠道一起排除
- 如果只排除测试渠道、不排除内部渠道，报表会继续混入 bob 团队视频
- 当前文档没有给出 TikTok / Ins 的团队边界，因此实现上只能先把 YouTube 按渠道过滤，TikTok / Ins 维持原口径；后续若补了白名单，再统一收口

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

## 自动发布跨平台视频只在投资人口径里按 `task_id` 去重

- `docs/rules/video-mapping.md` 已给出自动发布视频的统一映射：YouTube 看 `yt_uploads.task_id + status = COMPLETED`，Ins 看 `ig_uploads.task_id + status = COMPLETED`，TikTok 看 `video_uploads.task_id + status = COMPLETED`
- 同一个自动发布任务会把同一条视频发到多个平台；这些平台视频在上传表里共享同一个 `task_id`
- 同一 `task_id` 不要求所有平台都成功；只要成功平台数 `>= 1`，这个自动发布任务就应算 `1` 个视频，失败平台不参与统一视频口径
- 如果继续按原始 `video_key` 统计，`新增视频` 和 `活跃视频数` 会把同一个自动发布视频重复算多次
- 正确做法是先在 `dim_auto_publish_video_task` 里把自动发布视频统一映射成 `unified_video_key`，再由 `ads_daily_investor` 复用该统一键；手动发布视频继续沿用原始 `video_key`
- `新增视频` 不能只看“首次有采集记录”的日期，而要回看该统一视频键全历史最早 `views > 0` 的日期；否则同一任务跨平台首个正播放日期不一致时，会在多个日期重复记新增
- 为避免误并，只有当同一个 `video_key` 能稳定收敛到唯一 `task_id` 时才参与自动发布去重；出现多个不同 `task_id` 的异常映射时，宁可退回原始 `video_key`
- 这次去重只改视频数量类指标，不改 `新增视频播放数` 和 `新增视频平均播放数` 的播放量分子；否则会把“数量去重”和“播放量汇总”混成两个不同问题

## 新增视频播放数不能错误绑定 `is_new_video`

- `daily_view_increment` 表示视频在 `dt` 当天的播放增量，属于全量视频的日增量指标
- 如果源头累计播放值当天较前一日回落，`daily_view_increment` 为负数属于正常现象；当前投资人报表保留该负值，不做截断
- 投资人报表里的“新增视频播放数”实际要的是“当天新增播放数”，即 `dt` 当天全量视频 `daily_view_increment` 之和
- 如果写成 `SUM(IF(is_new_video, daily_view_increment, 0))`，会把分母错误收缩到“新增视频”，在 `is_new_video` 异常时进一步把播放增量也算成 `0`
- 正确做法是按 `SUM(daily_view_increment)` 汇总，再基于该值计算平均播放数和播放下载转化率

## 投资人日表会被“上游成熟度不一致”刷出半成品

- `ads_daily_investor` 的增量触发条件是四张上游事实表最近 `6` 小时内任一有更新；这意味着只要设备表或用户表先更新，ADS 就可能提前重刷当天分区
- 如果此时 `dws_video_daily` 或 `dws_appsflyer_download_daily` 还停留在上一轮早刷结果，投资人报表会出现“设备 / 用户指标正常，但视频 / 下载指标异常偏低”的混合成熟度状态
- 这次实际排查中，`2026-04-27` 的 `ads_daily_investor` 更新时间是 `2026-04-28 02:47 UTC`，但 `dws_video_daily` 对同一 `dt` 的更新时间仍是 `2026-04-27 09:45 UTC`；该时刻对应多伦多 `2026-04-27 05:45`，YouTube / Ins 当天快照尚未落齐
- 证据链要按 `update_time -> 原始快照落表时间 -> DWS 当天 has_snapshot` 逐层核对，不要只看 ADS 最终值
- 如果 `dws_video_daily` 当天只剩 TikTok 有 `has_snapshot = TRUE`，而 YouTube / Ins 为 `0`，通常不是投资人口径 SQL 写错，而是视频日表在多伦多当天结束前就被提前刷新了

## 新增视频平均播放数分子不能复用“新增视频播放数”

- “新增视频平均播放数”要看的不是当日全量视频新增了多少播放，而是新增视频首次被采到正播放量时，已经累计拿到了多少播放
- 因此分子不能复用 `SUM(daily_view_increment)`，也不能写成 `SUM(IF(is_new_video, daily_view_increment, 0))`
- 正确做法是仅在 `is_new_video = TRUE` 的当天取该视频的 `raw_day_end_views`，再按 `dt` 汇总后除以 `new_video_count`
- 如果误用“新增视频播放数”做分子，结果会变成“全量视频日增播放 / 新增视频数”，口径和“新增视频带来的播放”不是一回事

## 人均停留时长分母不能包含 `h5`

- `dws_device_daily` 会把活跃设备按 `platform` 落成 `iOS / Android / h5 / unknown`
- 只有 App 端 `app_launch` 才有 `args_session_duration`，`h5` 没有停留时长埋点
- 如果分子仍取 `SUM(session_duration_sec)`，但分母用全量 `COUNT(DISTINCT prop_device_id)`，就会把 `h5` 设备算进来，系统性压低“人均停留时长”
- 正确口径应为：`SUM(session_duration_sec) / COUNT(DISTINCT IF(platform != 'h5', prop_device_id, NULL))`
- 这类问题要先区分“展示层实时修正”和“ADS 历史分区修正”：chart 可直接按 `dws_device_daily` 重算口径；`ads_daily_investor` 的历史数据若要同步，必须先确认回填后再改生产数据

## app 已注册用户口径不能继续复用未过滤的 DWS 聚合表

- 本次需求把 `人均停留时长`、`人均内容展示量`、`人均内容点击量`、`内容点击率`、`人均商详页点击量`、`次日留存`、`7日留存` 都收敛到 `prop_url IS NULL OR prop_url = ''` 且 `prop_user_id` 非空
- `dws_content_item_device_daily`、`dws_product_detail_daily` 这类现有中间层没有保存“是否 app”“是否已注册”这两个过滤维度
- 如果直接拿这些未过滤聚合结果再在 ADS 层拼，会出现分子用了全量流量、分母想看注册用户，结果系统性偏大或偏小
- 当前正确做法是把目标口径下沉成 `dws_registered_app_device_daily` / `dws_registered_app_user_daily`，让 ADS 只做最终展示层派生

## 不能直接拿 `dt + platform` 报表回卷商详页曝光 UV

- `ads_daily_content_performance` 已有 `product_detail_exposure_uv / pv`，但它的粒度是 `dt + platform`
- 其中 `product_detail_exposure_uv` 的去重键是 `visitor_id = COALESCE(prop_user_id, prop_device_id)`，同一个访客可能跨平台出现
- 如果在投资人报表里直接把各 `platform` 的 UV 相加，会把跨平台活跃的同一访客重复计算
- 正确做法是在目标粒度 `dt` 上直接基于 `dwd_event_log` 重算去重 UV / PV，再派生 `人均商详页曝光`

## Lark 日报推送不能直接挂在高频共享 ETL 的成功回调上

- 当前远程服务器会高频执行主 ETL；如果把机器人推送直接挂在共享成功回调上，会在同一天重复发多次
- 投资人报表机器人需求是“每天只推一次”，应拆成独立脚本和独立 cron，而不是复用主链路触发频率
- 推送查询的 `dt` 也不能按上海自然日昨天理解，必须按 `America/Toronto` 的昨天取数

## 上海固定 12 点不一定等于多伦多昨天已经结束

- `America/Toronto` 有夏令时，和上海的时差不是全年固定 12 小时
- 冬令时下，上海 `12:00` 时多伦多前一自然日可能还没有结束，直接推会拿到未成熟数据
- 如果用固定上海时刻发“多伦多昨天”日报，建议把 cron 放到上海 `13:00` 之后，再叠加你们实际 ETL 完成时间留缓冲
