# 【Social Content】橱窗号数据 排查经验

## 播放增量突然放大，先查采集覆盖再查计算逻辑

- 现象：橱窗号账号维度日报 `2026-08-14` 播放增量 98.8 万、`2026-08-17` 播放增量 69.7 万，其余日期只有几千。
- 根因：不是增量算错，而是服务端 `videos.video_stats_log` 对橱窗号视频的采集覆盖在逐步放开。`08-12` 只采集 162 个视频，`08-14` 起 417 个，`08-17` 起 1129 个。新纳入采集的视频第一次出现快照时带着历史存量播放，被当日增量口径一次性吃进去。
- 排查顺序：
  1. 按 `dt` 统计 `dws_social_video_snapshot_daily_v2` 中该批账号的 `COUNT(DISTINCT video_key)`，先确认采集视频数是否稳定
  2. 采集数稳定后，再看单视频的日末累计序列是否连续
  3. 最后才怀疑 ADS 的增量表达式
- 结论：接入一批新账号或新平台后，前几天的增量口径默认不可信，要在主题文档里写清可用起点。

## 服务端账号表没有历史，"新增" 类指标必须先落快照

- `videos.users` 只有当前状态（`follower_count` 还是 STRING），没有任何历史版本，也没有按天的日志表。
- 因此 `新增粉丝数` 无法回溯，只能从 `dws_social_creator_snapshot_daily` 首次运行日开始逐日累积，首日 `follower_increment` 必然为空。
- 不要用 `users.updated_at` 反推历史粉丝数：它只是最后一次变更时间，拿不到变更前的值。
- 同类需求（新增关注、新增订阅）先确认源表是状态表还是日志表，再决定能不能给历史。

## video_stats_log 与 users 的关联 key

- 需求侧描述的关联 key 是 `username`，实测 `video_stats_log.user_name = users.username` 与 `video_stats_log.user_id = users.id` 结果完全一致（同为 1146 条橱窗号视频）。
- 仓库内 Social Content 链路统一用 `creator_id = users.id`（`dim_video.creator_id`），所以本主题也走 id，`username` 只作为展示 handle，不再引入第二套 join key。
