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

## ADS 加列必须走 `--full-refresh`，增量跑会直接失败

- 两张橱窗号 ADS 的增量写法是 `SELECT * FROM 目标表 WHERE dt NOT IN (...) UNION ALL BY NAME SELECT * FROM staging`。
- `UNION ALL BY NAME` 要求两侧列名集合完全一致，所以只要 staging 多了新列，增量跑当场报错，不会静默写坏数据。
- 加列的正确顺序：改 SQL → 改 DDL 文件 → 用 `--full-refresh` 重刷（表由 staging 直接换名，列顺序与 DDL 一致）。
- 如果改用 `ALTER TABLE ADD COLUMN` + 增量跑也能通，但换名后列会被追加到末尾，和 DDL 文件的列顺序对不上。

## 汇率字典没有 `USD -> USD` 这一行

- `decom.currency_conversion_dict` 只存跨币种汇率，`from_currency = to_currency` 的自反行不存在。
- 直接 `LEFT JOIN` 取 `to_currency = 'USD'` 的汇率，会让本来就是 USD 计价的金额换算结果为 `NULL`。
- 写法固定为 `IF(UPPER(currency) = 'USD', 1, r.usd_rate)`；字典真的缺某个币种时保持 `NULL` 并打 warning，不要 `IFNULL(rate, 1)` —— 那会把一个错误金额伪装成正确的美元值。

## RPA 抓取表的坑：表名、字段名、覆盖范围都要先实测

- 表名是复数 `videos.tiktok_shop_rpa_video_stats_snapshots` / `videos.tiktok_shop_rpa_account_stats_snapshots`，账号表的账号字段是 `username`（不是 `user_name`）。
- RPA 覆盖是逐步放开的：接入时 10 个橱窗号只覆盖 2 个，被覆盖账号也只采到部分视频。做 `LEFT JOIN` 前先按维度统计一遍覆盖率，不要假设源表是全量。
- 一个统计日会在次日被抓多次（实测一天 4 次），必须按 `max(fetched_at)` 去重；`id DESC` 作为同时刻的兜底 tie-break。
- `product_matched_by` 说明商品是怎么关联上视频的，当前全是 `publish_date`（按发布日期推断）。这类推断型归因要在 `fact.md` 写清可靠性边界，不能当结算依据。

## video_stats_log 与 users 的关联 key

- 需求侧描述的关联 key 是 `username`，实测 `video_stats_log.user_name = users.username` 与 `video_stats_log.user_id = users.id` 结果完全一致（同为 1146 条橱窗号视频）。
- 仓库内 Social Content 链路统一用 `creator_id = users.id`（`dim_video.creator_id`），所以本主题也走 id，`username` 只作为展示 handle，不再引入第二套 join key。
