# 投资人报表踩坑记录

- 视频播放相关源表的 `views / view_count` 是累计值，不是日增值；日报不能直接按当天采集值汇总。
- 正确口径是先按 `America/Toronto` 切天，再取每个视频当天 `max(collected_at)` 对应的日末累计值，最后与前一日日末累计值做差。
- 如果某视频次日没有采集记录，次日的日末累计值要沿用前一日，否则会把“无采集”误算成负增量或漏算。
- AppsFlyer `daily_report/v5` 同一天会返回多行聚合结果，不能把 `dt + platform + app_id` 误当成唯一键。
- AppsFlyer 实际返回的分组列可能与预期列名不完全一致，ODS 不能只保留猜测字段，必须额外保留完整原始行，如 `raw_row_json`，否则后续无法回查分组维度。
- AppsFlyer 常见字段名不是固定的 `Media Source` / `Campaign`，实际可能是 `Media Source (pid)`、`Campaign (c)` 这类变体；解析时不能只做精确列名匹配。
- AppsFlyer 某些 `campaign` 值可能出现编码异常，例如中文 campaign 在返回结果里显示成乱码；当前“新增下载总数”口径不受影响，但如果后续要按 campaign 分析，需优先回看 `raw_row_json` 并单独确认编码处理方案。
