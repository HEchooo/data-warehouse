# 视频编码转化

## `dwd_event_id` 表名误用

- 需求里提到 `decom.dwd_event_id`，但当前数据集中不存在该表
- 本主题应统一改为使用 `decom.dwd_event_log`
- 排查方式：
  1. 先查 `decom.INFORMATION_SCHEMA.TABLES` 确认真实表名
  2. 再核对脚本和文档是否仍引用了错误表名

## overall 搜索用户数被跨编码重复计数

- 现象：`【AI-Fashion】视频编码转化看板` 中 `stat_scope = 'overall'` 的“搜索用户数”大于原始事件按天全局去重的用户数
- 根因：同一用户同一天搜索多个 8 位编码时，task 粒度 `search_user_count` 会在多个 `promotion_code / task_id` 行各记 1 次；如果 `overall` 直接 `SUM(task_level.search_user_count)`，就会把同一用户跨编码重复累计
- 修复方式：
  1. 先沉淀 `dt × promotion_code × user_id` 粒度的 `dws_video_code_search_user_daily`
  2. task 粒度继续按 `promotion_code -> task_id` 聚合
  3. `overall` 分子改为在全部已映射编码上按用户全局去重后再聚合
- 排查方式：
  1. 先核对 `dwd_event_log` 中 Toronto 日期的 `COUNT(DISTINCT prop_user_id)` 和 `COUNT(DISTINCT CONCAT(prop_user_id, '#', search_content))`
  2. 再核对 `ads_daily_video_code_conversion` 的 `overall.search_user_count` 是否等于 task 粒度逐行求和
