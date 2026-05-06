# 视频编码转化

## `dwd_event_id` 表名误用

- 需求里提到 `decom.dwd_event_id`，但当前数据集中不存在该表
- 本主题应统一改为使用 `decom.dwd_event_log`
- 排查方式：
  1. 先查 `decom.INFORMATION_SCHEMA.TABLES` 确认真实表名
  2. 再核对脚本和文档是否仍引用了错误表名
