# Makepod 事件明细经验

## 新建表后不要立即用 `Table.num_rows` 做强一致校验

- 首次执行 `dwd_makepod_event_log` 时，`oss_key_process_log` 实际已有 29408 行，但 BigQuery 表元数据 `num_rows` 暂时仍返回 29406
- 这会让 staging/swap 的行数校验误报，阻止 `makepod_analyze_state` 推进
- 需要强一致行数时使用 `SELECT COUNT(*)`，不要依赖可能延迟刷新的表元数据
- 本次误报发生在目标表写入后、状态推进前；因为 DWD 按 `oss_key` 整体替换且状态仍为 `0`，修正校验后可以安全重跑
