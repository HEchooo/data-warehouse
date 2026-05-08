# push 数据看板

## 报表

- 表：`ads_daily_push`
- 粒度：`dt`
- Dashboard：[【AI-Fashion】push 数据看板](https://bi.alvinclub.ca/superset/dashboard/108/)（id=108）
- Superset：物理 dataset `ads_daily_push`（id=238），chart `push 数据（日表）-明细表`（id=590）、`push 数据（日表）-已读率趋势`（id=591）

### `dws_push_daily`

- 粒度：`dt`
- 说明：push 主题中间层，不是最终报表层

## 字段

| 需求字段 | 定义 |
|---|---|
| 日期 | 报表日期，按 `America/Toronto` 切天 |
| 每日发送的 push 消息 | `v3_decom.user_notice` 中当日发送消息条数；按 `COUNT(1)` |
| 每日已读的消息数量 | 当日发送消息中当前 `read_status = 1` 的消息条数；按 `COUNTIF(read_status = 1)` |
| 已读率 | `每日已读的消息数量 / 每日发送的 push 消息` |

## 已落地字段

| 需求字段 | 落地表 | 落地字段 | 口径 |
|---|---|---|---|
| 日期 | `decom.ads_daily_push` | `dt` | 按 `America/Toronto` 切天 |
| 每日发送的 push 消息 | `decom.ads_daily_push` | `push_send_count` | `COUNT(1)` |
| 每日已读的消息数量 | `decom.ads_daily_push` | `read_count` | 同一批发送消息里当前 `read_status = 1` 的条数 |
| 已读率 | `decom.ads_daily_push` | `read_rate` | `read_count / push_send_count`；`push_send_count = 0` 时返回 `0` |

补充说明：

- 当前 `user_notice` 未提供独立“已读时间”字段，因此本主题按“发送日快照口径”统计，而不是“用户在哪一天读掉”
- 当前不区分 `tenant_code`，直接统计 `v3_decom.user_notice` 全量消息
- `portal_code` 不单独拆维度，当前报表为各国家站整体汇总
- `user_notice.gmt_create` 视为 UTC `DATETIME`，统一按 `DATE(TIMESTAMP(gmt_create, 'UTC'), 'America/Toronto')` 切业务日
- `read_rate` 跨天聚合时应使用 `SUM(read_count) / SUM(push_send_count)`，不要直接 `AVG(read_rate)`

## 依赖

- 上游：`v3_decom.user_notice`
- 中间表：`decom.dws_push_daily`
- 报表表：`decom.ads_daily_push`
- 脚本：`jobs/dws/dws_push_daily.py`, `jobs/ads/ads_daily_push.py`
- DDL：`ddl/dws/dws_push_daily.sql`, `ddl/ads/ads_daily_push.sql`
- 编排：`env/etl_config.json`, `etl_run.py`, `jobs/ads/ads_daily_push.py`
- Superset：物理 dataset `ads_daily_push`（id=238），chart `push 数据（日表）-明细表`（id=590）、`push 数据（日表）-已读率趋势`（id=591），dashboard `【AI-Fashion】push 数据看板`（id=108）
