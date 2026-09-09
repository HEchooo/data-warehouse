# push 数据看板

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | push 数据看板 | AI-Fashion push 数据看板、push 消息召回看板、push 召回数据 |
| 指标 | 每日发送的 push 消息 | push 发送数、推送发送量 |
| 指标 | 每日已读的消息数量 | push 已读数、推送已读量 |
| 指标 | 已读率 | push 已读率、推送已读率 |
| 指标 | 点击率 | push 点击率、push CTR |

## 报表

### `ads_daily_push`

- 表：`ads_daily_push`
- 粒度：`dt`
- Dashboard：[【AI-Fashion】push 数据看板](https://bi.alvinclub.ca/superset/dashboard/108/)（id=108）
- Superset：物理 dataset `ads_daily_push`（id=238），chart `push 数据（日表）-明细表`（id=590）、`push 数据（日表）-已读率趋势`（id=591）

#### `dws_push_daily`

- 粒度：`dt`
- 说明：push 主题中间层，不是最终报表层

### `ads_daily_push_recall`

- 表：`ads_daily_push_recall`
- 粒度：`dt × recall_strategy`
- Dashboard：[【AI-Fashion】push 消息召回看板](https://bi.alvinclub.ca/superset/dashboard/112/)（id=112）
- Superset：物理 dataset `ads_daily_push_recall`（id=245），chart `push 消息（日表）- 发送点击趋势`（id=609）、`push 消息（日表）- 明细表`（id=610）

#### `dws_push_recall_daily`

- 粒度：`dt × recall_strategy × push_run_id × actor_id`
- 说明：push 召回主题主体级中间层，用于按 `run_id + 用户/设备主体` 去重后聚合发送个数与点击个数

## 字段

### `ads_daily_push`

| 需求字段 | 定义 |
|---|---|
| 日期 | 报表日期，按 `America/Toronto` 切天 |
| 每日发送的 push 消息 | `v3_decom.user_notice` 中当日发送消息条数；按 `COUNT(1)` |
| 每日已读的消息数量 | 当日发送消息中当前 `read_status = 1` 的消息条数；按 `COUNTIF(read_status = 1)` |
| 已读率 | `每日已读的消息数量 / 每日发送的 push 消息` |

### `ads_daily_push_recall`

| 需求字段 | 定义 |
|---|---|
| 日期 | 报表日期，按 `America/New_York` 切天；底层 `logAt_timestamp` 需先按 `prop_timezone` 解释，再转换为美东业务日 |
| 召回类型 | `JSON_VALUE(ext, '$.push.recall_strategy')` 的非空值 |
| 发送个数 | `event_name = 'send_push'` 且 `JSON_VALUE(ext, '$.push.status') = 'SUCCESS'` 时，按 `dt + recall_strategy` 对唯一 `push_run_id + actor_id` 去重后的数量 |
| 点击个数 | `event_name = 'c_push'` 时，按 `dt + recall_strategy` 对唯一 `push_run_id + actor_id` 去重后的数量 |
| 点击率 | `点击个数 / 发送个数`；`发送个数 = 0` 时返回 `0` |
| 明细表 | 展示 `dt`、`recall_strategy`、`send_count`、`click_count` |
| 筛选器 | 至少包含日期范围、召回类型 |

## 已落地字段

### `decom.ads_daily_push`

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

### `decom.ads_daily_push_recall`

| 需求字段 | 落地表 | 落地字段 | 口径 |
|---|---|---|---|
| 日期 | `decom.ads_daily_push_recall` | `dt` | 按 `America/New_York` 切天；`logAt_timestamp` 需先按 `prop_timezone` 解释，再转换为业务日 |
| 召回类型 | `decom.ads_daily_push_recall` | `recall_strategy` | `JSON_VALUE(ext, '$.push.recall_strategy')` 的非空原始值 |
| 发送个数 | `decom.ads_daily_push_recall` | `send_count` | `event_name = 'send_push'` 且 `JSON_VALUE(ext, '$.push.status') = 'SUCCESS'` 时，按 `dt + recall_strategy + push_run_id + actor_id` 去重后的主体数 |
| 点击个数 | `decom.ads_daily_push_recall` | `click_count` | `event_name = 'c_push'` 时，按 `dt + recall_strategy + push_run_id + actor_id` 去重后的主体数 |
| 主体级中间层 | `decom.dws_push_recall_daily` | `dt + recall_strategy + push_run_id + actor_id` | 对有效事件保留唯一主体一行，并落 `has_send` / `has_click` 标记供 ADS 聚合 |

补充说明：

- 当前 `ADS` 不直接读取 `dwd_event_log`，而是只消费主体级 `DWS`
- `actor_id` 统一取 `COALESCE(NULLIF(prop_user_id, ''), NULLIF(prop_device_id, ''))`
- `send_push` 与 `c_push` 都要求 `recall_strategy`、`push_run_id`、`actor_id` 非空后才进入统计
- `send_push` 额外要求 `JSON_VALUE(ext, '$.push.status') = 'SUCCESS'`，失败的发送事件不计入 `send_count`；`c_push` 不做 `status` 限制
- `点击率` 当前仅在 Superset 明细表 `push 消息（日表）- 明细表` 中派生展示，表达式为 `CASE WHEN send_count = 0 THEN 0 ELSE SAFE_DIVIDE(click_count, send_count) END`，不回写 `decom.ads_daily_push_recall` 物理字段
- 截至 `2026-05-21` 的只读校验样本中：
  - `send_push` 的有效业务日范围为美东 `2026-05-11` 到 `2026-05-20`
  - `c_push` 的有效业务日范围为美东 `2026-05-09` 到 `2026-05-20`
  - `c_push` 存在部分缺失 `recall_strategy` / `push_run_id` / `actor_id` 的脏数据，不能直接按事件条数统计点击个数
  - 已完成 BigQuery 建表、Superset 资产创建与生产历史初始化；当前已初始化美东 `2026-05-09`、`2026-05-12` 到 `2026-05-20` 共 10 个业务日

## 依赖

### `ads_daily_push`

- 上游：`v3_decom.user_notice`
- 中间表：`decom.dws_push_daily`
- 报表表：`decom.ads_daily_push`
- 脚本：`jobs/dws/dws_push_daily.py`, `jobs/ads/ads_daily_push.py`
- DDL：`ddl/dws/dws_push_daily.sql`, `ddl/ads/ads_daily_push.sql`
- 编排：`env/etl_config.json`, `etl_run.py`, `jobs/ads/ads_daily_push.py`
- Superset：物理 dataset `ads_daily_push`（id=238），chart `push 数据（日表）-明细表`（id=590）、`push 数据（日表）-已读率趋势`（id=591），dashboard `【AI-Fashion】push 数据看板`（id=108）

### `ads_daily_push_recall`

- 上游：`decom.dwd_event_log`
- 中间表：`decom.dws_push_recall_daily`
- 报表表：`decom.ads_daily_push_recall`
- 脚本：`jobs/dws/dws_push_recall_daily.py`, `jobs/ads/ads_daily_push_recall.py`
- DDL：`ddl/dws/dws_push_recall_daily.sql`, `ddl/ads/ads_daily_push_recall.sql`
- 编排：`env/etl_config.json`, `etl_run.py`, `jobs/ads/ads_daily_push_recall.py`
- Superset：物理 dataset `ads_daily_push_recall`（id=245），chart `push 消息（日表）- 发送点击趋势`（id=609）、`push 消息（日表）- 明细表`（id=610），dashboard `【AI-Fashion】push 消息召回看板`（id=112）
