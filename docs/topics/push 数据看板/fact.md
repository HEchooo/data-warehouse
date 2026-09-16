# push 数据看板

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | push 数据看板 | AI-Fashion push 数据看板、push 消息召回看板、push 召回数据 |
| 指标 | 每日发送的 push 消息 | push 发送数、推送发送量 |
| 指标 | 每日已读的消息数量 | push 已读数、推送已读量 |
| 指标 | 已读率 | push 已读率、推送已读率 |
| 指标 | 点击率 | push 点击率、push CTR |

## 适用范围

- 业务线：`Alvin's Club App`
- 适合：查询 push 的发送、已读，以及按召回策略统计的发送与点击效果。
- 不适合：App 全量活跃应使用[全量报表](../全量报表/fact.md)；非 push 的社区互动不属于本主题。
- 需要澄清：需要确认普通 push 或召回 push、日期范围和召回策略。

## 报表

### `ads_daily_push`

- 表：`my-project-8584-jetonai.decom.ads_daily_push`
- 状态：`active`
- 粒度：`dt`
- 时区：`America/Toronto`
- 数据范围：push 日发送、已读与已读率。
- 必要过滤：无。
- 不包含：召回策略维度。
- Dashboard：[【AI-Fashion】push 数据看板](https://bi.alvinclub.ca/superset/dashboard/108/)（id=108）
- Superset：物理 dataset `ads_daily_push`（id=238），chart `push 数据（日表）-明细表`（id=590）、`push 数据（日表）-已读率趋势`（id=591）

#### `dws_push_daily`

- 粒度：`dt`
- 说明：push 主题中间层，不是最终报表层

### `ads_daily_push_recall`

- 表：`my-project-8584-jetonai.decom.ads_daily_push_recall`
- 状态：`active`
- 粒度：`dt × recall_strategy`
- 时区：`America/Toronto`
- 数据范围：push 召回按策略的发送与点击表现。
- 必要过滤：按目标 `recall_strategy` 过滤。
- 不包含：普通 push 已读口径。
- Dashboard：[【AI-Fashion】push 消息召回看板](https://bi.alvinclub.ca/superset/dashboard/112/)（id=112）
- Superset：物理 dataset `ads_daily_push_recall`（id=245），chart `push 消息（日表）- 发送点击趋势`（id=609）、`push 消息（日表）- 明细表`（id=610）

#### `dws_push_recall_daily`

- 粒度：`dt × recall_strategy × push_run_id × actor_id`
- 说明：push 召回主题主体级中间层，用于按 `run_id + 用户/设备主体` 去重后聚合发送个数与点击个数

## 字段

### ads_daily_push

按“发送日快照”口径统计：`user_notice` 没有独立的已读时间字段，已读数是当日发送消息在快照时刻的已读状态，不是“用户在哪一天读掉”。`user_notice.gmt_create` 视为 UTC `DATETIME`，按 `DATE(TIMESTAMP(gmt_create, 'UTC'), 'America/Toronto')` 切业务日。

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 发送消息数 | `push_send_count` | 消息 | `v3_decom.user_notice` 当日发送消息条数 `COUNT(1)`；不区分 `tenant_code`，`portal_code` 不拆维度，各国家站整体汇总 | 无 | 可跨天求和 |
| 已读消息数 | `read_count` | 消息 | 同一批发送消息中当前 `read_status = 1` 的条数 `COUNTIF(read_status = 1)` | 无 | 可跨天求和 |
| 已读率 | `read_rate` | 消息 | `read_count / push_send_count`；分母为 0 时返回 `0` | 无 | 不可 `AVG`；跨天须按 `SUM(read_count) / SUM(push_send_count)` 重算 |

### ads_daily_push_recall

日期按 `America/New_York` 切天：`logAt_timestamp` 先按 `prop_timezone` 解释，再转美东业务日。`actor_id` 统一取 `COALESCE(NULLIF(prop_user_id, ''), NULLIF(prop_device_id, ''))`。

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 召回类型 | `recall_strategy` | 召回策略 | `JSON_VALUE(ext, '$.push.recall_strategy')` 的非空原始值 | 按目标策略过滤 | 不聚合 |
| 发送个数 | `send_count` | `push_run_id + actor_id` | `event_name = 'send_push'`，按 `dt + recall_strategy + push_run_id + actor_id` 去重后的主体数 | `JSON_VALUE(ext, '$.push.status') = 'SUCCESS'`；`recall_strategy` / `push_run_id` / `actor_id` 均非空 | 不可跨天或跨策略直接求和；跨范围须回 `dws_push_recall_daily` 重新去重 |
| 点击个数 | `click_count` | `push_run_id + actor_id` | `event_name = 'c_push'`，按同一组合去重后的主体数；不限制 `status` | `recall_strategy` / `push_run_id` / `actor_id` 均非空——`c_push` 存在缺失这三项的脏数据，不能直接按事件条数计点击 | 同上 |
| 点击率 | 未落地，仅在 Superset 明细表派生 | `push_run_id + actor_id` | `CASE WHEN send_count = 0 THEN 0 ELSE SAFE_DIVIDE(click_count, send_count) END`，不回写物理字段 | 无 | 不可 `AVG`；跨天或跨策略须分别汇总点击与发送后重算 |

主体级中间层 `decom.dws_push_recall_daily` 按 `dt + recall_strategy + push_run_id + actor_id` 保留唯一主体一行，落 `has_send` / `has_click` 标记供 ADS 聚合；ADS 不直接读取 `dwd_event_log`。

补充说明：

- 数据可用范围：`send_push` 有效业务日从美东 `2026-05-11` 起，`c_push` 从 `2026-05-09` 起；召回表已初始化美东 `2026-05-09`、`2026-05-12 ~ 2026-05-20` 共 10 个业务日

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
