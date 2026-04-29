# 维表与分区增量规则

## 1. 先做决策（30 秒版）

按任务类型先选模式，不要先写 SQL：

1. `DIM` 维表同步（有可靠 `gmt_update`，主键稳定）
- 选 **模式 A：窗口增量 upsert**（默认 6 小时）

2. `DWS/ADS` 分区事实（按 `dt`/`month` 重算）
- 选 **模式 B：分区重算**（先发现待刷分区，再 `DELETE + INSERT`）

3. 多上游、无统一水位、但要控成本
- 选 **模式 C：固定回刷窗口**（例如最近 3 天、当月+上月）

4. 需要严格历史一致（窗口外更新也不能漏）
- 不用窗口增量，改为更强的水位/state 方案，先和需求方确认成本

---

## 2. 模式 A：窗口增量 upsert（DIM 默认）

### 适用条件

- 源表有稳定更新时间字段（优先 `gmt_update`）
- 目标表有明确业务主键
- 允许“窗口内重复重算，窗口外可能漏更”的取舍

### 标准流程

1. 判断目标表是否存在/为空
2. 空表：全量初始化
3. 非空：读取最近 `6` 小时源侧更新
4. 窗口内按主键去重
5. 与目标表做 key-based upsert（推荐 staging + changed + swap）

### 默认窗口

```sql
WHERE source_update_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
```

### 注意

- `update_time` 表示“本表写入时间”，不是源侧水位
- 不要默认用 `create_time` 代替 `gmt_update`

---

## 3. 模式 B：分区重算（DWS/ADS 事实默认）

### 适用条件

- 目标表按 `dt` / `month` 分区
- 指标依赖多上游并集，不适合单行 upsert
- 语义是“分区重算”而不是“主键补丁”

### 标准流程

1. 发现待刷分区（由本表自己的上游决定）
2. 对这些分区执行幂等重算

```sql
DELETE FROM target WHERE dt IN (...);
INSERT INTO target SELECT ... WHERE dt IN (...);
```

### 红线

- 不要把多张表硬塞到共享 `dt` 发现脚本
- 不要复用别的主题日期范围
- 每张表自己对自己的上游负责

---

## 4. 模式 C：固定回刷窗口（性能优先）

当“多上游变更回推历史”成本过高时，允许在模式 B 结果上做二次截断：

- 日表：仅最近 `N` 天（例如 3 天）
- 月表：仅最近 `N` 月（例如当月+上月）

### 取舍说明（必须写在需求里）

- 优点：算力稳定、调度可控
- 风险：窗口外历史修正不会自动回刷
- 要求：若需追溯历史，改人工触发补刷或临时扩窗口

---

## 5. 不适用场景

以下情况不要直接套“6 小时窗口”：

- 只有 `create_time`，无可靠更新时点
- 调度经常中断超过窗口
- 晚到/补录超过窗口且业务不能接受漏刷
- 表同时依赖事件、服务端事实、维表修正且无统一强水位

---

## 6. 仓库内参考实现

- 模式 A（窗口增量 upsert）
  - `jobs/dim/dim_column.py`
  - `jobs/dim/dim_post.py`

- 模式 B（分区重算）
  - `jobs/dws/dws_column_interaction_daily.py`
  - `jobs/ads/ads_daily_column_performance.py`

- 模式 C（固定回刷窗口）
  - 日表近 3 天：`dws_column_interaction_daily` / `ads_daily_column_performance`
  - 月表当月+上月：`dws_column_interaction_monthly` / `ads_monthly_column_performance` / `dws_module_interaction_monthly` / `ads_monthly_module_performance`

---

## 7. 执行前检查清单

1. 任务属于 A/B/C 哪种模式？
2. 待刷范围由谁决定（本表上游，还是固定窗口）？
3. 是否需要回刷历史分区？边界到哪天/哪月？
4. 是否已向业务方明确“性能 vs 一致性”取舍？
