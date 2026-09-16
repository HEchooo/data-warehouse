# 用户时长与访问频次

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 指标 | DAU | 日活、日活设备数、活跃设备数 |
| 指标 | MAU | 月活、月活设备数、月活跃设备数 |
| 指标 | dau_mau | DAU/MAU、用户粘性、Stickiness |
| 指标 | avg_duration_min | 人均停留时长、平均使用时长 |
| 指标 | avg_visit_freq | 人均访问频次、平均启动次数 |
| 指标 | next_day_retention_rate | 次日留存率、D1 留存率 |
| 指标 | day_7_retention_rate | 7 日留存率、D7 留存率 |
| 指标 | day_30_retention_rate | 30 日留存率、D30 留存率 |

## 适用范围

- 业务线：`Alvin's Club App`
- 适合：查询用户分群的 DAU、MAU、访问频次、使用时长和粘性。
- 不适合：不分用户段的全量日活和人均时长应使用[全量报表](../全量报表/fact.md)。
- 需要澄清：需要确认用户分群、日期范围，以及 DAU、MAU、频次或时长指标。

## 报表

### ads_daily_user_duration_frequency

- 表：`my-project-8584-jetonai.decom.ads_daily_user_duration_frequency`
- 状态：`active`
- 粒度：需求粒度为 `dt × user_segment`
- 时区：`America/Toronto`
- 数据范围：App 用户分群的活跃、时长和访问频次。
- 必要过滤：按目标 `user_segment` 过滤。
- 不包含：未分群的业务明细。
- Dashboard：[【AI-Fashion】用户时长与访问频次](https://bi.alvinclub.ca/superset/dashboard/95/)（id=95）

## 字段

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 用户分群 | `user_segment` | 随分群变化 | 固定取值 `全量设备` / `游客` / `已登录用户` / `新用户` / `老用户`；各分群统计主体见下方补充说明 | 按目标分群过滤 | 各分群统计主体不一致，不可相加或互相校验 |
| 日活 UV | `dau_uv` | 设备或用户（随分群） | 按当前 `user_segment` 的统计主体去重 | 排除 `send_push`、`send_email` 等被动触达事件 | 不可跨天或跨分群求和 |
| 总使用时长（分钟） | `total_duration_min` | 会话 | `app_launch` 的 `args_session_duration` 按 `COALESCE(raw_event_id, hash_id)` 去重后求和 | 事件名为 `app_launch` | 可跨天求和；不可跨分群求和 |
| 人均使用时长（分钟） | `avg_duration_min` | 设备或用户（随分群） | `total_duration_min / dau_uv`；分母为 0 时返回 `0` | 无 | 不可求和或平均；跨天须用汇总分子分母重算 |
| 启动次数 | `app_launch_count` | 事件 | 事件 `app_launch` 按 `COALESCE(raw_event_id, hash_id)` 去重 | 事件名为 `app_launch` | 可跨天求和；不可跨分群求和 |
| 人均访问频次 | `avg_visit_freq` | 设备或用户（随分群） | `app_launch_count / dau_uv`；分母为 0 时返回 `0` | 无 | 不可求和或平均；跨天须用汇总分子分母重算 |
| 次日留存率 | `next_day_retention_rate` | 当日活跃 cohort | cohort 在 `dt+1` 仍活跃的比例；分子只要求同一主体在观察日活跃，不要求仍属当日分群 | 排除被动触达事件；只取已走完观察日的 cohort | 未到观察日返回 `NULL`，不可当 `0`；不可跨天或跨分群求和、平均 |
| 7 日留存率 | `day_7_retention_rate` | 当日活跃 cohort | cohort 在 `dt+7` 仍活跃的比例 | 同上 | 同上 |
| 30 日留存率 | `day_30_retention_rate` | 当日活跃 cohort | cohort 在 `dt+30` 仍活跃的比例 | 同上 | 同上 |
| 月活 UV | `mau_uv` | 设备或用户（随分群） | 所在自然月的去重主体数 | 排除被动触达事件 | 已是自然月口径，不可按天相加，也不可跨月求和 |
| 粘性 | `dau_mau` | 设备或用户（随分群） | `dau_uv / mau_uv`；分母为 0 时返回 `0` | 无 | 不可求和或平均；跨天须用汇总分子分母重算 |

补充说明：
- `全量设备` 口径：不感知 `prop_user_id`，按 `NULLIF(prop_device_id,'')` 去重设备
- `游客` 口径：仅保留 `prop_user_id IS NULL OR prop_user_id = ''` 的事件，按 `NULLIF(prop_device_id,'')` 去重设备
- `已登录用户` 口径：仅保留 `prop_user_id IS NOT NULL AND prop_user_id != ''` 的事件，按 `NULLIF(prop_user_id,'')` 去重用户
- `新用户` 口径：仅保留已登录用户，且 `dim_user_first_active.first_active_date = dt`，按 `prop_user_id` 去重用户
- `老用户` 口径：仅保留已登录用户，且 `dim_user_first_active.first_active_date < dt`，按 `prop_user_id` 去重用户
- 留存 cohort 和 MAU 都按各自分群下的同一统计主体理解
- 活动事实已下沉到 `decom.dws_device_activity_daily`；`新用户` / `老用户` 两个分群的代码已补齐，生产回填完成前线上表只有 `全量设备` / `游客` / `已登录用户` 三类数据

## 依赖

- 上游：`decom.dws_device_activity_daily`, `decom.dim_user_first_active`
- 中间表：`decom.dws_device_activity_daily`, `decom.dim_user_first_active`
- 脚本：`jobs/ads/ads_daily_user_duration_frequency.py`
- DWS 脚本：`jobs/dws/dws_device_activity_daily.py`
- DDL：`ddl/ads/ads_daily_user_duration_frequency.sql`
- DWS DDL：`ddl/dws/dws_device_activity_daily.sql`
- 编排：独立表级入口 `jobs/ads/ads_daily_user_duration_frequency.py`
