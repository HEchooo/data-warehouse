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

| 字段 | 口径 |
|------|------|
| user_segment | 用户分群；固定取值为 `全量设备` / `游客` / `已登录用户` / `新用户` / `老用户` |
| dau_uv | 日活 UV；按当前 `user_segment` 的统计主体去重 |
| total_duration_min | 总使用时长（分钟）；`app_launch` 的 `args_session_duration` 按 `COALESCE(raw_event_id, hash_id)` 去重后求和 |
| avg_duration_min | `total_duration_min / dau_uv` |
| app_launch_count | 启动次数；事件：`app_launch`；按 `COALESCE(raw_event_id, hash_id)` 去重 |
| avg_visit_freq | `app_launch_count / dau_uv` |
| next_day_retention_rate | 当日活跃 cohort 在 dt+1 仍活跃的比例；未到期返回 NULL |
| day_7_retention_rate | 当日活跃 cohort 在 dt+7 仍活跃的比例；未到期返回 NULL |
| day_30_retention_rate | 当日活跃 cohort 在 dt+30 仍活跃的比例；未到期返回 NULL |
| mau_uv | 当月月活（自然月） |
| dau_mau | `dau_uv / mau_uv` |

补充说明：
- `全量设备` 口径：不感知 `prop_user_id`，按 `NULLIF(prop_device_id,'')` 去重设备
- `游客` 口径：仅保留 `prop_user_id IS NULL OR prop_user_id = ''` 的事件，按 `NULLIF(prop_device_id,'')` 去重设备
- `已登录用户` 口径：仅保留 `prop_user_id IS NOT NULL AND prop_user_id != ''` 的事件，按 `NULLIF(prop_user_id,'')` 去重用户
- `新用户` 口径：仅保留已登录用户，且 `dim_user_first_active.first_active_date = dt`，按 `prop_user_id` 去重用户
- `老用户` 口径：仅保留已登录用户，且 `dim_user_first_active.first_active_date < dt`，按 `prop_user_id` 去重用户
- `新用户` / `老用户` 的留存分母按当日 cohort 分群，未来留存分子只要求同一 `prop_user_id` 在观察日活跃，不要求未来仍属于当日分群
- `send_push`、`send_email` 属于被动触达事件，不计入任何分群的“活跃”与留存判定
- 人均值、粘性和留存率在分母为 0 时落 `0`；留存观察日未成熟时返回 `NULL`，两者不能互换
- 人均值、粘性和留存率不可跨天或跨分群求和、平均；必须用目标范围的分子、分母重算
- 留存 cohort 和 MAU 都按各自分群下的同一统计主体理解
- `全量设备`、`游客`、`已登录用户`、`新用户`、`老用户` 的统计主体不完全一致，分群结果不能直接相加校验
- Batch 6 的代码链路已把活动事实下沉到 `decom.dws_device_activity_daily`；生产切换与历史回填需单独执行

## 已落地字段

| 需求字段 | 当前已落地 | 说明 |
|------|------|------|
| `dt` | `dt` | 已在线上表落地 |
| `user_segment` | 已在线上表落地 | 原有 `全量设备` / `游客` / `已登录用户` 已有数据；`新用户` / `老用户` 代码已补齐，待生产回填或后续增量执行后进入 Superset filter 选项 |
| 其余指标字段 | 已落地 | 当前线上表已存在 `dau_uv`、`total_duration_min`、`avg_duration_min`、`app_launch_count`、`avg_visit_freq`、`next_day_retention_rate`、`day_7_retention_rate`、`day_30_retention_rate`、`mau_uv`、`dau_mau` |

## 依赖

- 上游：`decom.dws_device_activity_daily`, `decom.dim_user_first_active`
- 中间表：`decom.dws_device_activity_daily`, `decom.dim_user_first_active`
- 脚本：`jobs/ads/ads_daily_user_duration_frequency.py`
- DWS 脚本：`jobs/dws/dws_device_activity_daily.py`
- DDL：`ddl/ads/ads_daily_user_duration_frequency.sql`
- DWS DDL：`ddl/dws/dws_device_activity_daily.sql`
- 编排：独立表级入口 `jobs/ads/ads_daily_user_duration_frequency.py`
