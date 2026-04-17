# 留存用户特征数据

## 报表

- 表：`ads_daily_retention_user_feature`
- 粒度：`dt × user_group × feature_name`
- Dashboard：[【AI-Fashion】留存用户的特征数据](https://bi.alvinclub.ca/superset/dashboard/103/)（id=103）
- 首个 Chart：`留存用户与非留存用户特征对比`（id=581）

## 字段

| 需求字段 | 定义 |
|------|------|
| 日期 | cohort 日期；以 `dt` 当日活跃的已登录用户为观察对象 |
| 用户群 | `新用户 / 老用户 / 全量登录用户`；新用户 = `first_active_date = dt`，老用户 = `first_active_date < dt` |
| 次日留存用户 | `dt` 当日活跃、`dt+1` 仍活跃的已登录用户 |
| 7日留存用户 | `dt` 当日活跃、`dt+7` 仍活跃的已登录用户 |
| 次日非留存用户 | `dt` 当日活跃、`dt+1` 未活跃的已登录用户 |
| 7日非留存用户 | `dt` 当日活跃、`dt+7` 未活跃的已登录用户 |
| 日均停留时长 | 对应 cohort 用户在 `dt` 当日的平均停留时长；仅统计 `app_launch.args_session_duration`，按 `COALESCE(raw_event_id, hash_id)` 去重后汇总，单位秒 |
| AI试穿 | 对应 cohort 用户中，在 `dt` 当日发生过 `v3_decom.common_ext_ai_task.task_type IN ('TRY_ON', 'STYLE_ME')` 的去重用户数 |
| 关注账号 | 对应 cohort 用户中，在 `dt` 当日发生过 `v3_decom.community_star_follow` 的去重用户数 |
| 浏览商详 | 对应 cohort 用户中，在 `dt` 当日发生过 `event_name = 'v_product_detail'` 的去重用户数 |
| 创建模特 | 对应 cohort 用户中，在 `dt` 当日发生过 `v3_decom.common_ext_ai_task.task_type = 'MODEL'` 的去重用户数 |
| 搜索 | 对应 cohort 用户中，在 `dt` 当日发生过 `event_name IN ('c_home_search', 'c_shop_search')` 的去重用户数 |

## 已落地字段

| 需求字段 | 落地表 | 落地字段 | 口径 |
|------|------|------|------|
| 用户群 | `decom.ads_daily_retention_user_feature` | `user_group` | `新用户 / 老用户 / 全量登录用户`；按用户粒度从 `decom.dws_user_daily` 收敛，不直接复用平台粒度 `is_new_user` |
| 指标行顺序 | `decom.ads_daily_retention_user_feature` | `feature_order` | 固定为 `1-6`，对应停留时长、AI试穿、关注、商详、创建模特、搜索 |
| 指标名称 | `decom.ads_daily_retention_user_feature` | `feature_name` | 当前固定输出 6 个特征项，供 Superset 直接展示 |
| 次日留存用户值 | `decom.ads_daily_retention_user_feature` | `next_day_retained_value` | 若 `dt+1` 的观察日尚未开始则返回 `NULL`；观察日开始后按实时回访进度滚动更新。停留时长行为平均秒数，其余行为为做过该行为的去重用户数 |
| 7日留存用户值 | `decom.ads_daily_retention_user_feature` | `day_7_retained_value` | 若 `dt+7` 的观察日尚未开始则返回 `NULL`；观察日开始后按实时回访进度滚动更新。停留时长行为平均秒数，其余行为为做过该行为的去重用户数 |
| 次日非留存用户值 | `decom.ads_daily_retention_user_feature` | `next_day_not_retained_value` | 若 `dt+1` 的观察日尚未开始则返回 `NULL`；观察日开始后按实时未回访进度滚动更新。停留时长行为平均秒数，其余行为为做过该行为的去重用户数 |
| 7日非留存用户值 | `decom.ads_daily_retention_user_feature` | `day_7_not_retained_value` | 若 `dt+7` 的观察日尚未开始则返回 `NULL`；观察日开始后按实时未回访进度滚动更新。停留时长行为平均秒数，其余行为为做过该行为的去重用户数 |
| 次日留存用户数 | `decom.ads_daily_retention_user_feature` | `next_day_retained_user_count` | `dt` 当日活跃、`dt+1` 仍活跃的已登录去重用户数；在同一 `dt × user_group` 下会重复写入到每个指标行，供展示层校验分母 |
| 7日留存用户数 | `decom.ads_daily_retention_user_feature` | `day_7_retained_user_count` | `dt` 当日活跃、`dt+7` 仍活跃的已登录去重用户数；在同一 `dt × user_group` 下会重复写入到每个指标行，供展示层校验分母 |
| 次日非留存用户数 | `decom.ads_daily_retention_user_feature` | `next_day_not_retained_user_count` | `dt` 当日活跃、`dt+1` 未活跃的已登录去重用户数；在同一 `dt × user_group` 下会重复写入到每个指标行，供展示层校验分母 |
| 7日非留存用户数 | `decom.ads_daily_retention_user_feature` | `day_7_not_retained_user_count` | `dt` 当日活跃、`dt+7` 未活跃的已登录去重用户数；在同一 `dt × user_group` 下会重复写入到每个指标行，供展示层校验分母 |
| 次日留存时长分子 | `decom.ads_daily_retention_user_feature` | `next_day_retained_duration_sum_sec` | 仅 `feature_name = '日均停留时长'` 时写入；值为次日留存 cohort 在 `dt` 当日的停留时长总和（秒），供展示层做 `SUM(分子) / SUM(分母)` |
| 7日留存时长分子 | `decom.ads_daily_retention_user_feature` | `day_7_retained_duration_sum_sec` | 仅 `feature_name = '日均停留时长'` 时写入；值为 7 日留存 cohort 在 `dt` 当日的停留时长总和（秒），供展示层做 `SUM(分子) / SUM(分母)` |
| 次日非留存时长分子 | `decom.ads_daily_retention_user_feature` | `next_day_not_retained_duration_sum_sec` | 仅 `feature_name = '日均停留时长'` 时写入；值为次日非留存 cohort 在 `dt` 当日的停留时长总和（秒），供展示层做 `SUM(分子) / SUM(分母)` |
| 7日非留存时长分子 | `decom.ads_daily_retention_user_feature` | `day_7_not_retained_duration_sum_sec` | 仅 `feature_name = '日均停留时长'` 时写入；值为 7 日非留存 cohort 在 `dt` 当日的停留时长总和（秒），供展示层做 `SUM(分子) / SUM(分母)` |

补充说明：

- 当前主题只统计已登录用户，不含游客设备
- cohort 主体来自 `decom.dws_user_daily`，按 `dt + prop_user_id` 去重后再做新老用户拆分
- 服务端行为 `AI试穿 / 关注账号 / 创建模特` 分别来自 `v3_decom.common_ext_ai_task`、`v3_decom.community_star_follow`、`v3_decom.common_ext_ai_task`
- 事件侧行为 `浏览商详 / 搜索` 来自 `decom.dwd_event_log`
- 行为类指标采用布尔判定口径：用户在 `dt` 当日只要发生过至少 1 次，就记入该行为人数
- `日均停留时长` 为了支持跨天聚合，除单日平均值外，额外落地 4 个时长分子字段；报表层应使用 `SUM(分子) / SUM(对应用户数)` 计算区间平均值
- 当前已完成生产表字段扩展、Superset 资产更新，并已回刷 `2026-04-01 ~ 2026-04-17` 历史分区
- 若要继续补更早历史数据，需要单独确认回填范围和执行方案

## 依赖

- 上游：`decom.dws_user_daily`, `decom.dwd_event_log`, `v3_decom.common_ext_ai_task`, `v3_decom.community_star_follow`
- 报表表：`decom.ads_daily_retention_user_feature`
- Superset：物理 dataset `ads_daily_retention_user_feature`（id=232），chart `留存用户与非留存用户特征对比`（id=581），dashboard `【AI-Fashion】留存用户的特征数据`（id=103）
- 脚本：`jobs/ads/ads_daily_retention_user_feature.py`
- DDL：`ddl/ads/ads_daily_retention_user_feature.sql`
- 编排：`jobs/ads/ads_daily.py`
