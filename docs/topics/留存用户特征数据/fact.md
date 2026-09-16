# 留存用户特征数据

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | 留存用户特征数据 | 留存用户的特征数据、留存特征看板、留存 vs 非留存用户行为对比 |
| 指标 | 次日留存用户数 | D1 留存人数 |
| 指标 | 7 日留存用户数 | D7 留存人数 |

## 适用范围

- 业务线：`Alvin's Club App`
- 适合：比较留存与非留存用户的行为均值，以及时长、关注、试穿和商详浏览分布。
- 不适合：平台整体留存率应使用[全量报表](../全量报表/fact.md)或[用增报表](../用增报表/fact.md)。
- 需要澄清：需要确认次日或 7 日留存、用户组、特征或分桶维度和日期。

## 报表

### 留存用户与非留存用户特征对比

- 表：`my-project-8584-jetonai.decom.ads_daily_retention_user_feature`
- 状态：`active`
- 粒度：`dt × user_group × feature_name`
- 时区：`America/Toronto`
- 数据范围：留存与非留存用户行为特征对比。
- 必要过滤：按目标 `user_group` 和 `feature_name` 过滤。
- 不包含：分桶分布。
- Dashboard：[【AI-Fashion】留存用户的特征数据](https://bi.alvinclub.ca/superset/dashboard/103/)（id=103）
- Chart：`留存用户与非留存用户特征对比`（id=581）

### 留存与非留存用户时长分布

- 表：`my-project-8584-jetonai.decom.ads_daily_retention_user_duration_bucket`
- 状态：`active`
- 粒度：`dt × user_group × duration_bucket`
- 时区：`America/Toronto`
- 数据范围：留存与非留存用户使用时长分布。
- 必要过滤：按目标 `user_group` 过滤。
- 不包含：其他特征分桶。
- Dashboard：[【AI-Fashion】留存用户的特征数据](https://bi.alvinclub.ca/superset/dashboard/103/)（id=103）
- 筛选器：复用 dashboard 103 现有 `日期`、`用户群` 原生 filter

### 留存与非留存用户累计关注账号数分布

- 表：`my-project-8584-jetonai.decom.ads_daily_retention_user_follow_bucket`
- 状态：`active`
- 粒度：`dt × user_group × follow_account_bucket`
- 时区：`America/Toronto`
- 数据范围：留存与非留存用户累计关注账号数分布。
- 必要过滤：按目标 `user_group` 过滤。
- 不包含：新增关注分桶。
- Dashboard：[【AI-Fashion】留存用户的特征数据](https://bi.alvinclub.ca/superset/dashboard/103/)（id=103）
- 筛选器：复用 dashboard 103 现有 `日期`、`用户群` 原生 filter

### 留存与非留存用户新增关注账号数分布

- 表：`my-project-8584-jetonai.decom.ads_daily_retention_user_new_follow_bucket`
- 状态：`active`
- 粒度：`dt × user_group × new_follow_account_bucket`
- 时区：`America/Toronto`
- 数据范围：留存与非留存用户新增关注账号数分布。
- 必要过滤：按目标 `user_group` 过滤。
- 不包含：累计关注分桶。
- Dashboard：[【AI-Fashion】留存用户的特征数据](https://bi.alvinclub.ca/superset/dashboard/103/)（id=103）
- 筛选器：复用 dashboard 103 现有 `日期`、`用户群` 原生 filter

### 留存与非留存用户 AI 试穿次数分布

- 表：`my-project-8584-jetonai.decom.ads_daily_retention_user_tryon_bucket`
- 状态：`active`
- 粒度：`dt × user_group × tryon_bucket`
- 时区：`America/Toronto`
- 数据范围：留存与非留存用户 AI 试穿次数分布。
- 必要过滤：按目标 `user_group` 过滤。
- 不包含：其他特征分桶。
- Dashboard：[【AI-Fashion】留存用户的特征数据](https://bi.alvinclub.ca/superset/dashboard/103/)（id=103）
- 筛选器：复用 dashboard 103 现有 `日期`、`用户群` 原生 filter

### 留存与非留存用户浏览商详次数分布

- 表：`my-project-8584-jetonai.decom.ads_daily_retention_user_product_detail_bucket`
- 状态：`active`
- 粒度：`dt × user_group × product_detail_bucket`
- 时区：`America/Toronto`
- 数据范围：留存与非留存用户商详浏览次数分布。
- 必要过滤：按目标 `user_group` 过滤。
- 不包含：其他特征分桶。
- Dashboard：[【AI-Fashion】留存用户的特征数据](https://bi.alvinclub.ca/superset/dashboard/103/)（id=103）
- 筛选器：复用 dashboard 103 现有 `日期`、`用户群` 原生 filter

## 字段

### 主题共用口径

cohort 为 `dt` 当日活跃的已登录用户；当日活跃与观察日活跃都排除 `send_push`、`send_email` 这类被动触达事件。六张报表复用同一 cohort 与同一组留存判定。

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 用户群 | `user_group` | 已登录用户 | 固定取值 `新用户 / 老用户 / 全量登录用户`；新用户 = `first_active_date = dt`，老用户 = `first_active_date < dt`；按用户粒度从 `decom.dws_user_activity_daily` 收敛，不复用平台粒度 `is_new_user` | 按目标用户群过滤 | `全量登录用户` 与新老用户行不可一起聚合 |
| 次日留存用户数 | `next_day_retained_user_count` | 已登录用户 | `dt` 当日活跃且 `dt+1` 仍活跃的去重用户数 | 无 | 观察日未成熟返回 `NULL`，不可当 `0`；不可跨天或跨用户群求和 |
| 7 日留存用户数 | `day_7_retained_user_count` | 已登录用户 | `dt` 当日活跃且 `dt+7` 仍活跃的去重用户数 | 无 | 观察日未成熟返回 `NULL`，不可当 `0`；不可跨天或跨用户群求和 |
| 次日非留存用户数 | `next_day_not_retained_user_count` | 已登录用户 | `dt` 当日活跃且 `dt+1` 未活跃的去重用户数 | 无 | 观察日未成熟返回 `NULL`，不可当 `0`；不可跨天或跨用户群求和 |
| 7 日非留存用户数 | `day_7_not_retained_user_count` | 已登录用户 | `dt` 当日活跃且 `dt+7` 未活跃的去重用户数 | 无 | 观察日未成熟返回 `NULL`，不可当 `0`；不可跨天或跨用户群求和 |

### 留存用户与非留存用户特征对比

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 特征名称与顺序 | `feature_name` / `feature_order` | 行为特征 | 固定 8 个特征，`feature_order` 为 `1-8`：日均停留时长、AI试穿、关注账号、浏览商详、创建模特、搜索、衣橱渗透、非衣橱渗透 | 按目标 `feature_name` 过滤 | 不同特征行不可相加 |
| 次日留存用户特征值 | `next_day_retained_value` | 已登录用户 / 秒 | 次日留存 cohort 在 `dt` 当日的特征值：`日均停留时长` 行为平均秒数，其余行为做过该行为的去重用户数 | 无 | 观察日未成熟返回 `NULL`，不可当 `0`；不可跨天或跨用户群求和 |
| 7 日留存用户特征值 | `day_7_retained_value` | 已登录用户 / 秒 | 同上，观察日为 `dt+7` | 无 | 观察日未成熟返回 `NULL`，不可当 `0`；不可跨天或跨用户群求和 |
| 次日非留存用户特征值 | `next_day_not_retained_value` | 已登录用户 / 秒 | 同上，对象为次日未回访 cohort | 无 | 观察日未成熟返回 `NULL`，不可当 `0`；不可跨天或跨用户群求和 |
| 7 日非留存用户特征值 | `day_7_not_retained_value` | 已登录用户 / 秒 | 同上，对象为 7 日未回访 cohort | 无 | 观察日未成熟返回 `NULL`，不可当 `0`；不可跨天或跨用户群求和 |
| 停留时长分子 | `next_day_retained_duration_sum_sec` / `day_7_retained_duration_sum_sec` / `next_day_not_retained_duration_sum_sec` / `day_7_not_retained_duration_sum_sec` | 秒 | 仅 `feature_name = '日均停留时长'` 行写入，为对应 cohort 在 `dt` 当日的停留时长总和 | `feature_name = '日均停留时长'` | 可跨天求和；跨天平均值须按 `SUM(分子) / SUM(对应用户数)` 重算，不可 `AVG` 单日平均值 |

四个人数字段在同一 `dt × user_group` 下会重复写入到每个特征行，供展示层校验分母；按特征行相加会重复计数。

行为特征的判定口径（布尔，当日发生至少 1 次即计入）：

| 特征 | 判定 |
|---|---|
| 日均停留时长 | `app_launch.args_session_duration` 按 `COALESCE(raw_event_id, hash_id)` 去重后汇总，单位秒 |
| AI试穿 | `v3_decom.common_ext_ai_task.task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')` |
| 关注账号 | `v3_decom.community_star_follow` |
| 浏览商详 | `event_name = 'v_product_detail'` |
| 创建模特 | `v3_decom.common_ext_ai_task.task_type = 'MODEL'` |
| 搜索 | `event_name IN ('c_home_search', 'c_shop_search')` |
| 衣橱渗透 | 同日存在 `event_name IN ('v_closet', 'v_smart_closet_home')`，且 `v3_decom.user_fashion_closet` 当日存在 `type IN ('SINGLE', 'OUTFIT')` |
| 非衣橱渗透 | 同一 cohort 下不满足衣橱渗透条件的补集 |

### 五张分桶报表

五张分桶表结构一致：`桶名称`、`桶排序` 两个维度字段，加上主题共用的四个留存 / 非留存人数字段（口径、必要过滤和聚合限制与共用口径一致，只是统计范围收敛到桶内）。桶排序字段仅用于展示排序，不参与聚合。

| 报表 | 桶字段 | 分桶定义 | 是否含 `0` 值用户 |
|---|---|---|---|
| `ads_daily_retention_user_duration_bucket` | `duration_bucket` / `duration_bucket_order` | 当日停留时长按分钟分为 `(0~1]`、`(1~3]`、`(3~5]`、`(5~10]`、`(10~15]`、`15分钟以上`；时长取 `app_launch.args_session_duration` 按 `COALESCE(raw_event_id, hash_id)` 去重后汇总 | 否，排除 `0` 秒用户 |
| `ads_daily_retention_user_follow_bucket` | `follow_account_bucket` / `follow_account_bucket_order` | 当日关注去重账号数分为 `(0~3]`、`(3~5]`、`(5~10]`、`(11~20]`、`20次以上`；取 `dws_user_activity_daily.followed_account_count` | 否，排除当日 `0` 关注用户 |
| `ads_daily_retention_user_new_follow_bucket` | `new_follow_account_bucket` / `new_follow_account_bucket_order` | 同一 `followed_account_count` 字段，分桶为 `0`、`(1~2]`、`(3~5]`、`5个以上` | 是 |
| `ads_daily_retention_user_tryon_bucket` | `tryon_bucket` / `tryon_bucket_order` | 当日 AI 试穿任务次数分为 `(0~3]`、`(3~7]`、`(7~10]`、`(10~15]`、`15次以上`；取 `dws_user_activity_daily.ai_tryon_count` | 否，排除 `0` 次试穿用户 |
| `ads_daily_retention_user_product_detail_bucket` | `product_detail_bucket` / `product_detail_bucket_order` | 当日商详浏览次数分为 `(0~0]`、`(1~3]`、`(3~7]`、`(7~10]`、`10次以上`；取 `dws_user_activity_daily.product_detail_view_count` | 是 |

排除 `0` 值用户的分桶表，各桶人数之和默认不回对到特征对比表的 cohort 总人数；含 `0` 值用户的两张表应当回对得上。

补充说明：

- 当前主题只统计已登录用户，不含游客设备
- cohort 主体先沉到 `decom.dws_user_activity_daily`，再分别聚合到 `decom.dws_user_retention_feature_daily` 和 `decom.ads_daily_retention_user_duration_bucket`
- 事件侧行为 `浏览商详 / 搜索 / 停留时长 / 衣橱首页曝光` 先沉到 `decom.dws_user_activity_daily`
- `衣橱渗透` 的留存主题口径为“同日 `v_closet` / `v_smart_closet_home` + 同日 `type IN ('SINGLE', 'OUTFIT')` 的 cohort 用户数”；这里不直接复用 `【AI-Fashion】衣橱概览` 中 `type = 'SINGLE'` 的分子定义
  - 衣橱首页曝光埋点 2026-07-07 前后由 `v_closet` 迁移到 `v_smart_closet_home`（约 07-10 完成），口径同时兼容新旧事件；否则切换后渗透 cohort 归零、次日留存会假性掉到 0
- `dws_user_activity_daily.followed_account_count` 表示 cohort 用户在 `dt` 当天关注的去重账号数；新 chart 虽沿用“累计关注账号数”展示名，但实际按该字段分桶
- `dws_user_activity_daily.product_detail_view_count` 表示 cohort 用户在 `dt` 当天浏览商详的去重事件次数（`event_name = 'v_product_detail'`）；用于“浏览商详次数分桶”chart
- `dws_user_activity_daily.ai_tryon_count` 表示 cohort 用户在 `dt` 当天 `task_type IN ('QUICK_TRY_ON','TRY_ON','OUTFIT')` 的服务端任务行数；和特征对比的 `used_ai_tryon` 布尔指标不是同一个字段，不要混用
- 生产可用性：`ads_daily_retention_user_feature` 与 `ads_daily_retention_user_new_follow_bucket` 已建表并有生产数据（后者已回刷 `2026-05-01 ~ 2026-05-25`）；`duration` / `follow` / `tryon` / `product_detail` 四张分桶表的代码、DDL 与调度已就绪，生产表尚未创建，查询前先确认表是否存在

## 依赖

- 上游：`decom.dws_user_retention_feature_daily`, `decom.dws_user_activity_daily`
- 中间表：`decom.dws_user_activity_daily`, `decom.dws_user_retention_feature_daily`, `decom.dws_user_daily`, `decom.dwd_event_log`, `v3_decom.common_ext_ai_task`, `v3_decom.community_star_follow`, `v3_decom.user_fashion_closet`
- 报表表：`decom.ads_daily_retention_user_feature`, `decom.ads_daily_retention_user_duration_bucket`, `decom.ads_daily_retention_user_follow_bucket`, `decom.ads_daily_retention_user_new_follow_bucket`, `decom.ads_daily_retention_user_tryon_bucket`, `decom.ads_daily_retention_user_product_detail_bucket`
- Superset：物理 dataset `ads_daily_retention_user_feature`（id=232），chart `留存用户与非留存用户特征对比`（id=581），dashboard `【AI-Fashion】留存用户的特征数据`（id=103）；新增关注账号数分桶 chart 物理 dataset `ads_daily_retention_user_new_follow_bucket`（id=248）+ chart `留存与非留存用户新增关注账号数分布`（id=626）已挂在 dashboard 103 上
- 脚本：`jobs/ads/ads_daily_retention_user_feature.py`, `jobs/ads/ads_daily_retention_user_duration_bucket.py`, `jobs/ads/ads_daily_retention_user_follow_bucket.py`, `jobs/ads/ads_daily_retention_user_new_follow_bucket.py`, `jobs/ads/ads_daily_retention_user_tryon_bucket.py`, `jobs/ads/ads_daily_retention_user_product_detail_bucket.py`
- DWS 脚本：`jobs/dws/dws_user_activity_daily.py`, `jobs/dws/dws_user_retention_feature_daily.py`
- DDL：`ddl/ads/ads_daily_retention_user_feature.sql`, `ddl/ads/ads_daily_retention_user_duration_bucket.sql`, `ddl/ads/ads_daily_retention_user_follow_bucket.sql`, `ddl/ads/ads_daily_retention_user_new_follow_bucket.sql`, `ddl/ads/ads_daily_retention_user_tryon_bucket.sql`, `ddl/ads/ads_daily_retention_user_product_detail_bucket.sql`
- DWS DDL：`ddl/dws/dws_user_activity_daily.sql`, `ddl/dws/dws_user_retention_feature_daily.sql`
- 编排：独立表级入口 `jobs/ads/ads_daily_retention_user_feature.py`, `jobs/ads/ads_daily_retention_user_duration_bucket.py`, `jobs/ads/ads_daily_retention_user_follow_bucket.py`, `jobs/ads/ads_daily_retention_user_new_follow_bucket.py`, `jobs/ads/ads_daily_retention_user_tryon_bucket.py`, `jobs/ads/ads_daily_retention_user_product_detail_bucket.py`
