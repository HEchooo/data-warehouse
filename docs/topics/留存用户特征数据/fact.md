# 留存用户特征数据

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | 留存用户特征数据 | 留存用户的特征数据、留存特征看板、留存 vs 非留存用户行为对比 |
| 指标 | 次日留存用户数 | D1 留存人数、1 日留存用户数 |
| 指标 | 7 日留存用户数 | D7 留存人数、第 7 日留存用户数 |
| 指标 | 日均停留时长 | 人均停留时长、平均使用时长 |
| 指标 | AI 试穿 | 试穿用户、Try On 用户 |

## 报表

### 留存用户与非留存用户特征对比

- 表：`ads_daily_retention_user_feature`
- 粒度：`dt × user_group × feature_name`
- Dashboard：[【AI-Fashion】留存用户的特征数据](https://bi.alvinclub.ca/superset/dashboard/103/)（id=103）
- Chart：`留存用户与非留存用户特征对比`（id=581）

### 留存与非留存用户时长分布

- 表：`ads_daily_retention_user_duration_bucket`
- 粒度：`dt × user_group × duration_bucket`
- Dashboard：[【AI-Fashion】留存用户的特征数据](https://bi.alvinclub.ca/superset/dashboard/103/)（id=103）
- 筛选器：复用 dashboard 103 现有 `日期`、`用户群` 原生 filter

### 留存与非留存用户累计关注账号数分布

- 表：`ads_daily_retention_user_follow_bucket`
- 粒度：`dt × user_group × follow_account_bucket`
- Dashboard：[【AI-Fashion】留存用户的特征数据](https://bi.alvinclub.ca/superset/dashboard/103/)（id=103）
- 筛选器：复用 dashboard 103 现有 `日期`、`用户群` 原生 filter

### 留存与非留存用户新增关注账号数分布

- 表：`ads_daily_retention_user_new_follow_bucket`
- 粒度：`dt × user_group × new_follow_account_bucket`
- Dashboard：[【AI-Fashion】留存用户的特征数据](https://bi.alvinclub.ca/superset/dashboard/103/)（id=103）
- 筛选器：复用 dashboard 103 现有 `日期`、`用户群` 原生 filter

### 留存与非留存用户 AI 试穿次数分布

- 表：`ads_daily_retention_user_tryon_bucket`
- 粒度：`dt × user_group × tryon_bucket`
- Dashboard：[【AI-Fashion】留存用户的特征数据](https://bi.alvinclub.ca/superset/dashboard/103/)（id=103）
- 筛选器：复用 dashboard 103 现有 `日期`、`用户群` 原生 filter

### 留存与非留存用户浏览商详次数分布

- 表：`ads_daily_retention_user_product_detail_bucket`
- 粒度：`dt × user_group × product_detail_bucket`
- Dashboard：[【AI-Fashion】留存用户的特征数据](https://bi.alvinclub.ca/superset/dashboard/103/)（id=103）
- 筛选器：复用 dashboard 103 现有 `日期`、`用户群` 原生 filter

## 字段

### 主题共用口径

| 需求字段 | 定义 |
|------|------|
| 日期 | cohort 日期；以 `dt` 当日活跃的已登录用户为观察对象 |
| 用户群 | `新用户 / 老用户 / 全量登录用户`；新用户 = `first_active_date = dt`，老用户 = `first_active_date < dt` |
| 次日留存用户数 | `dt` 当日活跃、`dt+1` 仍活跃的已登录用户 |
| 7日留存用户数 | `dt` 当日活跃、`dt+7` 仍活跃的已登录用户 |
| 次日非留存用户数 | `dt` 当日活跃、`dt+1` 未活跃的已登录用户 |
| 7日非留存用户数 | `dt` 当日活跃、`dt+7` 未活跃的已登录用户 |

补充说明：
- `dt` 当日活跃与未来观察日活跃，都会排除 `send_push`、`send_email` 这类被动触达事件

### 留存用户与非留存用户特征对比

| 需求字段 | 定义 |
|------|------|
| 日均停留时长 | 对应 cohort 用户在 `dt` 当日的平均停留时长；仅统计 `app_launch.args_session_duration`，按 `COALESCE(raw_event_id, hash_id)` 去重后汇总，单位秒 |
| AI试穿 | 对应 cohort 用户中，在 `dt` 当日发生过 `v3_decom.common_ext_ai_task.task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')` 的去重用户数 |
| 关注账号 | 对应 cohort 用户中，在 `dt` 当日发生过 `v3_decom.community_star_follow` 的去重用户数 |
| 浏览商详 | 对应 cohort 用户中，在 `dt` 当日发生过 `event_name = 'v_product_detail'` 的去重用户数 |
| 创建模特 | 对应 cohort 用户中，在 `dt` 当日发生过 `v3_decom.common_ext_ai_task.task_type = 'MODEL'` 的去重用户数 |
| 搜索 | 对应 cohort 用户中，在 `dt` 当日发生过 `event_name IN ('c_home_search', 'c_shop_search')` 的去重用户数 |
| 衣橱渗透 | 对应 cohort 用户中，在 `dt` 当日同时满足两件事的去重用户数：日志中存在 `event_name IN ('v_closet', 'v_smart_closet_home')`，且 `v3_decom.user_fashion_closet` 当日存在 `type IN ('SINGLE', 'OUTFIT')` |
| 非衣橱渗透 | 对应 cohort 用户中，在 `dt` 当日不满足“衣橱渗透”条件的去重用户数；等价于 cohort 总人数减去衣橱渗透用户数 |

### 留存与非留存用户时长分布

| 需求字段 | 定义 |
|------|------|
| 时长 | 对应 cohort 用户在 `dt` 当日的停留时长分桶；只统计 `duration_sec > 0` 的已登录用户；其中 `duration_sec` 为 `app_launch.args_session_duration` 按 `COALESCE(raw_event_id, hash_id)` 去重后汇总的秒数，再按分钟桶映射到 `(0~1]`、`(1~3]`、`(3~5]`、`(5~10]`、`(10~15]`、`15分钟以上` |
| 次日留存用户数 | 对应时长桶内，`dt` 当日活跃且 `dt+1` 仍活跃的已登录去重用户数 |
| 7日留存用户数 | 对应时长桶内，`dt` 当日活跃且 `dt+7` 仍活跃的已登录去重用户数 |
| 次日非留存用户数 | 对应时长桶内，`dt` 当日活跃且 `dt+1` 未活跃的已登录去重用户数 |
| 7日非留存用户数 | 对应时长桶内，`dt` 当日活跃且 `dt+7` 未活跃的已登录去重用户数 |

### 留存与非留存用户累计关注账号数分布

| 需求字段 | 定义 |
|------|------|
| 累计关注账号数 | 展示名沿用“累计关注账号数”，实际口径为 cohort 用户在 `dt` 当天关注的去重账号数分桶；关注来源 `v3_decom.community_star_follow`，按 `dt × user_id` 统计 `COUNT(DISTINCT star_user_id)`；只统计当日关注账号数大于 `0` 的已登录用户；分桶为 `(0~3]`、`(3~5]`、`(5~10]`、`(11~20]`、`20次以上` |
| 次日留存用户数 | 对应累计关注账号数桶内，`dt` 当日活跃且 `dt+1` 仍活跃的已登录去重用户数 |
| 7日留存用户数 | 对应累计关注账号数桶内，`dt` 当日活跃且 `dt+7` 仍活跃的已登录去重用户数 |
| 次日非留存用户数 | 对应累计关注账号数桶内，`dt` 当日活跃且 `dt+1` 未活跃的已登录去重用户数 |
| 7日非留存用户数 | 对应累计关注账号数桶内，`dt` 当日活跃且 `dt+7` 未活跃的已登录去重用户数 |

### 留存与非留存用户新增关注账号数分布

| 需求字段 | 定义 |
|------|------|
| 新增关注账号数 | cohort 用户在 `dt` 当天关注的去重账号数分桶；关注来源 `v3_decom.community_star_follow.star_user_id`，按 `dt × user_id` 统计 `COUNT(DISTINCT star_user_id)`；与“累计关注账号数”chart 共用同一 `dws_user_activity_daily.followed_account_count` 字段；**包含当日关注数为 `0` 的已登录用户**，分桶为 `0`、`(1~2]`、`(3~5]`、`5个以上` |
| 次日留存用户数 | 对应新增关注账号数桶内，`dt` 当日活跃且 `dt+1` 仍活跃的已登录去重用户数 |
| 7日留存用户数 | 对应新增关注账号数桶内，`dt` 当日活跃且 `dt+7` 仍活跃的已登录去重用户数 |
| 次日非留存用户数 | 对应新增关注账号数桶内，`dt` 当日活跃且 `dt+1` 未活跃的已登录去重用户数 |
| 7日非留存用户数 | 对应新增关注账号数桶内,`dt` 当日活跃且 `dt+7` 未活跃的已登录去重用户数 |

### 留存与非留存用户 AI 试穿次数分布

| 需求字段 | 定义 |
|------|------|
| AI试穿使用次数 | 对应 cohort 用户在 `dt` 当天的 AI 试穿任务次数分桶；试穿来源 `v3_decom.common_ext_ai_task`，按 `task_type IN ('QUICK_TRY_ON','TRY_ON','OUTFIT')` 与 cohort `user_id` 关联后取 `COUNT(1)`；只统计当日试穿次数大于 `0` 的已登录用户；分桶为 `(0~3]`、`(3~7]`、`(7~10]`、`(10~15]`、`15次以上` |
| 次日留存用户数 | 对应 AI 试穿次数桶内，`dt` 当日活跃且 `dt+1` 仍活跃的已登录去重用户数 |
| 7日留存用户数 | 对应 AI 试穿次数桶内，`dt` 当日活跃且 `dt+7` 仍活跃的已登录去重用户数 |
| 次日非留存用户数 | 对应 AI 试穿次数桶内，`dt` 当日活跃且 `dt+1` 未活跃的已登录去重用户数 |
| 7日非留存用户数 | 对应 AI 试穿次数桶内，`dt` 当日活跃且 `dt+7` 未活跃的已登录去重用户数 |

### 留存与非留存用户浏览商详次数分布

| 需求字段 | 定义 |
|------|------|
| 浏览商详次数 | 对应 cohort 用户在 `dt` 当天的商详浏览次数分桶；来源 `event_name = 'v_product_detail'`，按 `COALESCE(raw_event_id, hash_id)` 去重后统计用户当日次数；**包含当日浏览次数为 `0` 的已登录用户**，分桶为 `(0~0]`、`(1~3]`、`(3~7]`、`(7~10]`、`10次以上` |
| 次日留存用户数 | 对应商详浏览次数桶内，`dt` 当日活跃且 `dt+1` 仍活跃的已登录去重用户数 |
| 7日留存用户数 | 对应商详浏览次数桶内，`dt` 当日活跃且 `dt+7` 仍活跃的已登录去重用户数 |
| 次日非留存用户数 | 对应商详浏览次数桶内，`dt` 当日活跃且 `dt+1` 未活跃的已登录去重用户数 |
| 7日非留存用户数 | 对应商详浏览次数桶内，`dt` 当日活跃且 `dt+7` 未活跃的已登录去重用户数 |

## 已落地字段

### 留存用户与非留存用户特征对比

| 需求字段 | 落地表 | 落地字段 | 口径 |
|------|------|------|------|
| 用户群 | `decom.ads_daily_retention_user_feature` | `user_group` | `新用户 / 老用户 / 全量登录用户`；按用户粒度从 `decom.dws_user_activity_daily` 收敛，不直接复用平台粒度 `is_new_user` |
| 指标行顺序 | `decom.ads_daily_retention_user_feature` | `feature_order` | 固定为 `1-8`，对应停留时长、AI试穿、关注、商详、创建模特、搜索、衣橱渗透、非衣橱渗透 |
| 指标名称 | `decom.ads_daily_retention_user_feature` | `feature_name` | 当前固定输出 8 个特征项，供 Superset 直接展示 |
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

### 留存与非留存用户时长分布

| 需求字段 | 落地表 | 落地字段 | 口径 |
|------|------|------|------|
| 用户群 | `decom.ads_daily_retention_user_duration_bucket` | `user_group` | `新用户 / 老用户 / 全量登录用户`；与特征对比报表完全复用同一 cohort 分群 |
| 时长桶排序 | `decom.ads_daily_retention_user_duration_bucket` | `duration_bucket_order` | 固定为 `1-6`，对应 `(0~1]`、`(1~3]`、`(3~5]`、`(5~10]`、`(10~15]`、`15分钟以上` |
| 时长桶名称 | `decom.ads_daily_retention_user_duration_bucket` | `duration_bucket` | 展示层直接使用的时长分桶标签 |
| 次日留存用户数 | `decom.ads_daily_retention_user_duration_bucket` | `next_day_retained_user_count` | 若 `dt+1` 的观察日尚未开始则返回 `NULL`；观察日开始后统计桶内次日留存去重用户数 |
| 7日留存用户数 | `decom.ads_daily_retention_user_duration_bucket` | `day_7_retained_user_count` | 若 `dt+7` 的观察日尚未开始则返回 `NULL`；观察日开始后统计桶内 7 日留存去重用户数 |
| 次日非留存用户数 | `decom.ads_daily_retention_user_duration_bucket` | `next_day_not_retained_user_count` | 若 `dt+1` 的观察日尚未开始则返回 `NULL`；观察日开始后统计桶内次日非留存去重用户数 |
| 7日非留存用户数 | `decom.ads_daily_retention_user_duration_bucket` | `day_7_not_retained_user_count` | 若 `dt+7` 的观察日尚未开始则返回 `NULL`；观察日开始后统计桶内 7 日非留存去重用户数 |

### 留存与非留存用户累计关注账号数分布

| 需求字段 | 落地表 | 落地字段 | 口径 |
|------|------|------|------|
| 用户群 | `decom.ads_daily_retention_user_follow_bucket` | `user_group` | `新用户 / 老用户 / 全量登录用户`；与特征对比报表完全复用同一 cohort 分群 |
| 累计关注账号数桶排序 | `decom.ads_daily_retention_user_follow_bucket` | `follow_account_bucket_order` | 固定为 `1-5`，对应 `(0~3]`、`(3~5]`、`(5~10]`、`(11~20]`、`20次以上` |
| 累计关注账号数桶名称 | `decom.ads_daily_retention_user_follow_bucket` | `follow_account_bucket` | 展示层直接使用的累计关注账号数分桶标签 |
| 次日留存用户数 | `decom.ads_daily_retention_user_follow_bucket` | `next_day_retained_user_count` | 若 `dt+1` 的观察日尚未开始则返回 `NULL`；观察日开始后统计桶内次日留存去重用户数 |
| 7日留存用户数 | `decom.ads_daily_retention_user_follow_bucket` | `day_7_retained_user_count` | 若 `dt+7` 的观察日尚未开始则返回 `NULL`；观察日开始后统计桶内 7 日留存去重用户数 |
| 次日非留存用户数 | `decom.ads_daily_retention_user_follow_bucket` | `next_day_not_retained_user_count` | 若 `dt+1` 的观察日尚未开始则返回 `NULL`；观察日开始后统计桶内次日非留存去重用户数 |
| 7日非留存用户数 | `decom.ads_daily_retention_user_follow_bucket` | `day_7_not_retained_user_count` | 若 `dt+7` 的观察日尚未开始则返回 `NULL`；观察日开始后统计桶内 7 日非留存去重用户数 |

### 留存与非留存用户新增关注账号数分布

| 需求字段 | 落地表 | 落地字段 | 口径 |
|------|------|------|------|
| 用户群 | `decom.ads_daily_retention_user_new_follow_bucket` | `user_group` | `新用户 / 老用户 / 全量登录用户`；与特征对比报表完全复用同一 cohort 分群 |
| 新增关注账号数桶排序 | `decom.ads_daily_retention_user_new_follow_bucket` | `new_follow_account_bucket_order` | 固定为 `1-4`，对应 `0`、`(1~2]`、`(3~5]`、`5个以上` |
| 新增关注账号数桶名称 | `decom.ads_daily_retention_user_new_follow_bucket` | `new_follow_account_bucket` | 展示层直接使用的新增关注账号数分桶标签 |
| 次日留存用户数 | `decom.ads_daily_retention_user_new_follow_bucket` | `next_day_retained_user_count` | 若 `dt+1` 的观察日尚未开始则返回 `NULL`；观察日开始后统计桶内次日留存去重用户数（包含 `0` 关注桶用户）|
| 7日留存用户数 | `decom.ads_daily_retention_user_new_follow_bucket` | `day_7_retained_user_count` | 若 `dt+7` 的观察日尚未开始则返回 `NULL`；观察日开始后统计桶内 7 日留存去重用户数（包含 `0` 关注桶用户）|
| 次日非留存用户数 | `decom.ads_daily_retention_user_new_follow_bucket` | `next_day_not_retained_user_count` | 若 `dt+1` 的观察日尚未开始则返回 `NULL`；观察日开始后统计桶内次日非留存去重用户数（包含 `0` 关注桶用户）|
| 7日非留存用户数 | `decom.ads_daily_retention_user_new_follow_bucket` | `day_7_not_retained_user_count` | 若 `dt+7` 的观察日尚未开始则返回 `NULL`；观察日开始后统计桶内 7 日非留存去重用户数（包含 `0` 关注桶用户）|

### 留存与非留存用户 AI 试穿次数分布

| 需求字段 | 落地表 | 落地字段 | 口径 |
|------|------|------|------|
| 用户群 | `decom.ads_daily_retention_user_tryon_bucket` | `user_group` | `新用户 / 老用户 / 全量登录用户`；与特征对比报表完全复用同一 cohort 分群 |
| AI 试穿次数桶排序 | `decom.ads_daily_retention_user_tryon_bucket` | `tryon_bucket_order` | 固定为 `1-5`，对应 `(0~3]`、`(3~7]`、`(7~10]`、`(10~15]`、`15次以上` |
| AI 试穿次数桶名称 | `decom.ads_daily_retention_user_tryon_bucket` | `tryon_bucket` | 展示层直接使用的 AI 试穿次数分桶标签 |
| 次日留存用户数 | `decom.ads_daily_retention_user_tryon_bucket` | `next_day_retained_user_count` | 若 `dt+1` 的观察日尚未开始则返回 `NULL`；观察日开始后统计桶内次日留存去重用户数 |
| 7日留存用户数 | `decom.ads_daily_retention_user_tryon_bucket` | `day_7_retained_user_count` | 若 `dt+7` 的观察日尚未开始则返回 `NULL`；观察日开始后统计桶内 7 日留存去重用户数 |
| 次日非留存用户数 | `decom.ads_daily_retention_user_tryon_bucket` | `next_day_not_retained_user_count` | 若 `dt+1` 的观察日尚未开始则返回 `NULL`；观察日开始后统计桶内次日非留存去重用户数 |
| 7日非留存用户数 | `decom.ads_daily_retention_user_tryon_bucket` | `day_7_not_retained_user_count` | 若 `dt+7` 的观察日尚未开始则返回 `NULL`；观察日开始后统计桶内 7 日非留存去重用户数 |

### 留存与非留存用户浏览商详次数分布

| 需求字段 | 落地表 | 落地字段 | 口径 |
|------|------|------|------|
| 用户群 | `decom.ads_daily_retention_user_product_detail_bucket` | `user_group` | `新用户 / 老用户 / 全量登录用户`；与特征对比报表完全复用同一 cohort 分群 |
| 浏览商详次数桶排序 | `decom.ads_daily_retention_user_product_detail_bucket` | `product_detail_bucket_order` | 固定为 `1-5`，对应 `(0~0]`、`(1~3]`、`(3~7]`、`(7~10]`、`10次以上` |
| 浏览商详次数桶名称 | `decom.ads_daily_retention_user_product_detail_bucket` | `product_detail_bucket` | 展示层直接使用的浏览商详次数分桶标签 |
| 次日留存用户数 | `decom.ads_daily_retention_user_product_detail_bucket` | `next_day_retained_user_count` | 若 `dt+1` 的观察日尚未开始则返回 `NULL`；观察日开始后统计桶内次日留存去重用户数（包含 `0` 次桶用户） |
| 7日留存用户数 | `decom.ads_daily_retention_user_product_detail_bucket` | `day_7_retained_user_count` | 若 `dt+7` 的观察日尚未开始则返回 `NULL`；观察日开始后统计桶内 7 日留存去重用户数（包含 `0` 次桶用户） |
| 次日非留存用户数 | `decom.ads_daily_retention_user_product_detail_bucket` | `next_day_not_retained_user_count` | 若 `dt+1` 的观察日尚未开始则返回 `NULL`；观察日开始后统计桶内次日非留存去重用户数（包含 `0` 次桶用户） |
| 7日非留存用户数 | `decom.ads_daily_retention_user_product_detail_bucket` | `day_7_not_retained_user_count` | 若 `dt+7` 的观察日尚未开始则返回 `NULL`；观察日开始后统计桶内 7 日非留存去重用户数（包含 `0` 次桶用户） |

补充说明：

- 当前主题只统计已登录用户，不含游客设备
- cohort 主体先沉到 `decom.dws_user_activity_daily`，再分别聚合到 `decom.dws_user_retention_feature_daily` 和 `decom.ads_daily_retention_user_duration_bucket`
- 服务端行为 `AI试穿 / 关注账号 / 创建模特` 分别来自 `v3_decom.common_ext_ai_task`、`v3_decom.community_star_follow`、`v3_decom.common_ext_ai_task`
- 事件侧行为 `浏览商详 / 搜索 / 停留时长 / 衣橱首页曝光` 先沉到 `decom.dws_user_activity_daily`
- `衣橱渗透` 的留存主题口径为“同日 `v_closet` / `v_smart_closet_home` + 同日 `type IN ('SINGLE', 'OUTFIT')` 的 cohort 用户数”；这里不直接复用 `【AI-Fashion】衣橱概览` 中 `type = 'SINGLE'` 的分子定义
  - 衣橱首页曝光埋点 2026-07-07 前后由 `v_closet` 迁移到 `v_smart_closet_home`（约 07-10 完成），口径同时兼容新旧事件；否则切换后渗透 cohort 归零、次日留存会假性掉到 0
- `非衣橱渗透` 为同一 cohort 下“不满足衣橱渗透条件”的补集用户数
- 行为类指标采用布尔判定口径：用户在 `dt` 当日只要发生过至少 1 次，就记入该行为人数
- `日均停留时长` 为了支持跨天聚合，除单日平均值外，额外落地 4 个时长分子字段；报表层应使用 `SUM(分子) / SUM(对应用户数)` 计算区间平均值
- 时长分桶图明确排除 `0 秒` 用户，因此各时长桶用户数加总默认不会回对到第一张图里的 cohort 总人数
- `dws_user_activity_daily.followed_account_count` 表示 cohort 用户在 `dt` 当天关注的去重账号数；新 chart 虽沿用“累计关注账号数”展示名，但实际按该字段分桶
- 关注账号数分桶图明确排除 `0` 个当日关注账号的用户，因此各关注桶用户数加总默认不会回对到第一张图里的 cohort 总人数
- `dws_user_activity_daily.product_detail_view_count` 表示 cohort 用户在 `dt` 当天浏览商详的去重事件次数（`event_name = 'v_product_detail'`）；用于“浏览商详次数分桶”chart
- 浏览商详次数分桶图包含 `0` 次用户，因此各桶 cohort 用户数加总应当等于第一张图里的 cohort 总人数
- `dws_user_activity_daily.ai_tryon_count` 表示 cohort 用户在 `dt` 当天 `task_type IN ('QUICK_TRY_ON','TRY_ON','OUTFIT')` 的服务端任务行数；和特征对比的 `used_ai_tryon` 布尔指标不是同一个字段，不要混用
- AI 试穿次数分桶图明确排除 `0` 次试穿用户，因此各试穿桶用户数加总默认不会回对到第一张图里的 cohort 总人数
- 特征对比表原有字段已完成生产表扩展与 Superset 资产更新；本次新增“衣橱渗透”后，是否执行 DDL、刷新受影响分区并让 dashboard 生效，需要单独执行
- 时长分桶表的代码、DDL 与调度配置已补齐；是否创建生产表、刷新历史分区并创建 Superset 资产，需要单独执行
- 累计关注账号数分桶表的代码、DDL 与调度配置已补齐；是否创建生产表、刷新历史分区并创建 Superset 资产，需要单独执行
- 新增关注账号数分桶 chart 与累计关注账号数 chart 共用 `dws_user_activity_daily.followed_account_count` 字段，分桶定义不同；新 chart **包含 `0` 关注用户**，因此各桶 cohort 用户数加总应当等于第一张图里的 cohort 总人数
- 新增关注账号数分桶表的代码、DDL、调度配置已补齐，生产表已创建并完成 `2026-05-01 ~ 2026-05-25` 历史分区回刷；Superset 物理 dataset `ads_daily_retention_user_new_follow_bucket`（id=248）与 chart `留存与非留存用户新增关注账号数分布`（id=626）已创建并挂到 dashboard 103 的「累计关注账号数」之后，dashboard 的「日期」「用户群」原生 filter 已扩到新 chart
- AI 试穿次数分桶表的代码、DDL、`dws_user_activity_daily.ai_tryon_count` 扩列与调度配置已补齐；是否执行 DWS 扩列 + 历史分区重刷、创建 ADS 生产表、刷新历史分区并创建 Superset 资产，需要单独执行
- 浏览商详次数分桶表的代码、DDL、`dws_user_activity_daily.product_detail_view_count` 扩列与调度配置已补齐；是否执行 DWS 扩列 + 历史分区重刷、创建 ADS 生产表、刷新历史分区并创建 Superset 资产，需要单独执行

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
