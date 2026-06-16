# 留存用户特征数据

## 新老用户要按用户粒度重算，不要直接加总平台粒度 `is_new_user`

- `decom.dws_user_daily` 的粒度是 `dt × prop_user_id × platform`
- 这个主题没有平台筛选，若直接按 `is_new_user` 聚合再跨平台相加，会把同一用户在多平台活跃的行重复参与分群
- 正确做法是先按 `dt × prop_user_id` 去重，再用 `first_active_date = dt` / `< dt` 判定新老用户

## 观察日尚未开始时返回 `NULL`，观察日开始后按实时进度滚动

- `次日非留存` 和 `7日非留存` 都依赖未来日期是否活跃
- 当 `dt+1` 或 `dt+7` 的观察日还没开始时，应返回 `NULL`
- 一旦观察日开始，即使当天还没结束，也应按当前回访进度实时更新留存与非留存人数

## 跨天展示时，均值类指标不能直接聚合现成平均值

- `AI试穿 / 关注账号 / 浏览商详 / 创建模特 / 搜索` 这类人数指标可以跨天直接 `SUM`
- `日均停留时长` 不能跨天直接 `SUM` 或简单 `AVG` 现成日均值，否则区间口径会漂
- 正确做法是底表额外落地时长分子字段，报表层对区间做 `SUM(时长分子) / SUM(对应用户数分母)`

## 时长分桶图默认排除 `0 秒` 用户

- 本次“时长分布”需求明确要求 `0 秒` 用户不进入任何时长桶
- 因此新 chart 各桶的 `次日留存 / 7日留存 / 次日非留存 / 7日非留存` 加总，只覆盖 `dt` 当日停留时长大于 `0` 的 cohort 用户
- 如果拿桶内加总去对比第一张图里的 cohort 总人数，先确认是否已经排除了 `0 秒` 用户

## 服务端行为晚到时，先看 `dws_user_activity_daily`，不要直接怀疑最终 ADS

- Batch 6 之后，`AI试穿 / 关注账号 / 创建模特 / 搜索 / 浏览商详 / 停留时长` 都先沉到 `decom.dws_user_activity_daily`
- `decom.ads_daily_retention_user_feature` 只再消费 `decom.dws_user_retention_feature_daily`
- `decom.ads_daily_retention_user_duration_bucket` 直接消费 `decom.dws_user_activity_daily`
- 如果是单个行为人数不对，优先查 `dws_user_activity_daily` 对应日期和用户，再决定是否回溯 `dwd_event_log` 或服务端事实表

## “累计关注账号数” 是展示名，实际按 cohort 当天关注去重账号数分桶

- 本主题新 chart 虽然沿用“累计关注账号数”这个展示名，但实际口径不是历史累计
- 正确口径是：对 `dt` 当天 cohort 用户，统计其在 `dt` 当天关注的去重账号数，再看这些用户在 `dt+1` / `dt+7` 是否留存
- 排查桶人数异常时，先看 `dws_user_activity_daily.followed_account_count`，不要先去查历史累计关系

## “AI 试穿使用次数” 是 task_type 任务行数，不是布尔行为

- `dws_user_activity_daily.used_ai_tryon` 是 0/1 布尔，只要当天发生过一次试穿就置 1，用于特征对比表的人数指标
- `dws_user_activity_daily.ai_tryon_count` 是 `task_type IN ('QUICK_TRY_ON','TRY_ON','OUTFIT')` 的当日任务行数，用于试穿次数分桶
- 排查桶人数对不上时，不要拿 `used_ai_tryon` 的去重用户数去对桶人数加总；先确认查的是次数字段还是布尔字段
- 试穿用户的 cohort user_id 取数和 `tryon_behavior_daily` 一致：优先 `request_param.user_id / userId`，回退到 `common_ext_ai_task.user_id`；次数字段直接在同一 CTE 内 `COUNT(1)`，不需要再拉一遍源表

## 试穿次数分桶图默认排除 `0` 次用户

- 本次“AI 试穿次数分布”需求明确要求 `0` 次用户不进入任何分桶
- 因此各试穿桶的 `次日留存 / 7日留存 / 次日非留存 / 7日非留存` 加总，只覆盖 `dt` 当日发生过至少 1 次试穿的 cohort 用户
- 如果拿桶内加总去对比第一张图里的 cohort 总人数，先确认是否已经排除了 `0` 次用户

## “衣橱渗透” 不要直接照搬衣橱概览的 `SINGLE` 口径

- `【AI-Fashion】衣橱概览` 的渗透人数分子是“同日 `v_closet` + 同日 `type = 'SINGLE'`”
- 本主题“留存用户与非留存用户特征对比”里的“衣橱渗透”展示的是 cohort 用户数，但服务端侧要按 `type IN ('SINGLE', 'OUTFIT')` 判定
- “非衣橱渗透”则是同一条件下的补集，不要再单独发明第二套判定口径
- 遇到两个主题都叫“渗透”时，先核对主题文档，不要因为名称相同就直接复用另一张表的分子逻辑

## “新增关注账号数” 与 “累计关注账号数” 共用一个 DWS 字段

- 两个 chart 都用 `dws_user_activity_daily.followed_account_count`，本质都是 cohort 在 `dt` 当天关注的去重账号数
- 区别只有：分桶范围 + 是否包含 `0` 关注用户
  - “累计关注账号数”：`(0~3]/(3~5]/(5~10]/(11~20]/20次以上`，**排除 `0` 用户**，桶人数加总 < cohort 总人数
  - “新增关注账号数”：`0/(1~2]/(3~5]/5个以上`，**包含 `0` 用户**，桶人数加总 = cohort 总人数
- 排查桶人数不一致时，先确认看的是哪张表 / 哪种分桶逻辑，不要混用两套分桶定义
