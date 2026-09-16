# 【AI-Fashion】Style Me 看板

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | Style Me 看板 | StyleMe 看板、Style Me 日报、StyleMe 日报 |
| 指标 | Style Me 用户数 | StyleMe 用户、Style Me UV |
| 指标 | 次日留存率 | D1 留存率、1 日留存率 |
| 指标 | 7 日留存率 | D7 留存率、第 7 日留存率 |
| 指标 | 30 日留存率 | D30 留存率、第 30 日留存率 |

## 适用范围

- 业务线：`Alvin's Club App`
- 适合：查询 Style Me 用户、使用场景及次日、7 日、30 日留存。
- 不适合：通用试穿点击与转化应使用[穿搭工具点击使用](../穿搭工具点击使用/fact.md)或[试穿结果转化](../试穿结果转化/fact.md)。
- 需要澄清：需要确认留存周期、Style Me 场景和日期范围。

## 报表

### ads_daily_ai_fashion_styleme

- 主题：Style Me 日报
- 表：`my-project-8584-jetonai.decom.ads_daily_ai_fashion_styleme`
- 状态：`active`
- 粒度：一行代表一个 `dt`
- 时区：`America/Toronto`
- 数据范围：Style Me 用户、三种使用场景和平台回访留存。
- 必要过滤：无。
- 不包含：非 Style Me 的通用试穿行为。
- 明细展示：Chart 2 通过 Superset 虚拟 dataset `ads_daily_ai_fashion_styleme_scenario_detail` 将宽表转为长表
- Dashboard：[121](https://bi.alvinclub.ca/superset/dashboard/121/)

### Chart 1：Style Me 留存

Style Me 用户的次日、第 7 日、第 30 日留存率。

**Style Me 用户群组定义（OR 关系）：**
1. 当日有 `c_styleme_try_btn` 埋点事件的用户
2. 当日有 `v_styleme_page` 事件且后台有 STYLE_ME 任务的用户（`task_scene = 'STYLE_ME'`）

**留存口径：** 群组用户在 dt+1 / dt+7 / dt+30 回到平台（出现在 `dws_user_daily`）。

### Chart 2：Different Scenarios

三种场景：Quick Mode、Plus Item、Plus Outfit。

- 展示粒度：`dt + scenario`
- 每日固定 3 行：`Quick Mode`、`Plus Item`、`Plus Outfit`

## 字段

所有 UV 按已登录 `prop_user_id` / 服务端 `user_id` 在 `dt` 日内去重；不同场景的 UV 不可相加。

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| Style Me 用户数 | `styleme_user_uv` | 已登录用户 | 留存基数；当日满足任一条件的去重用户：① 有 `c_styleme_try_btn` 事件；② 有 `v_styleme_page` 事件且后台有 `task_scene = 'STYLE_ME'` 任务 | 无 | 不可跨天或跨场景求和；须回明细按目标范围重新去重 |
| 次日留存人数 | `next_day_retained_uv` | 已登录用户 | 群组用户在 `dt+1` 回到平台（出现在 `dws_user_daily`）的去重人数 | 观察窗口 `DATE_ADD(dt, INTERVAL 1 DAY) <= CURRENT_DATE('America/Toronto')` | 未到观察日返回 `NULL`，不可当 `0`；不可跨天求和或平均 |
| 次日留存率 | `next_day_retention_rate` | 已登录用户 | `retained / (retained + not_retained)`；分母为 0 时返回 `NULL` | 同上 | 未到观察日返回 `NULL`，不可当 `0`；不可跨天求和或平均 |
| 第 7 日留存人数 | `day_7_retained_uv` | 已登录用户 | 群组用户在 `dt+7` 回到平台的去重人数 | 观察日已到 | 未到观察日返回 `NULL`，不可当 `0`；不可跨天求和或平均 |
| 第 7 日留存率 | `day_7_retention_rate` | 已登录用户 | 同次日口径，观察日为 `dt+7` | 同上 | 未到观察日返回 `NULL`，不可当 `0`；不可跨天求和或平均 |
| 第 30 日留存人数 | `day_30_retained_uv` | 已登录用户 | 群组用户在 `dt+30` 回到平台的去重人数 | 观察日已到 | 未到观察日返回 `NULL`，不可当 `0`；不可跨天求和或平均 |
| 第 30 日留存率 | `day_30_retention_rate` | 已登录用户 | 同次日口径，观察日为 `dt+30` | 同上 | 未到观察日返回 `NULL`，不可当 `0`；不可跨天求和或平均 |
| 弹窗曝光 UV | `popup_exposure_uv` | 已登录用户 | `v_styleme_popup` 的去重用户数；三个场景共用同一分母 | 无 | 不可跨天或跨场景求和；须回明细按目标范围重新去重 |
| Quick Mode Try it 点击 UV | `quick_try_btn_uv` | 已登录用户 | `c_styleme_try_btn` 且 `button_type = 'quick_signle'` 的去重用户数 | 无 | 不可跨天或跨场景求和；须回明细按目标范围重新去重 |
| Quick Mode 渗透率 | `quick_penetration_rate` | 已登录用户 | `quick_try_btn_uv / popup_exposure_uv`；分母为 0 时返回 `NULL` | 无 | 不可 `AVG`；跨天须用对应去重分子、分母重算 |
| Quick Mode 任务成功数 | `quick_task_success_count` | 任务 | `task_type = 'QUICK_TRY_ON'` 且 `task_scene = 'STYLE_ME'` 且 `status = 'SUCCESS'` 的任务行数 | 无 | 可跨天求和 |
| Quick Mode Add to Closet UV | `quick_add_closet_uv` | 已登录用户 | `c_add_to_closet` 关联到 Quick Mode 任务后的去重用户数 | 无 | 不可跨天或跨场景求和；须回明细按目标范围重新去重 |
| Quick Mode Add to Closet & Calendar UV | `quick_add_closet_calendar_uv` | 已登录用户 | `c_add_to_closet_calendar` 关联到 Quick Mode 任务后的去重用户数 | 无 | 不可跨天或跨场景求和；须回明细按目标范围重新去重 |
| Quick Mode 结果成功页曝光 UV | `quick_result_success_uv` | 已登录用户 | `v_result_success` 且 `page_key = 'quick_result'` 的去重用户数 | 无 | 不可跨天或跨场景求和；须回明细按目标范围重新去重 |
| Quick Mode Add to Closet 率 | `quick_add_closet_rate` | 已登录用户 | `quick_add_closet_uv / quick_result_success_uv`；分母为 0 时返回 `NULL` | 无 | 不可 `AVG`；跨天须用对应去重分子、分母重算 |
| Quick Mode Add to Closet & Calendar 率 | `quick_add_closet_calendar_rate` | 已登录用户 | `quick_add_closet_calendar_uv / quick_result_success_uv`；分母为 0 时返回 `NULL` | 无 | 不可 `AVG`；跨天须用对应去重分子、分母重算 |
| Plus Item Try it 点击 UV | `plus_item_try_btn_uv` | 已登录用户 | `c_styleme_try_btn` 且 `button_type = 'plus_single'` 的去重用户数 | 无 | 不可跨天或跨场景求和；须回明细按目标范围重新去重 |
| Plus Item 渗透率 | `plus_item_penetration_rate` | 已登录用户 | `plus_item_try_btn_uv / popup_exposure_uv`；分母为 0 时返回 `NULL` | 无 | 不可 `AVG`；跨天须用对应去重分子、分母重算 |
| Plus Item 任务成功数 | `plus_item_task_success_count` | 任务 | `task_scene = 'STYLE_ME'` 且 `referenceProducts` 非空 且 `status = 'SUCCESS'` 的任务行数 | 无 | 可跨天求和 |
| Plus Item Add to Closet UV | `plus_item_add_closet_uv` | 已登录用户 | `c_add_to_closet` 关联到 Plus Item 任务后的去重用户数 | 无 | 不可跨天或跨场景求和；须回明细按目标范围重新去重 |
| Plus Item Add to Closet & Calendar UV | `plus_item_add_closet_calendar_uv` | 已登录用户 | `c_add_to_closet_calendar` 关联到 Plus Item 任务后的去重用户数 | 无 | 不可跨天或跨场景求和；须回明细按目标范围重新去重 |
| Plus Item Add to Closet 率 | `plus_item_add_closet_rate` | 已登录用户 | `plus_item_add_closet_uv / plus_result_success_uv`；分母为 0 时返回 `NULL` | 无 | 不可 `AVG`；跨天须用对应去重分子、分母重算 |
| Plus Item Add to Closet & Calendar 率 | `plus_item_add_closet_calendar_rate` | 已登录用户 | `plus_item_add_closet_calendar_uv / plus_result_success_uv`；分母为 0 时返回 `NULL` | 无 | 不可 `AVG`；跨天须用对应去重分子、分母重算 |
| Plus Outfit Try it 点击 UV | `plus_outfit_try_btn_uv` | 已登录用户 | `c_styleme_try_btn` 且 `button_type = 'plus_outfit'` 的去重用户数 | 无 | 不可跨天或跨场景求和；须回明细按目标范围重新去重 |
| Plus Outfit 渗透率 | `plus_outfit_penetration_rate` | 已登录用户 | `plus_outfit_try_btn_uv / popup_exposure_uv`；分母为 0 时返回 `NULL` | 无 | 不可 `AVG`；跨天须用对应去重分子、分母重算 |
| Plus Outfit 任务成功数 | `plus_outfit_task_success_count` | 任务 | `task_scene = 'STYLE_ME'` 且 `referenceProducts` 为空 且 `task_type != 'QUICK_TRY_ON'` 且 `status = 'SUCCESS'` 的任务行数 | 无 | 可跨天求和 |
| Plus Outfit Add to Closet UV | `plus_outfit_add_closet_uv` | 已登录用户 | `c_add_to_closet` 关联到 Plus Outfit 任务后的去重用户数 | 无 | 不可跨天或跨场景求和；须回明细按目标范围重新去重 |
| Plus Outfit Add to Closet & Calendar UV | `plus_outfit_add_closet_calendar_uv` | 已登录用户 | `c_add_to_closet_calendar` 关联到 Plus Outfit 任务后的去重用户数 | 无 | 不可跨天或跨场景求和；须回明细按目标范围重新去重 |
| Plus Outfit Add to Closet 率 | `plus_outfit_add_closet_rate` | 已登录用户 | `plus_outfit_add_closet_uv / plus_result_success_uv`；分母为 0 时返回 `NULL` | 无 | 不可 `AVG`；跨天须用对应去重分子、分母重算 |
| Plus Outfit Add to Closet & Calendar 率 | `plus_outfit_add_closet_calendar_rate` | 已登录用户 | `plus_outfit_add_closet_calendar_uv / plus_result_success_uv`；分母为 0 时返回 `NULL` | 无 | 不可 `AVG`；跨天须用对应去重分子、分母重算 |
| Plus 结果成功页曝光 UV | `plus_result_success_uv` | 已登录用户 | `v_result_success` 且 `page_key = 'plus_result'` 的去重用户数；Plus Item 与 Plus Outfit 共用 | 无 | 不可跨天或跨场景求和；须回明细按目标范围重新去重 |

Chart 2 的三种场景（Quick Mode / Plus Item / Plus Outfit）由 Superset 虚拟 dataset `ads_daily_ai_fashion_styleme_scenario_detail` 把上面的宽表转成 `dt + scenario` 长表，每日固定 3 行。

## 依赖

### 链路

```
dwd_event_log + v3_decom.common_ext_ai_task + dws_user_daily
  → dws_ai_fashion_styleme_daily
  → ads_daily_ai_fashion_styleme
```

### Superset

- 物理 dataset：`ads_daily_ai_fashion_styleme`（id=269）
- 虚拟 dataset：`ads_daily_ai_fashion_styleme_scenario_detail`（id=270）
- chart：`Style Me 留存（日表）- 趋势`（id=675）、`Style Me Different Scenarios（日表）- 明细`（id=676）
- dashboard：`【AI-Fashion】Style Me 看板`（id=121，默认日期 filter=`Last 30 days`）

### 场景归属逻辑

| 场景 | button_type | 任务成功数条件 | Add to Closet 条件 | 结果页 page_key |
|---|---|---|---|---|
| Quick Mode | `quick_signle` | `task_type='QUICK_TRY_ON'` | `task_type='QUICK_TRY_ON'` | `quick_result` |
| Plus Item | `plus_single` | `referenceProducts` 非空 | `referenceProducts` 非空 | `plus_result` |
| Plus Outfit | `plus_outfit` | `referenceProducts` 空且非 QUICK_TRY_ON | 同左 | `plus_result` |

### 留存观察窗口

- `is_next_day_observation_started`：`DATE_ADD(dt, INTERVAL 1 DAY) <= CURRENT_DATE('America/Toronto')`
- 观察未到位时，留存率和留存数为 NULL

## 已落地字段

全部字段均已落地。
