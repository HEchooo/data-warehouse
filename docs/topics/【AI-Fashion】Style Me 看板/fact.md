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

| ADS 字段 | 含义 | 计算 |
|---|---|---|
| `styleme_user_uv` | Style Me 用户数（留存基数） | 群组定义 |
| `next_day_retention_rate` | 次日留存率 | retained / (retained + not_retained) |
| `next_day_retained_uv` | 次日留存人数 | - |
| `day_7_retention_rate` | 第 7 日留存率 | 同上 |
| `day_7_retained_uv` | 第 7 日留存人数 | - |
| `day_30_retention_rate` | 第 30 日留存率 | 同上 |
| `day_30_retained_uv` | 第 30 日留存人数 | - |
| `popup_exposure_uv` | 弹窗曝光 UV（三场景共用） | `v_styleme_popup` UV |
| `quick_try_btn_uv` | Quick Mode Try it 点击 UV | `c_styleme_try_btn && button_type='quick_signle'` |
| `quick_penetration_rate` | Quick Mode 渗透率 | quick_try_btn_uv / popup_exposure_uv |
| `quick_task_success_count` | Quick Mode 任务成功数 | `task_type='QUICK_TRY_ON' && task_scene='STYLE_ME' && status='SUCCESS'` |
| `quick_add_closet_uv` | Quick Mode Add to Closet UV | `c_add_to_closet` join task（Quick Mode 条件） |
| `quick_add_closet_calendar_uv` | Quick Mode Add to Closet & Calendar UV | `c_add_to_closet_calendar` join task |
| `quick_result_success_uv` | Quick Mode 结果成功页曝光 UV | `v_result_success && page_key='quick_result'` |
| `quick_add_closet_rate` | Quick Mode Add to Closet 率 | quick_add_closet_uv / quick_result_success_uv |
| `quick_add_closet_calendar_rate` | Quick Mode Add to Closet & Calendar 率 | quick_add_closet_calendar_uv / quick_result_success_uv |
| `plus_item_try_btn_uv` | Plus Item Try it 点击 UV | `c_styleme_try_btn && button_type='plus_single'` |
| `plus_item_penetration_rate` | Plus Item 渗透率 | plus_item_try_btn_uv / popup_exposure_uv |
| `plus_item_task_success_count` | Plus Item 任务成功数 | `task_scene='STYLE_ME' && referenceProducts 非空 && status='SUCCESS'` |
| `plus_item_add_closet_uv` | Plus Item Add to Closet UV | `c_add_to_closet` join task（Plus Item 条件） |
| `plus_item_add_closet_calendar_uv` | Plus Item Add to Closet & Calendar UV | `c_add_to_closet_calendar` join task |
| `plus_result_success_uv` | Plus 结果成功页曝光 UV（Item 与 Outfit 共用） | `v_result_success && page_key='plus_result'` |
| `plus_item_add_closet_rate` | Plus Item Add to Closet 率 | plus_item_add_closet_uv / plus_result_success_uv |
| `plus_item_add_closet_calendar_rate` | Plus Item Add to Closet & Calendar 率 | plus_item_add_closet_calendar_uv / plus_result_success_uv |
| `plus_outfit_try_btn_uv` | Plus Outfit Try it 点击 UV | `c_styleme_try_btn && button_type='plus_outfit'` |
| `plus_outfit_penetration_rate` | Plus Outfit 渗透率 | plus_outfit_try_btn_uv / popup_exposure_uv |
| `plus_outfit_task_success_count` | Plus Outfit 任务成功数 | `task_scene='STYLE_ME' && referenceProducts 空 && task_type!='QUICK_TRY_ON' && status='SUCCESS'` |
| `plus_outfit_add_closet_uv` | Plus Outfit Add to Closet UV | `c_add_to_closet` join task（Plus Outfit 条件） |
| `plus_outfit_add_closet_calendar_uv` | Plus Outfit Add to Closet & Calendar UV | `c_add_to_closet_calendar` join task |
| `plus_outfit_add_closet_rate` | Plus Outfit Add to Closet 率 | plus_outfit_add_closet_uv / plus_result_success_uv |
| `plus_outfit_add_closet_calendar_rate` | Plus Outfit Add to Closet & Calendar 率 | plus_outfit_add_closet_calendar_uv / plus_result_success_uv |

补充说明：

- 所有 UV 均按已登录 `prop_user_id` / 服务端 `user_id` 在 `dt` 日内去重；不同场景的 UV 不可相加
- 所有留存率、渗透率和加衣橱率在分母为 0 时返回 `NULL`；未到观察日的留存同样为 `NULL`，不表示 0 留存
- 率值不可跨天或跨场景求和、平均；跨期必须用对应去重分子、分母重算

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
