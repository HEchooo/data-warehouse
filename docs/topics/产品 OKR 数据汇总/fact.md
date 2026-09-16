# 产品 OKR 数据汇总

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | 产品 OKR 数据汇总 | 产品OKR、产品 OKR 看板、产品 OKR 试穿周报 |
| 指标 | 平台整体 DAU | 平台日活、日活用户数 |
| 指标 | 次日留存率 | D1 留存率、1 日留存率 |
| 指标 | 7 日留存率 | D7 留存率、第 7 日留存率 |
| 指标 | 30 日留存率 | D30 留存率、第 30 日留存率 |
| 指标 | AI 试穿成功率 | 试穿成功率 |

## 适用范围

- 业务线：`Alvin's Club App`
- 适合：查询产品 OKR 的试穿、商品和留存周指标或日指标。
- 不适合：单一业务主题的明细分析应使用对应试穿、商品或留存 topic；本主题用于 OKR 汇总。
- 需要澄清：需要确认 v1 历史周表、v2 周宽表或 v2 日表，以及指标和周期。

## 报表

### 主题总览

- 主题：`产品 OKR 数据汇总`
- 当前已落地逻辑：`试穿OKR`、`商品OKR`、`留存OKR`
- 周维度：固定按 `周一~周日` 切周
- 周标签：`MMDD-MMDD`（例如 `0427-0503`）

### ads_weekly_ai_fashion_tryon_okr

- 表：`my-project-8584-jetonai.decom.ads_weekly_ai_fashion_tryon_okr`
- 状态：`deprecated`
- 粒度：`week_start_dt + metric_name + metric_bucket(可空)`
- 时区：`America/Toronto`
- 数据范围：试穿、商品和留存 OKR 的历史周级长表。
- 必要过滤：按目标 `metric_name` 过滤。
- 不包含：v2 宽表新增展示能力。
- 替代表：`my-project-8584-jetonai.decom.ads_weekly_ai_fashion_tryon_okr_v2`
- Dashboard：[产品 OKR 数据汇总（deprecated）](https://bi.alvinclub.ca/superset/dashboard/ai-fashion-product-okr-summary/)（id=114）
- Chart（核心指标）：[【AI-Fashion】试穿OKR（核心指标）](https://bi.alvinclub.ca/superset/explore/?slice_id=628)（id=628）
- Chart（分布）：[【AI-Fashion】人均每日试穿次数分布（周）](https://bi.alvinclub.ca/superset/explore/?slice_id=629)（id=629）

### 商品OKR

- 粒度：`week_start_dt + metric_name + metric_bucket(可空)`
- Dashboard：[产品 OKR 数据汇总（deprecated）](https://bi.alvinclub.ca/superset/dashboard/ai-fashion-product-okr-summary/)（id=114）
- Chart（核心指标）：[【AI-Fashion】商品OKR（核心指标）](https://bi.alvinclub.ca/superset/explore/?slice_id=630)（id=630）
- Chart（分布）：[【AI-Fashion】人均商品点击次数分布（周）](https://bi.alvinclub.ca/superset/explore/?slice_id=631)（id=631）

### 留存OKR

- 粒度：`week_start_dt + metric_name`
- Dashboard：[产品 OKR 数据汇总（deprecated）](https://bi.alvinclub.ca/superset/dashboard/ai-fashion-product-okr-summary/)（id=114）

### ads_weekly_ai_fashion_tryon_okr_v2

- 表：`my-project-8584-jetonai.decom.ads_weekly_ai_fashion_tryon_okr_v2`
- 状态：`active`
- 粒度：`week_start_dt`
- 时区：`America/Toronto`
- 数据范围：试穿、商品和留存 OKR 的周级宽表。
- 必要过滤：无。
- 不包含：日级明细。
- 设计目标：复用 v1 已验证指标逻辑，将 `metric_name / metric_value` 的 long form 转为每个指标一个字段，便于 Superset 直接可视化
- Dashboard：[产品 OKR 数据汇总（Week）](https://bi.alvinclub.ca/superset/dashboard/ai-fashion-product-okr-summary-v2/)（id=117）
- 物理 Dataset：`id=263`（`decom.ads_weekly_ai_fashion_tryon_okr_v2`）
- Chart（试穿核心指标）：`id=644`
- Chart（商品核心指标）：`id=645`
- Chart（留存核心指标）：`id=646`
- Chart（试穿分布）：`id=647`
- Chart（试穿时长分布）：`id=649`
- Chart（商品点击分布）：`id=648`

### ads_daily_ai_fashion_tryon_okr_v2

- 表：`my-project-8584-jetonai.decom.ads_daily_ai_fashion_tryon_okr_v2`
- 状态：`active`
- 粒度：`dt`
- 时区：`America/Toronto`
- 数据范围：试穿、商品和留存 OKR 的日级宽表。
- 必要过滤：无。
- 不包含：周级天数与周标签字段。
- 设计目标：复用 v2 周级宽表同口径核心指标，直接输出日级字段，供 Day 看板使用
- 口径裁剪：不输出各 `*_observed_day_count`，也不保留任何 `week_*` 维度列
- Dashboard：[产品 OKR 数据汇总（Day）](https://bi.alvinclub.ca/superset/dashboard/ai-fashion-product-okr-summary-day/)（id=120）
- 物理 Dataset：`id=268`（`decom.ads_daily_ai_fashion_tryon_okr_v2`）
- Chart（试穿核心指标）：`id=669`
- Chart（商品核心指标）：`id=670`
- Chart（留存核心指标）：`id=671`
- Chart（试穿分布）：`id=672`
- Chart（商品点击分布）：`id=673`
- Chart（试穿时长分布）：`id=674`

## 字段

周桶从 `2026-04-27` 开始，固定每 7 天（周一至周日）。周级数值统一遵循「先按天计算，再在周内按规则聚合」：率指标一律「周内分子求和 / 周内分母求和」，不直接 `AVG(每日率)`；日均 UV 为「周内各日 UV 之和 / 周内天数」。留存口径统一使用 `prop_user_id`。

整体试穿用户、整体试穿次数、帖子试穿和「使用 AI 试穿」留存统一使用服务端 `common_ext_ai_task` 的三种试穿任务；首页和商详试穿因依赖页面来源，继续使用前端 `c_tryon`。

下表字段为 v2 宽表 `decom.ads_weekly_ai_fashion_tryon_okr_v2` 的列名。

### 试穿 OKR

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 新用户试穿率 | `new_user_tryon_rate` | 已登录用户 | 周内服务端试穿新用户 `tryon_new_uv` 之和 / 周内 `dau_new_uv` 之和 | `task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')` | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| 新用户试穿 UV | `new_user_tryon_uv` | 已登录用户 | 周内 `tryon_new_uv` 之和 / 周内天数 | `task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')` | 周内日均值，不可跨周求和或平均；跨周须回 DWS 按目标周期重算 |
| 老用户试穿率 | `old_user_tryon_rate` | 已登录用户 | 周内服务端试穿老用户 `tryon_old_uv` 之和 / 周内 `dau_old_uv` 之和 | `task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')` | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| 老用户试穿 UV | `old_user_tryon_uv` | 已登录用户 | 周内 `tryon_old_uv` 之和 / 周内天数 | `task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')` | 周内日均值，不可跨周求和或平均；跨周须回 DWS 按目标周期重算 |
| 首页信息流试穿 UV | `home_tryon_uv` | 已登录用户 | 周内 `home_tryon_uv` 之和 / 周内天数 | 前端 `c_tryon AND args_page_key = 'home_feeds'` | 周内日均值，不可跨周求和或平均；跨周须回 DWS 按目标周期重算 |
| 首页试穿渗透率 | `home_tryon_penetration_rate` | 已登录用户 | 周内 `home_tryon_uv` 之和 / 周内 `home_feeds_uv` 之和 | 同上 | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| 帖子试穿 UV | `post_tryon_uv` | 已登录用户 | 周内服务端带 `request_param.postId` 且可映射 `dim_post` 的试穿用户之和 / 周内天数 | `task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')` | 周内日均值，不可跨周求和或平均；跨周须回 DWS 按目标周期重算 |
| 帖子试穿渗透率 | `post_tryon_penetration_rate` | 已登录用户 | 周内服务端帖子试穿 `post_tryon_uv` 之和 / 周内前端帖子场景曝光 `post_scene_exposure_uv` 之和；这是跨源转化口径，分子服务端、分母前端 | `task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')` | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| 商品详情页试穿 UV | `product_detail_tryon_uv` | 已登录用户 | 周内 `product_detail_tryon_uv` 之和 / 周内天数 | 前端 `c_tryon AND args_page_key = 'product_detail'` | 周内日均值，不可跨周求和或平均；跨周须回 DWS 按目标周期重算 |
| 商品详情页试穿渗透率 | `product_detail_tryon_penetration_rate` | 已登录用户 | 周内 `product_detail_tryon_uv` 之和 / 周内 `product_detail_uv` 之和 | 同上 | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| styleme 试穿 UV | `styleme_tryon_uv` | 已登录用户 | 周内 `styleme_uv` 之和 / 周内天数 | 无 | 周内日均值，不可跨周求和或平均；跨周须回 DWS 按目标周期重算 |
| styleme 试穿渗透率 | `styleme_tryon_penetration_rate` | 已登录用户 | 周内服务端 `styleme_uv` 之和 / 周内前端 `v_styleme_popup` 或 `v_styleme_page` UV 之和 | 无 | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| 平台试穿率 | `platform_tryon_rate` | 已登录用户 | 周内服务端试穿 `tryon_uv_total` 之和 / 周内平台 DAU 之和；平台 DAU 与留存 OKR 的 `platform_dau` 同口径 | `task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')` | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| 人均试穿次数 | `tryon_avg_count_per_tryon_user` | 已登录用户 | 周内服务端试穿次数 `tryon_pv` 之和 / 周内服务端试穿用户 `tryon_uv_total` 之和 | `task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')` | 人均值，不可跨周求和或平均；跨周须用汇总分子分母重算 |
| 日均试穿 UV | `avg_daily_tryon_uv` | 已登录用户 | 周内服务端试穿用户 `tryon_uv_total` 之和 / 周内天数 | `task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')` | 周内日均值，不可跨周求和或平均；跨周须回 DWS 按目标周期重算 |
| 前端 tryon 点击 UV | `front_tryon_click_uv` | 已登录用户 | 周内 `event_name = 'c_tryon'` 的 `prop_user_id` UV 之和 / 周内天数 | 无 | 周内日均值，不可跨周求和或平均；跨周须回 DWS 按目标周期重算 |
| 商城信息流试穿 UV | `shop_feeds_tryon_uv` | 已登录用户 | 周内 `c_tryon AND args_page_key = 'shop_feeds'` 的 UV 之和 / 周内天数 | 无 | 周内日均值，不可跨周求和或平均；跨周须回 DWS 按目标周期重算 |
| 商城信息流试穿渗透率 | `shop_feeds_tryon_penetration_rate` | 已登录用户 | 周内 `c_tryon AND args_page_key = 'shop_feeds'` 的 UV 之和 / 周内 `v_shop_feeds` UV 之和 | 无 | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| 衣橱试穿 UV | `closet_feeds_tryon_uv` | 已登录用户 | 周内 `c_tryon AND args_page_key = 'closet_feeds'` 的 UV 之和 / 周内天数 | 无 | 周内日均值，不可跨周求和或平均；跨周须回 DWS 按目标周期重算 |
| 衣橱试穿渗透率 | `closet_feeds_tryon_penetration_rate` | 已登录用户 | 周内 `c_tryon AND args_page_key = 'closet_feeds'` 的 UV 之和 / 周内 `v_closet_feeds` UV 之和 | 无 | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| AI 试穿成功率（quick） | `quick_tryon_success_rate` | 试穿任务 | 周内 `task_type = 'QUICK_TRY_ON'` 且 `status = 'SUCCESS'` 的任务数 / 同 `task_type` 全部任务数，按 `task_id` 去重 | 无 | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| AI 试穿成功率（plus） | `plus_tryon_success_rate` | 试穿任务 | 周内 `task_type = 'TRY_ON'` 且 `status = 'SUCCESS'` 的任务数 / 同 `task_type` 全部任务数，按 `task_id` 去重 | 无 | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| AI 试穿任务平均完成时长（quick） | `quick_tryon_avg_duration_min` | 试穿任务 | 周内 `QUICK_TRY_ON` 成功任务的 `json_value(output, '$.totalDuration')` 总和 / 有效成功任务数；源单位毫秒，展示转分钟 | `status = 'SUCCESS'` | 人均值，不可跨周求和或平均；跨周须用汇总分子分母重算 |
| AI 试穿任务平均完成时长（plus） | `plus_tryon_avg_duration_min` | 试穿任务 | 周内 `TRY_ON` 成功任务的 `totalDuration` 总和 / 有效成功任务数；源单位毫秒，展示转分钟 | `status = 'SUCCESS'` | 人均值，不可跨周求和或平均；跨周须用汇总分子分母重算 |

### 商品 OKR

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 商品点击 UV | `product_click_uv` | 已登录用户 | 周内 `product_click_uv` 之和 / 周内天数 | 无 | 周内日均值，不可跨周求和或平均；跨周须回 DWS 按目标周期重算 |
| 商品曝光 UV | `product_exposure_uv` | 已登录用户 | 周内 `product_exposure_uv` 之和 / 周内天数 | 无 | 周内日均值，不可跨周求和或平均；跨周须回 DWS 按目标周期重算 |
| 平台商品 UV 点击率 | `product_exposure_click_rate` | 已登录用户 | 周内 `SUM(product_click_uv) / SUM(product_exposure_uv)` | 无 | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| 人均点击次数 | `product_click_avg_count_per_click_user` | 已登录用户 | 周内 `SUM(product_click_pv_raw) / SUM(product_click_uv)` | 无 | 人均值，不可跨周求和或平均；跨周须用汇总分子分母重算 |
| 首页商品点击 UV | `home_product_click_uv` | 已登录用户 | 周内 `home_product_click_uv` 之和 / 周内天数 | 无 | 周内日均值，不可跨周求和或平均；跨周须回 DWS 按目标周期重算 |
| 首页商品曝光点击率 | `home_product_exposure_click_rate` | 已登录用户 | 周内 `home_product_click_uv` 之和 / 周内 `home_product_exposure_uv` 之和 | 无 | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| 试穿结果页点击 UV | `tryon_result_click_uv` | 已登录用户 | 周内 `tryon_result_click_uv` 之和 / 周内天数 | 无 | 周内日均值，不可跨周求和或平均；跨周须回 DWS 按目标周期重算 |
| 试穿结果页曝光点击率 | `tryon_result_exposure_click_rate` | 已登录用户 | 周内 `tryon_result_click_uv` 之和 / 周内 `tryon_result_exposure_uv` 之和 | 无 | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| 商城商品点击 UV | `shop_click_uv` | 已登录用户 | 周内 `shop_click_uv` 之和 / 周内天数 | 无 | 周内日均值，不可跨周求和或平均；跨周须回 DWS 按目标周期重算 |
| 商城商品曝光点击率 | `shop_exposure_click_rate` | 已登录用户 | 周内 `shop_click_uv` 之和 / 周内 `shop_exposure_uv` 之和 | 无 | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |

### 留存 OKR

留存类指标另有 `*_observed_day_count` 系列字段（`all_` / `new_` / `closet_` / `product_detail_` / `ai_tryon_` / `follow_` / `model_` 前缀），表示该指标周内「观察日已开始」的天数，是日均人数的分母。

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 平台整体 DAU | `platform_dau` | 已登录用户 | 已登录活跃用户；周内 `all_user_dau` 之和 / 周内天数 | 无 | 周内日均值，不可跨周求和或平均；跨周须回 DWS 按目标周期重算 |
| 次日留存率 | `next_day_retention_rate` | 已登录用户 cohort | 周内次日留存用户数求和 / 周内（次日留存 + 次日非留存）求和；cohort 主体为 `prop_user_id` | 观察日已开始的天 | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| 次日留存用户数 | `next_day_retained_user_count` | 已登录用户 cohort | 上式分子 / `all_next_day_observed_day_count` | 观察日已开始的天 | 周内日均人数，不可跨周求和 |
| 7 日留存率 | `day_7_retention_rate` | 已登录用户 cohort | 周内 7 日留存用户数求和 / 周内（7 日留存 + 7 日非留存）求和 | 观察日已开始的天 | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| 7 日留存用户数 | `day_7_retained_user_count` | 已登录用户 cohort | 上式分子按观察日已开始的天做日均 | 观察日已开始的天 | 周内日均人数，不可跨周求和 |
| 30 日留存率 | `day_30_retention_rate` | 已登录用户 cohort | 周内 30 日留存用户数求和 / 周内（30 日留存 + 30 日非留存）求和 | 观察日已开始的天 | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| 30 日留存用户数 | `day_30_retained_user_count` | 已登录用户 cohort | 上式分子按观察日已开始的天做日均 | 观察日已开始的天 | 周内日均人数，不可跨周求和 |
| 平台人均停留时长（分钟） | `platform_avg_duration_min` | 已登录用户 | 周内 `SUM(duration_sec) / SUM(登录用户数) / 60` | 无 | 人均值，不可跨周求和或平均；跨周须用汇总分子分母重算 |
| 平台人均内容消费 | `platform_avg_content_consume` | 已登录用户 | 周内 `SUM(content_consume_count) / SUM(登录用户数)` | 无 | 人均值，不可跨周求和或平均；跨周须用汇总分子分母重算 |
| 平台首页曝光点击率 | `home_feeds_exposure_click_rate` | 已登录用户 | 周内首页信息流点击登录用户数之和 / 曝光登录用户数之和；日层为 `c_home_feeds` 与 `v_home_feeds` 的 `COUNT(DISTINCT prop_user_id)` | 无 | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| 新用户 DAU | `new_user_dau` | 已登录用户 | 新用户为首日访问的已登录用户；周内 `new_user_dau` 之和 / 周内天数 | 无 | 周内日均值，不可跨周求和或平均；跨周须回 DWS 按目标周期重算 |
| 新用户次日留存率 | `new_user_next_day_retention_rate` | 新用户 cohort | 新用户 cohort 的周内次日留存用户数求和 / 周内（次日留存 + 次日非留存）求和 | 观察日已开始的天 | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| 新用户次日留存用户数 | `new_user_next_day_retained_user_count` | 新用户 cohort | 上式分子 / `new_next_day_observed_day_count` | 观察日已开始的天 | 周内日均人数，不可跨周求和 |
| 新用户人均停留时长（分钟） | `new_user_avg_duration_min` | 已登录用户 | 周内新用户 `SUM(duration_sec) / SUM(dau) / 60` | 无 | 人均值，不可跨周求和或平均；跨周须用汇总分子分母重算 |
| 新用户人均内容消费 | `new_user_avg_content_consume` | 已登录用户 | 周内新用户 `SUM(content_consume_count) / SUM(dau)` | 无 | 人均值，不可跨周求和或平均；跨周须用汇总分子分母重算 |
| 使用衣橱用户次日留存率 | `closet_user_next_day_retention_rate` | 行为 cohort | 周内次日留存用户求和 / 周内（次日留存 + 次日非留存）求和 | `user_group='全量登录用户'` 且 `feature_name='衣橱渗透'` | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| 使用衣橱用户次日留存用户数 | `closet_user_next_day_retained_user_count` | 行为 cohort | 上式分子按观察日已开始的天做日均 | `user_group='全量登录用户'` 且 `feature_name='衣橱渗透'` | 周内日均人数，不可跨周求和 |
| 浏览商详用户次日留存率 | `product_detail_user_next_day_retention_rate` | 行为 cohort | 周内次日留存用户求和 / 周内（次日留存 + 次日非留存）求和 | `user_group='全量登录用户'` 且 `feature_name='浏览商详'` | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| 浏览商详用户次日留存用户数 | `product_detail_user_next_day_retained_user_count` | 行为 cohort | 上式分子按观察日已开始的天做日均 | `user_group='全量登录用户'` 且 `feature_name='浏览商详'` | 周内日均人数，不可跨周求和 |
| 使用 AI 试穿用户次日留存率 | `ai_tryon_user_next_day_retention_rate` | 行为 cohort | 周内次日留存用户求和 / 周内（次日留存 + 次日非留存）求和 | `user_group='全量登录用户'` 且 `feature_name='AI试穿'` | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| 使用 AI 试穿用户次日留存用户数 | `ai_tryon_user_next_day_retained_user_count` | 行为 cohort | 上式分子按观察日已开始的天做日均 | `user_group='全量登录用户'` 且 `feature_name='AI试穿'` | 周内日均人数，不可跨周求和 |
| 关注账号用户次日留存率 | `follow_user_next_day_retention_rate` | 行为 cohort | 周内次日留存用户求和 / 周内（次日留存 + 次日非留存）求和 | `user_group='全量登录用户'` 且 `feature_name='关注账号'` | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| 关注账号用户次日留存用户数 | `follow_user_next_day_retained_user_count` | 行为 cohort | 上式分子按观察日已开始的天做日均 | `user_group='全量登录用户'` 且 `feature_name='关注账号'` | 周内日均人数，不可跨周求和 |
| 创建模特用户次日留存率 | `model_user_next_day_retention_rate` | 行为 cohort | 周内次日留存用户求和 / 周内（次日留存 + 次日非留存）求和 | `user_group='全量登录用户'` 且 `feature_name='创建模特'` | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| 创建模特用户次日留存用户数 | `model_user_next_day_retained_user_count` | 行为 cohort | 上式分子按观察日已开始的天做日均 | `user_group='全量登录用户'` 且 `feature_name='创建模特'` | 周内日均人数，不可跨周求和 |
| 使用 style me 用户次日留存率 | `styleme_user_next_day_retention_rate` | 行为 cohort | 周内次日留存用户求和 / 周内（次日留存 + 次日非留存）求和 | 当日服务端 `v3_decom.common_ext_ai_task.task_scene = 'STYLE_ME'` 的用户 | 率值，不可跨周 `AVG`；跨周须回 DWS 汇总分子分母后重算 |
| 使用 style me 用户次日留存用户数 | `styleme_user_next_day_retained_user_count` | 行为 cohort | 上式分子按观察日已开始的天做日均 | 当日服务端 `v3_decom.common_ext_ai_task.task_scene = 'STYLE_ME'` 的用户 | 周内日均人数，不可跨周求和 |

### 分布指标

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| AI 试穿时长分布 | `tryon_avg_duration_bucket_*_day_count / tryon_avg_duration_bucket_*_day_ratio` | 已登录用户 | 每用户周内全试穿成功任务总时长 / 成功任务数后分桶：`（0s,30s]`、`（30s,60s]`、`（60s,120s]`、`（120s,300s]`、`（300s,+∞)`、`无有效时长`；`*_day_count` 存桶内用户数，`*_day_ratio` 存用户占比，分母为周内去重 DAU | 数据来源 `dws_ai_fashion_tryon_okr_user_daily` | 占比不可跨周 `AVG`；用户数不可跨周求和，同一用户跨周会重复 |
| 人均每日试穿次数分布 | `tryon_avg_daily_count_bucket_*_day_count / tryon_avg_daily_count_bucket_*_day_ratio` | 已登录用户 | 每用户周内服务端试穿任务总数 / 周天数后分桶：`(0~3]`、`(3~6]`、`(6~10]`、`(10~15]`、`15次以上`、`0`；`0` 桶 = DAU − 有试穿行为用户数 | `task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')`；数据来源 `dws_ai_fashion_tryon_okr_user_daily` | 同上 |
| 人均商品点击次数分布 | `product_click_avg_daily_count_bucket_*_day_count / product_click_avg_daily_count_bucket_*_day_ratio` | 已登录用户 | 每用户周内商品点击 PV（`raw_event_id` 去重）/ 周天数后分桶：`(0~3]`、`(3~5]`、`(5~10]`、`(10~15]`、`15次以上`、`0`；`0` 桶 = DAU − 有点击行为用户数 | 数据来源 `dws_ai_fashion_tryon_okr_user_daily` | 同上 |

### v1 指标长表

`decom.ads_weekly_ai_fashion_tryon_okr` 是同一批指标的长表形态，粒度 `week_start_dt × metric_type × metric_name × metric_bucket`：`metric_type` 取 `试穿OKR核心指标` / `商品OKR核心指标` / `留存OKR核心指标` / `人均每日试穿次数分布` / `人均商品点击次数分布`，`metric_name` 与上面各表的指标名同名，`metric_value` 为对应数值，`metric_day_count` 为该指标的周内天数（核心指标默认周天数，次留相关用观察日已开始的天数，分布指标为对应桶天数），`metric_bucket` / `metric_bucket_order` 仅分布指标有值。口径与 v2 宽表一致，取数时二选一，不要把两张表的同名指标相加。

## 依赖

- 上游：`decom.dwd_event_log`、`decom.dim_user_first_active`、`v3_decom.common_ext_ai_task`
- 中层事实表：
  - `decom.dws_ai_fashion_tryon_okr_daily`
  - `decom.dws_user_activity_daily`
  - `decom.dws_user_daily`
  - `decom.dws_user_retention_feature_daily`
- 中层脚本：
  - `jobs/dws/dws_ai_fashion_tryon_okr_daily.py`
  - `jobs/dws/dws_user_activity_daily.py`
  - `jobs/dws/dws_user_daily.py`
  - `jobs/dws/dws_user_retention_feature_daily.py`
- 中层 DDL：
  - `ddl/dws/dws_ai_fashion_tryon_okr_daily.sql`
  - `ddl/dws/dws_user_activity_daily.sql`
  - `ddl/dws/dws_user_daily.sql`
  - `ddl/dws/dws_user_retention_feature_daily.sql`
- 终层脚本：
  - v1：`jobs/ads/ads_weekly_ai_fashion_tryon_okr.py`
  - v2（日）：`jobs/ads/ads_daily_ai_fashion_tryon_okr_v2.py`
  - v2：`jobs/ads/ads_weekly_ai_fashion_tryon_okr_v2.py`
- 终层 DDL：
  - v1：`ddl/ads/ads_weekly_ai_fashion_tryon_okr.sql`
  - v2（日）：`ddl/ads/ads_daily_ai_fashion_tryon_okr_v2.sql`
  - v2：`ddl/ads/ads_weekly_ai_fashion_tryon_okr_v2.sql`
- 编排：`env/etl_config.json`
  - v1：`ads_weekly_ai_fashion_tryon_okr`
  - v2（周）：`ads_weekly_ai_fashion_tryon_okr_v2`
  - v2（日）：`ads_daily_ai_fashion_tryon_okr_v2`
- Superset 资产（当前）：
  - Dashboard：`id=114`，slug=`ai-fashion-product-okr-summary`（deprecated）
  - Dashboard：`id=117`，slug=`ai-fashion-product-okr-summary-v2`（Week）
  - Dashboard：`id=120`，slug=`ai-fashion-product-okr-summary-day`（Day）
  - 物理 Dataset：`id=250`（`decom.ads_weekly_ai_fashion_tryon_okr`）
  - 物理 Dataset：`id=263`（`decom.ads_weekly_ai_fashion_tryon_okr_v2`）
  - 物理 Dataset：`id=268`（`decom.ads_daily_ai_fashion_tryon_okr_v2`）
  - 虚拟 Dataset（试穿核心指标）：`id=256`（`decom.vds_ai_fashion_tryon_okr_core_weekly`）
  - 虚拟 Dataset（试穿分布）：`id=257`（`decom.vds_ai_fashion_tryon_okr_bucket_weekly`）
  - 虚拟 Dataset（商品核心指标）：`id=258`（`decom.vds_ai_fashion_product_okr_core_weekly`）
  - 虚拟 Dataset（商品分布）：`id=259`（`decom.vds_ai_fashion_product_okr_bucket_weekly`）
  - Chart：`id=628`（`【AI-Fashion】试穿OKR（核心指标）`）
  - Chart：`id=629`（`【AI-Fashion】人均每日试穿次数分布（周）`）
  - Chart：`id=630`（`【AI-Fashion】商品OKR（核心指标）`）
  - Chart：`id=631`（`【AI-Fashion】人均商品点击次数分布（周）`）
  - Chart：`id=669`（`【AI-Fashion Day】试穿OKR（核心指标）`）
  - Chart：`id=670`（`【AI-Fashion Day】商品OKR（核心指标）`）
  - Chart：`id=671`（`【AI-Fashion Day】留存OKR（核心指标）`）
  - Chart：`id=672`（`【AI-Fashion Day】人均每日试穿次数分布`）
  - Chart：`id=673`（`【AI-Fashion Day】人均商品点击次数分布`）
  - Chart：`id=674`（`【AI-Fashion Day】AI试穿时长分布`）
