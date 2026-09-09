# 产品 OKR 数据汇总

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | 产品 OKR 数据汇总 | 产品OKR、产品 OKR 看板、产品 OKR 试穿周报 |
| 指标 | 平台整体 DAU | 平台日活、日活用户数 |
| 指标 | 次日留存率 | D1 留存率、1 日留存率 |
| 指标 | 7 日留存率 | D7 留存率、第 7 日留存率 |
| 指标 | 30 日留存率 | D30 留存率、第 30 日留存率 |
| 指标 | AI 试穿成功率 | 试穿成功率、Try On Success Rate |

## 报表

### 主题总览

- 主题：`产品 OKR 数据汇总`
- 当前已落地逻辑：`试穿OKR`、`商品OKR`、`留存OKR`
- 周维度：固定按 `周一~周日` 切周
- 周标签：`MMDD-MMDD`（例如 `0427-0503`）

### 试穿OKR

- 表：`ads_weekly_ai_fashion_tryon_okr`
- 粒度：`week_start_dt + metric_name + metric_bucket(可空)`
- Dashboard：[产品 OKR 数据汇总（deprecated）](https://bi.alvinclub.ca/superset/dashboard/ai-fashion-product-okr-summary/)（id=114）
- Chart（核心指标）：[【AI-Fashion】试穿OKR（核心指标）](https://bi.alvinclub.ca/superset/explore/?slice_id=628)（id=628）
- Chart（分布）：[【AI-Fashion】人均每日试穿次数分布（周）](https://bi.alvinclub.ca/superset/explore/?slice_id=629)（id=629）

### 商品OKR

- 表：`ads_weekly_ai_fashion_tryon_okr`
- 粒度：`week_start_dt + metric_name + metric_bucket(可空)`
- Dashboard：[产品 OKR 数据汇总（deprecated）](https://bi.alvinclub.ca/superset/dashboard/ai-fashion-product-okr-summary/)（id=114）
- Chart（核心指标）：[【AI-Fashion】商品OKR（核心指标）](https://bi.alvinclub.ca/superset/explore/?slice_id=630)（id=630）
- Chart（分布）：[【AI-Fashion】人均商品点击次数分布（周）](https://bi.alvinclub.ca/superset/explore/?slice_id=631)（id=631）

### 留存OKR

- 表：`ads_weekly_ai_fashion_tryon_okr`
- 粒度：`week_start_dt + metric_name`
- Dashboard：[产品 OKR 数据汇总（deprecated）](https://bi.alvinclub.ca/superset/dashboard/ai-fashion-product-okr-summary/)（id=114）
- 当前状态：指标逻辑已写入周表，留存 OKR 图表待创建

### v2 周级宽表

- 表：`ads_weekly_ai_fashion_tryon_okr_v2`
- 粒度：`week_start_dt`
- 当前状态：已建表，已回刷 `0427-0503` 至 `0615-0621` 周桶，已接入 `env/etl_config.json` 调度
- 设计目标：复用 v1 已验证指标逻辑，将 `metric_name / metric_value` 的 long form 转为每个指标一个字段，便于 Superset 直接可视化
- Dashboard：[产品 OKR 数据汇总（Week）](https://bi.alvinclub.ca/superset/dashboard/ai-fashion-product-okr-summary-v2/)（id=117）
- 物理 Dataset：`id=263`（`decom.ads_weekly_ai_fashion_tryon_okr_v2`）
- Chart（试穿核心指标）：`id=644`
- Chart（商品核心指标）：`id=645`
- Chart（留存核心指标）：`id=646`
- Chart（试穿分布）：`id=647`
- Chart（试穿时长分布）：`id=649`
- Chart（商品点击分布）：`id=648`

### v2 日级宽表（Day）

- 表：`ads_daily_ai_fashion_tryon_okr_v2`
- 粒度：`dt`
- 当前状态：物理表已存在；仓库脚本 / DDL / 编排已补齐；Superset 物理 Dataset、6 张图和 Day dashboard 已创建；`2026-04-27` ~ `2026-06-29` 历史已完成回刷
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

### 需求字段（完整清单）

#### 试穿OKR

| 指标 | 口径 |
|---|---|
| 新用户试穿率 | 周内服务端试穿新用户 `tryon_new_uv` 之和 / 周内 `dau_new_uv` 之和；试穿为 `task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')` |
| 新用户试穿UV | 周内服务端试穿新用户 `tryon_new_uv` 之和 / 周内天数 |
| 老用户试穿率 | 周内服务端试穿老用户 `tryon_old_uv` 之和 / 周内 `dau_old_uv` 之和；试穿为 `task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')` |
| 老用户试穿UV | 周内服务端试穿老用户 `tryon_old_uv` 之和 / 周内天数 |
| 首页信息流试穿 UV | 周内 `home_tryon_uv` 之和 / 周内天数；保留前端 `c_tryon AND args_page_key = 'home_feeds'` |
| 首页试穿渗透率 | 周内 `home_tryon_uv` 之和 / 周内 `home_feeds_uv` 之和 |
| 帖子试穿UV | 周内服务端带 `request_param.postId` 且可映射 `dim_post` 的试穿用户 `post_tryon_uv` 之和 / 周内天数；试穿为 `task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')` |
| 帖子试穿渗透率 | 周内服务端帖子试穿 `post_tryon_uv` 之和 / 周内前端帖子场景曝光 `post_scene_exposure_uv` 之和 |
| 商品详情页试穿 UV | 周内 `product_detail_tryon_uv` 之和 / 周内天数；保留前端 `c_tryon AND args_page_key = 'product_detail'` |
| 商品详情页试穿渗透率 | 周内 `product_detail_tryon_uv` 之和 / 周内 `product_detail_uv` 之和 |
| styleme 试穿 UV | 周内 `styleme_uv` 之和 / 周内天数 |
| 平台试穿率 | 周内服务端试穿 `tryon_uv_total` 之和 / 周内平台 DAU 之和；平台 DAU 口径与留存 OKR 的 `platform_dau` 一致 |
| 人均试穿次数 | 周内服务端试穿次数 `tryon_pv` 之和 / 周内服务端试穿用户 `tryon_uv_total` 之和 |
| 日均试穿UV | 周内服务端试穿用户 `tryon_uv_total` 之和 / 周内天数 |
| 前端tryon点击UV | 周内前端 `event_name = 'c_tryon'` 的 `prop_user_id` UV 之和 / 周内天数 |
| 商城信息流试穿渗透率 | 周内 `c_tryon AND args_page_key = 'shop_feeds'` 的 UV 之和 / 周内 `v_shop_feeds` UV 之和 |
| 商城信息流试穿UV | 周内 `c_tryon AND args_page_key = 'shop_feeds'` 的 UV 之和 / 周内天数 |
| 衣橱试穿渗透率 | 周内 `c_tryon AND args_page_key = 'closet_feeds'` 的 UV 之和 / 周内 `v_closet_feeds` UV 之和 |
| 衣橱试穿UV | 周内 `c_tryon AND args_page_key = 'closet_feeds'` 的 UV 之和 / 周内天数 |
| styleme试穿渗透率 | 周内服务端 `styleme_uv` 之和 / 周内前端 `v_styleme_popup` 或 `v_styleme_page` UV 之和 |
| AI试穿成功率（quick） | 周内服务端 `task_type = 'QUICK_TRY_ON'` 且 `status = 'SUCCESS'` 的任务数 / 周内服务端 `task_type = 'QUICK_TRY_ON'` 的任务数；按 `task_id` 去重 |
| AI试穿成功率（plus） | 周内服务端 `task_type = 'TRY_ON'` 且 `status = 'SUCCESS'` 的任务数 / 周内服务端 `task_type = 'TRY_ON'` 的任务数；按 `task_id` 去重 |
| AI试穿任务平均完成时长（quick） | 周内服务端 `task_type = 'QUICK_TRY_ON'` 且 `status = 'SUCCESS'` 的成功任务 `json_value(output, '$.totalDuration')` 总和 / 有效成功任务数；`totalDuration` 单位毫秒，展示转分钟 |
| AI试穿任务平均完成时长（plus） | 周内服务端 `task_type = 'TRY_ON'` 且 `status = 'SUCCESS'` 的成功任务 `json_value(output, '$.totalDuration')` 总和 / 有效成功任务数；`totalDuration` 单位毫秒，展示转分钟 |
| AI试穿时长分布 | 按用户周内人均任务完成时长分桶：每用户的全试穿成功任务总时长 / 成功任务数，分母为周内去重 DAU；`totalDuration` 单位毫秒；分桶：`（0s,30s]`、`（30s,60s]`、`（60s,120s]`、`（120s,300s]`、`（300s,+∞)`、`无有效时长`；数据来源 `dws_ai_fashion_tryon_okr_user_daily` |
| 人均每日试穿次数分布 | 按用户周内日均试穿次数分桶：每用户的周内服务端试穿任务总数 / 周天数，分母为周内去重 DAU；试穿为 `task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')`；分桶：`(0~3]`、`(3~6]`、`(6~10]`、`(10~15]`、`15次以上`、`0`；`0` = DAU − 有试穿行为用户数；数据来源 `dws_ai_fashion_tryon_okr_user_daily` |

#### 商品OKR

| 指标 | 口径 |
|---|---|
| 商品点击 UV | 周内 `product_click_uv` 之和 / 周内天数 |
| 商品曝光 UV | 周内 `product_exposure_uv` 之和 / 周内天数 |
| 平台商品UV点击率 | 周内 `product_click_uv` 之和 / 周内 `product_exposure_uv` 之和 |
| 人均点击次数 | 周内 `product_click_pv_raw` 之和 / 周内 `product_click_uv` 之和 |
| 首页商品点击 UV | 周内 `home_product_click_uv` 之和 / 周内天数 |
| 首页商品曝光点击率 | 周内 `home_product_click_uv` 之和 / 周内 `home_product_exposure_uv` 之和 |
| 试穿结果页点击 UV | 周内 `tryon_result_click_uv` 之和 / 周内天数 |
| 试穿结果页曝光点击率 | 周内 `tryon_result_click_uv` 之和 / 周内 `tryon_result_exposure_uv` 之和 |
| 商城商品点击 UV | 周内 `shop_click_uv` 之和 / 周内天数 |
| 商城商品曝光点击率 | 周内 `shop_click_uv` 之和 / 周内 `shop_exposure_uv` 之和 |
| 人均商品点击次数分布 | 按用户周内日均商品点击次数分桶：每用户的周内商品点击 PV(raw_event_id 去重) / 周天数，分母为周内去重 DAU；分桶：`(0~3]`、`(3~5]`、`(5~10]`、`(10~15]`、`15次以上`、`0`；`0` = DAU − 有点击行为用户数；数据来源 `dws_ai_fashion_tryon_okr_user_daily` |

#### 留存OKR

| 指标 | 口径 |
|---|---|
| 平台整体DAU | 已登录活跃用户；周内 `all_user_dau` 之和 / 周内天数 |
| 次日留存率 | 以 `prop_user_id` 为 cohort 主体；周内 `次日留存用户数` 求和 / 周内 `(次日留存用户数 + 次日非留存用户数)` 求和 |
| 次日留存用户数 | 基于上式分子，按周内“观察日已开始”的天做日均 |
| 7日留存率 | 以 `prop_user_id` 为 cohort 主体；周内 `7日留存用户数` 求和 / 周内 `(7日留存用户数 + 7日非留存用户数)` 求和 |
| 7日留存用户数 | 基于上式分子，按周内“观察日已开始”的天做日均 |
| 30日留存率 | 以 `prop_user_id` 为 cohort 主体；周内 `30日留存用户数` 求和 / 周内 `(30日留存用户数 + 30日非留存用户数)` 求和 |
| 30日留存用户数 | 基于上式分子，按周内“观察日已开始”的天做日均 |
| 平台人均停留时长（min） | 周内 `SUM(duration_sec)` / 周内 `SUM(登录用户数)` / `60` |
| 平台人均内容消费 | 周内 `SUM(content_consume_count)` / 周内 `SUM(登录用户数)` |
| 平台首页曝光点击率 | 周内首页信息流点击登录用户数之和 / 周内首页信息流曝光登录用户数之和；日层为 `c_home_feeds count(distinct prop_user_id)` / `v_home_feeds count(distinct prop_user_id)` |
| 新用户 DAU | 新用户定义为“首日访问的已登录用户”；周内 `new_user_dau` 之和 / 周内天数 |
| 新用户次日留存率 | 新用户 cohort 的周内 `次日留存用户数` 求和 / 周内 `(次日留存用户数 + 次日非留存用户数)` 求和 |
| 新用户次日留存用户数 | 新用户 cohort 的周内 `次日留存用户数` 求和 / 周内“观察日已开始”的天数 |
| 新用户人均停留时长（min） | 周内新用户 `SUM(duration_sec)` / 周内新用户 `SUM(dau)` / `60` |
| 新用户人均消费内容 | 周内新用户 `SUM(content_consume_count)` / 周内新用户 `SUM(dau)` |
| 使用衣橱用户次日留存率 | `user_group='全量登录用户'` 且 `feature_name='衣橱渗透'`；周内 `次日留存用户` 求和 / 周内 `(次日留存用户 + 次日非留存用户)` 求和 |
| 使用衣橱用户次日留存用户数 | 上一指标分子，按周内“观察日已开始”的天做日均 |
| 浏览商详次日留存率 | `user_group='全量登录用户'` 且 `feature_name='浏览商详'`；周内 `次日留存用户` 求和 / 周内 `(次日留存用户 + 次日非留存用户)` 求和 |
| 浏览商详次日留存用户数 | 上一指标分子，按周内“观察日已开始”的天做日均 |
| 使用AI试穿次日留存率 | `user_group='全量登录用户'` 且 `feature_name='AI试穿'`；周内 `次日留存用户` 求和 / 周内 `(次日留存用户 + 次日非留存用户)` 求和 |
| 使用AI试穿次日留存用户数 | 上一指标分子，按周内“观察日已开始”的天做日均 |
| 关注账号次日留存率 | `user_group='全量登录用户'` 且 `feature_name='关注账号'`；周内 `次日留存用户` 求和 / 周内 `(次日留存用户 + 次日非留存用户)` 求和 |
| 关注账号次日留存用户数 | 上一指标分子，按周内“观察日已开始”的天做日均 |
| 创建模特次日留存率 | `user_group='全量登录用户'` 且 `feature_name='创建模特'`；周内 `次日留存用户` 求和 / 周内 `(次日留存用户 + 次日非留存用户)` 求和 |
| 创建模特次日留存用户数 | 上一指标分子，按周内“观察日已开始”的天做日均 |
| 使用style me用户次日留存率 | 当日服务端 `v3_decom.common_ext_ai_task.task_scene = 'STYLE_ME'` 用户；周内 `次日留存用户` 求和 / 周内 `(次日留存用户 + 次日非留存用户)` 求和 |
| 使用style me用户次日留存用户数 | 上一指标分子，按周内“观察日已开始”的天做日均 |

### 已落地字段

#### v1 指标长表

| 需求字段 | 落地表 | 落地字段 | 口径 |
|---|---|---|---|
| 周开始日期 | `decom.ads_weekly_ai_fashion_tryon_okr` | `week_start_dt` | 周一日期 |
| 周结束日期 | `decom.ads_weekly_ai_fashion_tryon_okr` | `week_end_dt` | 周日日期 |
| 周标签 | `decom.ads_weekly_ai_fashion_tryon_okr` | `week_label` | `MMDD-MMDD` |
| 指标分类 | `decom.ads_weekly_ai_fashion_tryon_okr` | `metric_type` | `试穿OKR核心指标` / `商品OKR核心指标` / `留存OKR核心指标` / `人均每日试穿次数分布` / `人均商品点击次数分布` |
| 指标名称 | `decom.ads_weekly_ai_fashion_tryon_okr` | `metric_name` | 与需求字段同名（不带编号） |
| 分桶序号 | `decom.ads_weekly_ai_fashion_tryon_okr` | `metric_bucket_order` | 仅分布指标有值 |
| 分桶名称 | `decom.ads_weekly_ai_fashion_tryon_okr` | `metric_bucket` | 仅分布指标有值 |
| 周内天数 | `decom.ads_weekly_ai_fashion_tryon_okr` | `metric_day_count` | 核心指标默认为周天数；次留相关人数/比率类使用“观察日已开始”的天数；分布指标为对应桶天数 |
| 指标值 | `decom.ads_weekly_ai_fashion_tryon_okr` | `metric_value` | 对应指标数值（率 / 日均 UV / 日均人数 / 人均值 / 分桶值）；试穿分布为 `day_ratio`，点击分布为 `day_count` |

#### v2 指标宽表

| 字段类型 | 落地表 | 落地字段 | 口径 |
|---|---|---|---|
| 周开始日期 | `decom.ads_weekly_ai_fashion_tryon_okr_v2` | `week_start_dt` | 周一日期 |
| 周结束日期 | `decom.ads_weekly_ai_fashion_tryon_okr_v2` | `week_end_dt` | 周日日期 |
| 周标签 | `decom.ads_weekly_ai_fashion_tryon_okr_v2` | `week_label` | `MMDD-MMDD` |
| 周内天数 | `decom.ads_weekly_ai_fashion_tryon_okr_v2` | `week_day_count` | 周日历天数，默认 7 |
| 次留观察天数 | `decom.ads_weekly_ai_fashion_tryon_okr_v2` | `all_next_day_observed_day_count` / `new_next_day_observed_day_count` / `closet_next_day_observed_day_count` / `product_detail_next_day_observed_day_count` / `ai_tryon_next_day_observed_day_count` / `follow_next_day_observed_day_count` / `model_next_day_observed_day_count` | 对应留存指标周内“观察日已开始”的天数 |
| 试穿 OKR 核心指标 | `decom.ads_weekly_ai_fashion_tryon_okr_v2` | `new_user_tryon_rate` / `new_user_tryon_uv` / `old_user_tryon_rate` / `old_user_tryon_uv` / `home_tryon_uv` / `home_tryon_penetration_rate` / `post_tryon_uv` / `post_tryon_penetration_rate` / `product_detail_tryon_uv` / `product_detail_tryon_penetration_rate` / `styleme_tryon_uv` / `platform_tryon_rate` / `tryon_avg_count_per_tryon_user` / `avg_daily_tryon_uv` / `front_tryon_click_uv` / `shop_feeds_tryon_penetration_rate` / `shop_feeds_tryon_uv` / `closet_feeds_tryon_penetration_rate` / `closet_feeds_tryon_uv` / `styleme_tryon_penetration_rate` / `quick_tryon_success_rate` / `plus_tryon_success_rate` / `quick_tryon_avg_duration_min` / `plus_tryon_avg_duration_min` | v1 已有指标对齐同名 `metric_name` 的 `metric_value`；v2 新增指标直接从 DWS 分子分母周聚合 |
| 商品 OKR 核心指标 | `decom.ads_weekly_ai_fashion_tryon_okr_v2` | `product_click_uv` / `product_exposure_uv` / `product_exposure_click_rate` / `product_click_avg_count_per_click_user` / `home_product_click_uv` / `home_product_exposure_click_rate` / `tryon_result_click_uv` / `tryon_result_exposure_click_rate` / `shop_click_uv` / `shop_exposure_click_rate` | `product_click_uv` 等 v1 已有指标对齐同名 `metric_name` 的 `metric_value`；`product_exposure_click_rate` 按周 `SUM(product_click_uv) / SUM(product_exposure_uv)`；`product_click_avg_count_per_click_user` 按周 `SUM(product_click_pv_raw) / SUM(product_click_uv)` |
| 留存 OKR 核心指标 | `decom.ads_weekly_ai_fashion_tryon_okr_v2` | `platform_dau` / `next_day_retention_rate` / `next_day_retained_user_count` / `day_7_retention_rate` / `day_7_retained_user_count` / `day_30_retention_rate` / `day_30_retained_user_count` / `home_feeds_exposure_click_rate` / `platform_avg_duration_min` / `platform_avg_content_consume` / `new_user_dau` / `new_user_next_day_retention_rate` / `new_user_next_day_retained_user_count` / `new_user_avg_duration_min` / `new_user_avg_content_consume` / `closet_user_next_day_retention_rate` / `closet_user_next_day_retained_user_count` / `product_detail_user_next_day_retention_rate` / `product_detail_user_next_day_retained_user_count` / `ai_tryon_user_next_day_retention_rate` / `ai_tryon_user_next_day_retained_user_count` / `follow_user_next_day_retention_rate` / `follow_user_next_day_retained_user_count` / `model_user_next_day_retention_rate` / `model_user_next_day_retained_user_count` / `styleme_user_next_day_retention_rate` / `styleme_user_next_day_retained_user_count` | v1 已有指标对齐同名 `metric_name`；v2 新增指标直接从 DWS 分子分母周聚合 |
| AI试穿时长分布 | `decom.ads_weekly_ai_fashion_tryon_okr_v2` | `tryon_avg_duration_bucket_*_day_count` / `tryon_avg_duration_bucket_*_day_ratio` | 按用户周内人均任务完成时长分桶；`*_day_count` 实际存储桶内用户数，`*_day_ratio` 存储用户占比（分母为周内去重 DAU）；数据来源 `dws_ai_fashion_tryon_okr_user_daily` |
| 人均每日试穿次数分布 | `decom.ads_weekly_ai_fashion_tryon_okr_v2` | `tryon_avg_daily_count_bucket_*_day_count` / `tryon_avg_daily_count_bucket_*_day_ratio` | 按用户周内日均试穿次数分桶；`*_day_count` 实际存储桶内用户数，`*_day_ratio` 存储用户占比（分母为周内去重 DAU）；数据来源 `dws_ai_fashion_tryon_okr_user_daily` |
| 人均商品点击次数分布 | `decom.ads_weekly_ai_fashion_tryon_okr_v2` | `product_click_avg_daily_count_bucket_*_day_count` / `product_click_avg_daily_count_bucket_*_day_ratio` | 按用户周内日均商品点击次数分桶；`*_day_count` 实际存储桶内用户数，`*_day_ratio` 存储用户占比（分母为周内去重 DAU）；新增 `*_day_ratio` 列；数据来源 `dws_ai_fashion_tryon_okr_user_daily` |

补充说明：

- 周级数值统一遵循“先按天计算，再在周内按规则聚合”
- 率指标统一按“周内分子求和 / 周内分母求和”，不直接 `AVG(每日率)`
- 方案 B 后，整体试穿用户、整体试穿次数、帖子试穿和使用 AI 试穿留存统一使用服务端 `common_ext_ai_task` 三种试穿任务；首页和商详试穿因依赖页面来源，继续使用前端 `c_tryon`
- 帖子试穿渗透率是跨源转化口径：分子为服务端 `postId` 归因后的帖子试穿 UV，分母为前端帖子场景曝光 UV
- 留存口径统一使用 `prop_user_id`
- 周桶从 `2026-04-27` 开始，固定每 7 天（周一~周日）

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
