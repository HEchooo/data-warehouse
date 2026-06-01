# 产品 OKR 数据汇总

## 报表

### 主题总览

- 主题：`产品 OKR 数据汇总`
- 当前已落地逻辑：`试穿OKR`、`商品OKR`、`留存OKR`
- 周维度：固定按 `周一~周日` 切周
- 周标签：`MMDD-MMDD`（例如 `0427-0503`）

### 试穿OKR

- 表：`ads_weekly_ai_fashion_tryon_okr`
- 粒度：`week_start_dt + metric_name + metric_bucket(可空)`
- Dashboard：[产品 OKR 数据汇总](https://bi.alvinclub.ca/superset/dashboard/ai-fashion-product-okr-summary/)（id=114）
- Chart（核心指标）：[【AI-Fashion】试穿OKR（核心指标）](https://bi.alvinclub.ca/superset/explore/?slice_id=628)（id=628）
- Chart（分布）：[【AI-Fashion】人均每日试穿次数分布（周）](https://bi.alvinclub.ca/superset/explore/?slice_id=629)（id=629）

### 商品OKR

- 表：`ads_weekly_ai_fashion_tryon_okr`
- 粒度：`week_start_dt + metric_name + metric_bucket(可空)`
- Dashboard：[产品 OKR 数据汇总](https://bi.alvinclub.ca/superset/dashboard/ai-fashion-product-okr-summary/)（id=114）
- Chart（核心指标）：[【AI-Fashion】商品OKR（核心指标）](https://bi.alvinclub.ca/superset/explore/?slice_id=630)（id=630）
- Chart（分布）：[【AI-Fashion】人均商品点击次数分布（周）](https://bi.alvinclub.ca/superset/explore/?slice_id=631)（id=631）

### 留存OKR

- 表：`ads_weekly_ai_fashion_tryon_okr`
- 粒度：`week_start_dt + metric_name`
- Dashboard：[产品 OKR 数据汇总](https://bi.alvinclub.ca/superset/dashboard/ai-fashion-product-okr-summary/)（id=114）
- 当前状态：指标逻辑已写入周表，留存 OKR 图表待创建

## 字段

### 需求字段（完整清单）

#### 试穿OKR

| 指标 | 口径 |
|---|---|
| 新用户试穿率 | 周内 `tryon_new_uv` 之和 / 周内 `dau_new_uv` 之和 |
| 新用户试穿uv | 周内 `tryon_new_uv` 之和 / 周内天数 |
| 老用户试穿率 | 周内 `tryon_old_uv` 之和 / 周内 `dau_old_uv` 之和 |
| 老用户试穿uv | 周内 `tryon_old_uv` 之和 / 周内天数 |
| 首页信息流试穿 UV | 周内 `home_tryon_uv` 之和 / 周内天数 |
| 首页试穿渗透率 | 周内 `home_tryon_uv` 之和 / 周内 `home_feeds_uv` 之和 |
| 帖子试穿UV | 周内 `post_tryon_uv` 之和 / 周内天数 |
| 帖子试穿渗透率 | 周内 `post_tryon_uv` 之和 / 周内 `post_scene_exposure_uv` 之和 |
| 商品详情页试穿 UV | 周内 `product_detail_tryon_uv` 之和 / 周内天数 |
| 商品详情页试穿渗透率 | 周内 `product_detail_tryon_uv` 之和 / 周内 `product_detail_uv` 之和 |
| styleme 试穿 UV | 周内 `styleme_uv` 之和 / 周内天数 |
| 人均每日试穿次数分布 | 先算每日 `tryon_pv / tryon_uv_total`，再按分桶统计周内 `day_count` 与 `day_ratio`；分桶：`(0~3]`、`(3~6]`、`(6~10]`、`(10~15]`、`15次以上`、`0或无试穿UV` |

#### 商品OKR

| 指标 | 口径 |
|---|---|
| 商品点击 UV | 周内 `product_click_uv` 之和 / 周内天数 |
| 商品曝光 UV | 周内 `product_exposure_uv` 之和 / 周内天数 |
| 首页商品点击 UV | 周内 `home_product_click_uv` 之和 / 周内天数 |
| 首页商品曝光点击率 | 周内 `home_product_click_uv` 之和 / 周内 `home_product_exposure_uv` 之和 |
| 试穿结果页点击 UV | 周内 `tryon_result_click_uv` 之和 / 周内天数 |
| 试穿结果页曝光点击率 | 周内 `tryon_result_click_uv` 之和 / 周内 `tryon_result_exposure_uv` 之和 |
| 商城商品点击 UV | 周内 `shop_click_uv` 之和 / 周内天数 |
| 商城商品曝光点击率 | 周内 `shop_click_uv` 之和 / 周内 `shop_exposure_uv` 之和 |
| 人均商品点击次数分布 | 先算每日 `product_click_pv_raw / product_click_uv`，再按分桶统计周内 `day_count`；分桶：`(0~3]`、`(3~5]`、`(5~10]`、`(10~15]`、`15次以上`、`0或无点击UV` |

#### 留存OKR

| 指标 | 口径 |
|---|---|
| 次日留存率 | 以 `prop_user_id` 为 cohort 主体；周内 `次日留存用户数` 求和 / 周内 `(次日留存用户数 + 次日非留存用户数)` 求和 |
| 次日留存用户数 | 基于上式分子，按周内“观察日已开始”的天做日均 |
| 平台人均停留时长（min） | 周内 `SUM(duration_sec)` / 周内 `SUM(登录用户数)` / `60` |
| 平台人均内容消费 | 周内 `SUM(content_consume_count)` / 周内 `SUM(登录用户数)` |
| 新用户 DAU | 新用户定义为“首日访问的已登录用户”；周内 `new_user_dau` 之和 / 周内天数 |
| 新用户次日留存 | 新用户 cohort 的周内 `次日留存用户数` 求和 / 周内 `(次日留存用户数 + 次日非留存用户数)` 求和 |
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

### 已落地字段

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

补充说明：

- 周级数值统一遵循“先按天计算，再在周内按规则聚合”
- 率指标统一按“周内分子求和 / 周内分母求和”，不直接 `AVG(每日率)`
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
- 终层脚本：`jobs/ads/ads_weekly_ai_fashion_tryon_okr.py`
- 终层 DDL：`ddl/ads/ads_weekly_ai_fashion_tryon_okr.sql`
- 编排：`env/etl_config.json`（`ads_weekly_ai_fashion_tryon_okr` 依赖已覆盖上述 4 张 DWS）
- Superset 资产（当前）：
  - Dashboard：`id=114`，slug=`ai-fashion-product-okr-summary`
  - 物理 Dataset：`id=250`（`decom.ads_weekly_ai_fashion_tryon_okr`）
  - 虚拟 Dataset（试穿核心指标）：`id=256`（`decom.vds_ai_fashion_tryon_okr_core_weekly`）
  - 虚拟 Dataset（试穿分布）：`id=257`（`decom.vds_ai_fashion_tryon_okr_bucket_weekly`）
  - 虚拟 Dataset（商品核心指标）：`id=258`（`decom.vds_ai_fashion_product_okr_core_weekly`）
  - 虚拟 Dataset（商品分布）：`id=259`（`decom.vds_ai_fashion_product_okr_bucket_weekly`）
  - Chart：`id=628`（`【AI-Fashion】试穿OKR（核心指标）`）
  - Chart：`id=629`（`【AI-Fashion】人均每日试穿次数分布（周）`）
  - Chart：`id=630`（`【AI-Fashion】商品OKR（核心指标）`）
  - Chart：`id=631`（`【AI-Fashion】人均商品点击次数分布（周）`）

