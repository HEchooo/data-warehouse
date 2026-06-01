# 产品 OKR 数据汇总

## 报表

### 主题总览

- 主题：`产品 OKR 数据汇总`
- 当前先交付：`试穿OKR` chart
- 后续规划：`留存OKR` chart、`商品OKR` chart
- 周维度：固定按 `周一~周日` 切周
- 周标签：`MMDD-MMDD`（例如 `0427-0503`）

### 试穿OKR

- 表：`ads_weekly_ai_fashion_tryon_okr`
- 粒度：`week_start_dt + metric_name + metric_bucket(可空)`
- Dashboard：[产品 OKR 数据汇总](https://bi.alvinclub.ca/superset/dashboard/ai-fashion-product-okr-summary/)（id=114）
- Chart（核心指标）：[【AI-Fashion】试穿OKR（核心指标）](https://bi.alvinclub.ca/superset/explore/?slice_id=628)（id=628）
- Chart（分布）：[【AI-Fashion】人均每日试穿次数分布（周）](https://bi.alvinclub.ca/superset/explore/?slice_id=629)（id=629）

## 字段

### 需求字段（完整清单）

#### 试穿OKR（本次落地）

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

#### 留存OKR（待落地）

| 指标 | 口径状态 |
|---|---|
| 留存OKR 全部字段 | 待补充（本次未实现） |

#### 商品OKR（待落地）

| 指标 | 口径状态 |
|---|---|
| 商品OKR 全部字段 | 待补充（本次未实现） |

### 已落地字段

| 需求字段 | 落地表 | 落地字段 | 口径 |
|---|---|---|---|
| 周开始日期 | `decom.ads_weekly_ai_fashion_tryon_okr` | `week_start_dt` | 周一日期 |
| 周结束日期 | `decom.ads_weekly_ai_fashion_tryon_okr` | `week_end_dt` | 周日日期 |
| 周标签 | `decom.ads_weekly_ai_fashion_tryon_okr` | `week_label` | `MMDD-MMDD` |
| 指标分类 | `decom.ads_weekly_ai_fashion_tryon_okr` | `metric_type` | `试穿OKR核心指标` / `人均每日试穿次数分布` |
| 指标名称 | `decom.ads_weekly_ai_fashion_tryon_okr` | `metric_name` | 与需求字段同名（不带编号） |
| 分桶序号 | `decom.ads_weekly_ai_fashion_tryon_okr` | `metric_bucket_order` | 仅分布指标有值 |
| 分桶名称 | `decom.ads_weekly_ai_fashion_tryon_okr` | `metric_bucket` | 仅分布指标有值 |
| 周内天数 | `decom.ads_weekly_ai_fashion_tryon_okr` | `metric_day_count` | 核心指标为周天数；分布指标为对应桶的天数 |
| 指标值 | `decom.ads_weekly_ai_fashion_tryon_okr` | `metric_value` | 对应指标数值（率或日均 UV 或 day_ratio） |

补充说明：

- 周级数值统一遵循“先按天计算，再在周内按规则聚合”
- 率指标统一按“周内分子求和 / 周内分母求和”，不直接 `AVG(每日率)`
- 分布指标与 `docs/topics/试穿与商品行为临时取数（2026-05）/query.sql` 保持一致

## 依赖

- 上游：`decom.dwd_event_log`、`decom.dim_user_first_active`、`v3_decom.common_ext_ai_task`
- 中层事实表：`decom.dws_ai_fashion_tryon_okr_daily`
- 中层脚本：`jobs/dws/dws_ai_fashion_tryon_okr_daily.py`
- 中层 DDL：`ddl/dws/dws_ai_fashion_tryon_okr_daily.sql`
- 终层脚本：`jobs/ads/ads_weekly_ai_fashion_tryon_okr.py`
- 终层 DDL：`ddl/ads/ads_weekly_ai_fashion_tryon_okr.sql`
- 编排：`env/etl_config.json`（`dws_ai_fashion_tryon_okr_daily`、`ads_weekly_ai_fashion_tryon_okr`）
- Superset 资产：
  - Dashboard：`id=114`，slug=`ai-fashion-product-okr-summary`
  - 物理 Dataset：`id=250`（`decom.ads_weekly_ai_fashion_tryon_okr`）
  - 虚拟 Dataset（核心指标）：`id=256`（`decom.vds_ai_fashion_tryon_okr_core_weekly`）
  - 虚拟 Dataset（分布）：`id=257`（`decom.vds_ai_fashion_tryon_okr_bucket_weekly`）
  - Chart：`id=628`（`【AI-Fashion】试穿OKR（核心指标）`）
  - Chart：`id=629`（`【AI-Fashion】人均每日试穿次数分布（周）`）

补充说明：

- 为避免“同一物理表多指标类型混查”风险，chart 不再直接消费 `id=250`，而是分别消费两个虚拟 Dataset。
- `id=256` 在 SQL 层固定 `metric_type = '试穿OKR核心指标'`。
- `id=257` 在 SQL 层固定 `metric_type = '人均每日试穿次数分布'`。
