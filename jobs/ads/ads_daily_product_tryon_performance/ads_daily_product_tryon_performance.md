# 商详页试穿维度 (ads_daily_product_tryon_performance)

## 报表定义

用于分析 **商详页试穿维度** 的核心指标，支持按 **日期 × SPU × SKU** 过滤/聚合。

Filters：
- 日期：`dt`
- SPU：`spu`
- SKU：`sku`（部分来源无 SKU，值允许为空）

## 表结构

| 字段 | 类型 | 说明 |
|------|------|------|
| dt | DATE | 日期（多伦多时间） |
| spu | STRING | SPU（来自 `dwd_event_log.product_code`，即 `args_spu` 展开后的单值） |
| sku | STRING | SKU（来自 `dwd_event_log.args_sku`；首页 Feed 流曝光无 SKU，置空） |
| exposure_uv | INT64 | 商品曝光UV（包含商详页 + 首页 Feed 流；分类页/购物车暂未纳入） |
| tryon_total_count | INT64 | 商品被试穿次数（PV，仅商详页触发的试穿） |
| add_cart_uv | INT64 | 商品加购UV（全量加购UV，不区分来源） |
| update_time | TIMESTAMP | 更新时间（UTC） |

**分区**: `dt`（按日期分区）

## 指标口径

### SPU / SKU 取值

- SPU：使用 `dwd_event_log.product_code`（由 `args_spu` 展开得到的单个 SPU）。
  - 说明：`dwd_event_log` 会对 `args_spu` 做数组展开，因此同一事件可能生成多行（每个 SPU 一行）。
- SKU：使用 `NULLIF(dwd_event_log.args_sku, '')`。

### 商品曝光UV (exposure_uv)

- **事件**：`v_product_detail`（商详页曝光）、`v_home_feeds`（首页 Feed 流曝光，仅统计产品曝光行）
- **SPU**：只统计 `product_code` 非空（即产品 SPU 可识别）的行
- **SKU**：
  - `v_product_detail`：使用 `args_sku`
  - `v_home_feeds`：无 SKU，落表为 `NULL`
- **去重维度**：`visitor_id = COALESCE(NULLIF(prop_user_id,''), NULLIF(prop_device_id,''))`
- **计算方式**：按 `dt × spu × sku` 统计 `COUNT(DISTINCT visitor_id)`
- **说明**：分类页/购物车当前无对应曝光埋点，因此暂未纳入 `exposure_uv`；后续补埋点后再扩展事件范围即可。

### 商品被试穿次数（PV）(tryon_total_count)

- **事件**：`c_tryon`
- **限定**：只统计商详页场景，即 `args_page_key = 'product_detail'`
- **去重维度**：按 `action_event_id = COALESCE(raw_event_id, hash_id)` 去重，避免 `dwd_event_log` 因展开导致 PV 放大
- **计算方式**：按 `dt × spu × sku` 统计 `COUNT(DISTINCT action_event_id)`

### 商品加购UV (add_cart_uv)

- **事件**：`c_add_cart`
- **说明**：统计全量加购UV，不区分来源（如首页 Feed 流、分类页、商详页、试穿结果页等）
- **去重维度**：`visitor_id`
- **计算方式**：按 `dt × spu × sku` 统计 `COUNT(DISTINCT visitor_id)`

## 数据来源

```
ads_daily_product_tryon_performance
    └── dwd_event_log
```

## 时区说明

| 字段 | 时区 |
|------|------|
| dt | America/Toronto（多伦多时间） |
| update_time | UTC |

## 更新机制

- **调度周期**: T+1（每日更新）
- **更新方式**: DELETE + INSERT（幂等）
- **增量判断**: 由入口任务 `jobs/ads/ads_daily/ads_daily.py` 基于 DWS 的 `update_time` 决定待处理日期

## 使用示例

```sql
-- 最近 7 天，按 SPU 汇总曝光UV、试穿次数、加购UV
SELECT
    dt,
    spu,
    SUM(exposure_uv) AS exposure_uv,
    SUM(tryon_total_count) AS tryon_total_count,
    SUM(add_cart_uv) AS add_cart_uv
FROM `my-project-8584-jetonai.decom.ads_daily_product_tryon_performance`
WHERE dt >= DATE_SUB(CURRENT_DATE('America/Toronto'), INTERVAL 7 DAY)
GROUP BY dt, spu
ORDER BY dt DESC, spu;
```

## 相关文件

- **DDL**: `ddl/ads/ads_daily_product_tryon_performance.sql`
- **任务脚本**: `jobs/ads/ads_daily_product_tryon_performance/ads_daily_product_tryon_performance.py` (函数 `run_ads_daily_product_tryon_performance`)
- **入口脚本**: `jobs/ads/ads_daily/ads_daily.py`

## Superset BI 看板

- **Dashboard**：`【AI-Fashion】商详页试穿维度`（dashboard_id=98）
  - 访问路径：`/superset/dashboard/98/`
  - 默认时间范围：`Last week`
  - 原生筛选器：`日期范围（多伦多）`、`SPU`、`SKU`（SKU 允许为空）
- **Dataset**：`decom.ads_daily_product_tryon_performance`（dataset_id=227，main_dttm_col=`dt`，database_id=5）
- **Charts**：
  - 564：`【AI-Fashion】商详页试穿维度-规模趋势`
  - 565：`【AI-Fashion】商详页试穿维度-转化趋势`
  - 566：`【AI-Fashion】商详页试穿维度-TopSPU`
  - 567：`【AI-Fashion】商详页试穿维度-TopSKU`
  - 568：`【AI-Fashion】商详页试穿维度-明细表`
- **生成脚本**：`.agents/skills/superset/scripts/create_ads_daily_product_tryon_performance_dashboard.py`
