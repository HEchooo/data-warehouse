# 【Launch】商详页总览

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 指标 | 商品页 UV | 商详页访问人数、商详页曝光 UV |
| 指标 | 商品页 PV | 商详页访问次数、商详页曝光 PV |
| 指标 | 第三方外跳 UV | 外链点击人数、跳转用户数 |
| 指标 | 第三方外跳 PV | 外链点击次数、跳转次数 |
| 指标 | 外跳率 | 跳转率、外链点击率 |

## 适用范围

- 业务线：`Alvin's Club 商详页 Launch 投放`
- 适合：查询 Launch 商详页整体 PV、UV、第三方外跳和外跳率。
- 不适合：按视频、平台、红人、国家或商家拆分应使用[Launch 商详页多维度](../【Launch】商详页多维度/fact.md)。
- 需要澄清：需要确认日期范围，以及商详访问或第三方外跳指标。

## 报表

### ads_daily_launch_product_detail

- 表：`my-project-8584-jetonai.decom.ads_daily_launch_product_detail`
- 状态：`active`
- 粒度：`dt`（每个业务日一行）
- 时区：`America/Toronto`
- 数据范围：Launch 商详页访问和第三方外跳的全量日汇总。
- 必要过滤：无。
- 不包含：视频、平台、红人、国家和商家维度拆分。
- 埋点：`event_name IN ('v_launch_product_detail', 'c_launch_product_detail_shopnow')`
- Dashboard：[【Launch】商详页总览](https://bi.alvinclub.ca/superset/dashboard/launch-product-detail-overview/)（id=126）
- Superset：物理 dataset `ads_daily_launch_product_detail`（id=282），chart `【Launch】商详页总览 - 商品页 PV / UV 趋势`（id=701）、`【Launch】商详页总览 - 第三方外跳 PV / UV 趋势`（id=702）、`【Launch】商详页总览 - 外跳率趋势`（id=703）、`【Launch】商详页总览 - 明细表`（id=704）；native filter `日期范围`（filter_time）

### `dws_launch_product_detail_device_daily`

- 粒度：`dt × video_id × channel × kol_name × country × merchant × device_id`
- 说明：Launch 商详页主题设备 + 多维度中间层，沉淀每设备每日商品页浏览 PV 与第三方外跳 PV（均按 `raw_event_id` 去重），供本报表汇总总量 PV / UV 与外跳率，也供 [【Launch】商详页多维度](../【Launch】商详页多维度/fact.md) 按视频代码 / 投放平台 / 红人账号 / 国家 / 商家拆分
- 5 个维度均来自 `args` JSON：`video_id` 缺失归为「无法识别来源」，其余维度缺失归为「-」

## 字段

| 需求字段 | 定义 |
|---|---|
| 日期 | 报表日期，按 `America/New_York` 切天；底层 `logAt_timestamp` 需先按 `prop_timezone` 解释，再转换为美东业务日 |
| 商品页 PV | `event_name = 'v_launch_product_detail'` 的 `COUNT(DISTINCT raw_event_id)` |
| 商品页 UV | `event_name = 'v_launch_product_detail'` 的 `COUNT(DISTINCT prop_device_id)` |
| 第三方外跳 PV | `event_name = 'c_launch_product_detail_shopnow'` 的 `COUNT(DISTINCT raw_event_id)` |
| 第三方外跳 UV | `event_name = 'c_launch_product_detail_shopnow'` 的 `COUNT(DISTINCT prop_device_id)` |
| UV 外跳率 | 第三方外跳 UV / 商品页 UV |
| PV 外跳率 | 第三方外跳 PV / 商品页 PV |
| 筛选器 | 日期（美东时区） |

## 已落地字段

### `decom.ads_daily_launch_product_detail`

| 需求字段 | 落地表 | 落地字段 | 口径 |
|---|---|---|---|
| 日期 | `decom.ads_daily_launch_product_detail` | `dt` | 按 `America/New_York` 切天 |
| 商品页 PV | `decom.ads_daily_launch_product_detail` | `product_detail_pv` | 设备级 `raw_event_id` 去重后按日求和 |
| 商品页 UV | `decom.ads_daily_launch_product_detail` | `product_detail_uv` | 当日 `product_detail_pv > 0` 的 `device_id` 去重计数 |
| 第三方外跳 PV | `decom.ads_daily_launch_product_detail` | `shopnow_pv` | 设备级 `raw_event_id` 去重后按日求和 |
| 第三方外跳 UV | `decom.ads_daily_launch_product_detail` | `shopnow_uv` | 当日 `shopnow_pv > 0` 的 `device_id` 去重计数 |
| UV 外跳率 | `decom.ads_daily_launch_product_detail` | `uv_shopnow_rate` | `SAFE_DIVIDE(shopnow_uv, product_detail_uv)`；分母为 0 返回 NULL |
| PV 外跳率 | `decom.ads_daily_launch_product_detail` | `pv_shopnow_rate` | `SAFE_DIVIDE(shopnow_pv, product_detail_pv)`；分母为 0 返回 NULL |
| 设备 + 多维度中间层 | `decom.dws_launch_product_detail_device_daily` | `dt + video_id + channel + kol_name + country + merchant + device_id` | 主题事实；沉淀 `product_detail_pv`、`shopnow_pv`（均按 `raw_event_id` 去重）供本报表与多维度报表汇总 |

补充说明：

- PV 口径按 `raw_event_id` 去重；`raw_event_id` 全局唯一，设备级先 `COUNT(DISTINCT raw_event_id)`，ADS 再对设备求和即得当日全局 PV
- UV 口径按 `device_id` 去重；DWS 统一 `NULLIF(prop_device_id, '')` 过滤空串。DWS 现按 5 个维度拆分为多行，同一设备当日可能落在多个维度组合，ADS 必须用 `COUNT(DISTINCT device_id)` 而非按行 `COUNTIF` 计算 UV
- 外跳率为 ADS 层轻派生，分子分母（PV / UV）均已单独落地
- `ADS` 不直接读取 `dwd_event_log`，只消费设备级 `DWS`
- 零曝光日由 DWS/ADS 固定补最近两天，产出 PV/UV 为 0 的行；此时外跳率为 NULL

## 依赖

- 上游：`decom.dwd_event_log`
- 中间表：`decom.dws_launch_product_detail_device_daily`
- 报表表：`decom.ads_daily_launch_product_detail`
- 脚本：`jobs/dws/dws_launch_product_detail_device_daily.py`, `jobs/ads/ads_daily_launch_product_detail.py`
- DDL：`ddl/dws/dws_launch_product_detail_device_daily.sql`, `ddl/ads/ads_daily_launch_product_detail.sql`
- 编排：`env/etl_config.json`, `etl_run.py`
- Superset：物理 dataset `ads_daily_launch_product_detail`（id=282），chart id=701/702/703/704，dashboard `【Launch】商详页总览`（id=126）
- `dws_launch_product_detail_device_daily` 同时被 [【Launch】商详页多维度](../【Launch】商详页多维度/fact.md) 复用
