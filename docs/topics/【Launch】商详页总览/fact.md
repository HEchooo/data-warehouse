# 【Launch】商详页总览

## 报表

- 表：`ads_daily_launch_product_detail`
- 粒度：`dt`（每个业务日一行）
- 埋点：`event_name IN ('v_launch_product_detail', 'c_launch_product_detail_shopnow')`
- Dashboard：[【Launch】商详页总览](https://bi.alvinclub.ca/superset/dashboard/launch-product-detail-overview/)（id=126）
- Superset：物理 dataset `ads_daily_launch_product_detail`（id=282），chart `【Launch】商详页总览 - 商品页 PV / UV 趋势`（id=701）、`【Launch】商详页总览 - 第三方外跳 PV / UV 趋势`（id=702）、`【Launch】商详页总览 - 外跳率趋势`（id=703）、`【Launch】商详页总览 - 明细表`（id=704）；native filter `日期范围`（filter_time）

### `dws_launch_product_detail_device_daily`

- 粒度：`dt × device_id`
- 说明：Launch 商详页主题设备级中间层，沉淀每设备每日商品页浏览 PV 与第三方外跳 PV（均按 `raw_event_id` 去重），供 ADS 汇总 PV / UV 与外跳率

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
| 商品页 UV | `decom.ads_daily_launch_product_detail` | `product_detail_uv` | 当日 `product_detail_pv > 0` 的设备计数 |
| 第三方外跳 PV | `decom.ads_daily_launch_product_detail` | `shopnow_pv` | 设备级 `raw_event_id` 去重后按日求和 |
| 第三方外跳 UV | `decom.ads_daily_launch_product_detail` | `shopnow_uv` | 当日 `shopnow_pv > 0` 的设备计数 |
| UV 外跳率 | `decom.ads_daily_launch_product_detail` | `uv_shopnow_rate` | `SAFE_DIVIDE(shopnow_uv, product_detail_uv)`；分母为 0 返回 NULL |
| PV 外跳率 | `decom.ads_daily_launch_product_detail` | `pv_shopnow_rate` | `SAFE_DIVIDE(shopnow_pv, product_detail_pv)`；分母为 0 返回 NULL |
| 设备级中间层 | `decom.dws_launch_product_detail_device_daily` | `dt + device_id` | 设备级主题事实；沉淀 `product_detail_pv`、`shopnow_pv`（均按 `raw_event_id` 去重）供 ADS 汇总 |

补充说明：

- PV 口径按 `raw_event_id` 去重；`raw_event_id` 全局唯一，设备级先 `COUNT(DISTINCT raw_event_id)`，ADS 再对设备求和即得当日全局 PV
- UV 口径按 `prop_device_id` 去重；DWS 统一 `NULLIF(prop_device_id, '')` 过滤空串
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
