# 【Launch】商详页多维度

## 报表

- 明细表：`ads_daily_launch_product_detail_dimension`（粒度 `dt`）/ `ads_weekly_launch_product_detail_dimension`（粒度 `week_start_dt`，周一至周日）/ `ads_monthly_launch_product_detail_dimension`（粒度 `month`）
- 来源概览表：`ads_daily_launch_product_detail_video_source`（粒度 `dt`）/ `ads_weekly_launch_product_detail_video_source`（粒度 `week_start_dt`）/ `ads_monthly_launch_product_detail_video_source`（粒度 `month`）
- 埋点：`event_name = 'v_launch_product_detail'`
- Dashboard：[【Launch】商详页多维度](https://bi.alvinclub.ca/superset/dashboard/launch-product-detail-dimension/)（id=127）
- Superset：
  - dataset：`ads_daily_launch_product_detail_dimension`（id=283）/ `ads_weekly_launch_product_detail_dimension`（id=284）/ `ads_monthly_launch_product_detail_dimension`（id=285）/ `ads_daily_launch_product_detail_video_source`（id=286）/ `ads_weekly_launch_product_detail_video_source`（id=287）/ `ads_monthly_launch_product_detail_video_source`（id=288）
  - chart：`日报明细表`（id=705）/ `周报明细表`（id=706）/ `月报明细表`（id=707）/ `日报来源概览`（id=708）/ `周报来源概览`（id=709）/ `月报来源概览`（id=710）
  - 周报明细表 / 周报来源概览的周期列显示为单一 `周期`（如 `2026-07-20 ~ 2026-07-26`），由 `week_start_dt` + `week_end_dt` 在 chart 层用 `CONCAT(FORMAT_DATE(...))` 拼接展示，不展示两个独立日期字段
  - 月报明细表 / 月报来源概览的 `month` 列展示格式为 `%Y-%m`（如 `2026-07`），不显示日
  - native filter：`日报日期` / `周报日期` / `月报日期`（`filter_time`，分别只对各自 dt/week/month 的 2 个 chart 生效）；`视频代码` / `投放平台` / `红人账号` / `国家` / `商家`（`filter_select`，全局生效，target dataset 为 id=283）

### `dws_launch_product_detail_device_daily`

- 粒度：`dt × video_id × channel × kol_name × country × merchant × device_id`
- 说明：与 [【Launch】商详页总览](../【Launch】商详页总览/fact.md) 共用同一张 DWS，本主题下所有报表只消费其中 `product_detail_pv`，不含第三方外跳

## 字段

### 明细表（按 5 个维度拆分）

| 需求字段 | 定义 |
|---|---|
| 视频代码 | `args.video_id`；为空归为「无法识别来源」 |
| 投放平台 | `args.channel`；为空归为「-」 |
| 红人账号 | `args.kol_name`；为空归为「-」 |
| 国家 | `args.country`；为空归为「-」 |
| 商家 | `args.merchant`；为空归为「-」 |
| 商品页 PV | `COUNT(DISTINCT raw_event_id)` |
| 商品页 UV | `COUNT(DISTINCT prop_device_id)` |
| PV 贡献占比 | 当前维度组合 PV / 当前 dt/week/month 全维度合计 PV |
| UV 贡献占比 | 当前维度组合 UV / 当前 dt/week/month 全维度合计 UV（合计 UV 按 `device_id` 去重，不对各维度 UV 求和） |
| 人均访问次数 | 当前维度组合 PV / 当前维度组合 UV |
| 周期粒度 | dt / week（周一起，ISO 周）/ month 三个独立表 |

### 来源概览表（不拆维度，按 video_id 是否有来源分桶）

| 需求字段 | 定义 |
|---|---|
| 商品页 PV | 当期全量 `COUNT(DISTINCT raw_event_id)`（不区分来源） |
| 商品页 UV | 当期全量 `COUNT(DISTINCT prop_device_id)`（不区分来源） |
| 有来源 UV | `video_id != '无法识别来源'` 的 `prop_device_id` 去重计数 |
| 无法识别来源 UV | `video_id = '无法识别来源'` 的 `prop_device_id` 去重计数 |
| 周期粒度 | dt / week（周一起）/ month 三个独立表 |

## 已落地字段

### `decom.ads_daily_launch_product_detail_dimension` / `decom.ads_weekly_launch_product_detail_dimension` / `decom.ads_monthly_launch_product_detail_dimension`

| 需求字段 | 落地字段 | 口径 |
|---|---|---|
| 日期/周/月 | `dt` / `week_start_dt` + `week_end_dt` / `month` | 周固定周一至周日；月为月初日期 |
| 视频代码 | `video_id` | `COALESCE(NULLIF(JSON_VALUE(args,'$.video_id'), ''), '无法识别来源')` |
| 投放平台 | `channel` | `COALESCE(NULLIF(JSON_VALUE(args,'$.channel'), ''), '-')` |
| 红人账号 | `kol_name` | `COALESCE(NULLIF(JSON_VALUE(args,'$.kol_name'), ''), '-')` |
| 国家 | `country` | `COALESCE(NULLIF(JSON_VALUE(args,'$.country'), ''), '-')` |
| 商家 | `merchant` | `COALESCE(NULLIF(JSON_VALUE(args,'$.merchant'), ''), '-')` |
| 商品页 PV | `product_detail_pv` | 维度组合内 `raw_event_id` 去重后求和 |
| 商品页 UV | `product_detail_uv` | 维度组合内 `pv > 0` 的 `device_id` 去重计数 |
| PV 贡献占比 | `pv_contribution_rate` | `SAFE_DIVIDE(维度 PV, 当期全维度合计 PV)` |
| UV 贡献占比 | `uv_contribution_rate` | `SAFE_DIVIDE(维度 UV, 当期全维度合计 UV)`；合计 UV 单独按 `device_id` 去重计算 |
| 人均访问次数 | `avg_visit_per_uv` | `SAFE_DIVIDE(维度 PV, 维度 UV)` |

### `decom.ads_daily_launch_product_detail_video_source` / `decom.ads_weekly_launch_product_detail_video_source` / `decom.ads_monthly_launch_product_detail_video_source`

| 需求字段 | 落地字段 | 口径 |
|---|---|---|
| 日期/周/月 | `dt` / `week_start_dt` + `week_end_dt` / `month` | 同上 |
| 商品页 PV | `product_detail_pv` | 当期全量 `raw_event_id` 去重后求和 |
| 商品页 UV | `product_detail_uv` | 当期全量 `pv > 0` 的 `device_id` 去重计数 |
| 有来源 UV | `has_source_uv` | `video_id != '无法识别来源' AND pv > 0` 的 `device_id` 去重计数 |
| 无法识别来源 UV | `no_source_uv` | `video_id = '无法识别来源' AND pv > 0` 的 `device_id` 去重计数 |

补充说明：

- 只统计商品页浏览（`v_launch_product_detail`），不含第三方外跳
- 明细表维度组合天然稀疏，不做零值行兜底（与总览报表固定补零曝光日不同）
- 分母为 0 时 `SAFE_DIVIDE` 返回 NULL
- 同一设备当期若既有可识别来源事件、也有无法识别来源事件，会分别计入 `has_source_uv` 和 `no_source_uv` 两个桶，因此二者之和不一定等于 `product_detail_uv`

## 依赖

- 上游：`decom.dwd_event_log`
- 中间表：`decom.dws_launch_product_detail_device_daily`（与总览报表共用）
- 报表表：`decom.ads_daily_launch_product_detail_dimension`, `decom.ads_weekly_launch_product_detail_dimension`, `decom.ads_monthly_launch_product_detail_dimension`, `decom.ads_daily_launch_product_detail_video_source`, `decom.ads_weekly_launch_product_detail_video_source`, `decom.ads_monthly_launch_product_detail_video_source`
- 脚本：`jobs/ads/ads_daily_launch_product_detail_dimension.py`, `jobs/ads/ads_weekly_launch_product_detail_dimension.py`, `jobs/ads/ads_monthly_launch_product_detail_dimension.py`, `jobs/ads/ads_daily_launch_product_detail_video_source.py`, `jobs/ads/ads_weekly_launch_product_detail_video_source.py`, `jobs/ads/ads_monthly_launch_product_detail_video_source.py`
- DDL：`ddl/ads/ads_daily_launch_product_detail_dimension.sql`, `ddl/ads/ads_weekly_launch_product_detail_dimension.sql`, `ddl/ads/ads_monthly_launch_product_detail_dimension.sql`, `ddl/ads/ads_daily_launch_product_detail_video_source.sql`, `ddl/ads/ads_weekly_launch_product_detail_video_source.sql`, `ddl/ads/ads_monthly_launch_product_detail_video_source.sql`
- 编排：`env/etl_config.json`, `etl_run.py`
- Superset：dataset id=283/284/285/286/287/288，chart id=705/706/707/708/709/710，dashboard `【Launch】商详页多维度`（id=127）
