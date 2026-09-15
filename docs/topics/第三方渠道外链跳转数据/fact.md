# 第三方渠道外链跳转数据

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | 第三方渠道外链跳转数据 | 第三方渠道曝光 UV 看板 |
| 指标 | 渠道曝光 UV | 渠道曝光人数 |
| 指标 | 视频详情页曝光 PV | 视频详情页曝光次数 |
| 指标 | 商品点击个数 | 商品点击数、外链点击量 |

## 适用范围

- 业务线：`Alvin's Club App`
- 适合：按第三方渠道查询曝光 UV、视频详情曝光和商品点击。
- 不适合：LTA 专属页面和商品转化应使用[LTA 数据看板](../LTA 数据看板/fact.md)；Launch 商详页外跳应使用[Launch 商详页总览](../【Launch】商详页总览/fact.md)。
- 需要澄清：需要确认渠道、日期范围和曝光或点击指标。

## 报表

### ads_daily_thirdapp_channel_uv

- 表：`my-project-8584-jetonai.decom.ads_daily_thirdapp_channel_uv`
- 状态：`active`
- 粒度：`dt × channel`
- 时区：`America/Toronto`
- 数据范围：App 第三方渠道曝光、视频详情曝光和商品点击日汇总。
- 必要过滤：全渠道去重结果使用文档定义的 `overall` 行。
- 不包含：LTA 专属页面商品转化和 Launch 商详页外跳。
- Dashboard：[【AI-Fashion】第三方渠道外链跳转数据](https://bi.alvinclub.ca/superset/dashboard/111/)（id=111）
- Superset：物理 dataset `ads_daily_thirdapp_channel_uv`（id=244），chart `第三方渠道曝光 UV（日表）- 曝光 UV 趋势`（id=607）、`第三方渠道曝光 UV（日表）- 视频详情页曝光 PV 趋势`（id=677）、`第三方渠道曝光 UV（日表）- 商品点击个数趋势`（id=678）、`第三方渠道曝光 UV（日表）- 明细表`（id=608）

### `dws_thirdapp_channel_device_daily`

- 粒度：`dt × channel × device_id`
- 说明：第三方渠道主题设备级中间层，用于渠道粒度 UV 聚合、`overall` 全渠道全局去重，以及视频详情曝光 / 商品点击指标沉淀

## 字段

| 需求字段 | 定义 |
|---|---|
| 日期 | 报表日期，按 `America/New_York` 切天；底层 `logAt_timestamp` 需先按 `prop_timezone` 解释，再转换为美东业务日 |
| 渠道 | 仅纳入 `args.sf` 非空串且非 `NULL` 的事件；若 `args.app_name` 为空且 `args.sf like '%youtube%'`，记为 `youtube`；其他情况使用 `args.app_name` 的规范化值 |
| 分渠道曝光 UV | 每日每渠道 `COUNT(DISTINCT prop_device_id)` |
| overall 曝光 UV | 同日对全部渠道的 `prop_device_id` 做全局去重后的 UV；不能直接对分渠道 UV 求和 |
| 视频详情页曝光 PV | `event_name = 'v_kol_post_detail_page'` 的事件条数；按相同 `dt × channel` 聚合 |
| 商品点击个数 | `event_name = 'c_third_party_goods'` 的 `COUNT(product_code)`；`product_code` 为空或 `NULL` 不计入 |
| 明细表 | 展示 `dt`、`channel`、`exposure_uv`、`video_detail_exposure_pv`、`product_click_count` |
| 筛选器 | 至少包含日期范围、渠道 |

## 已落地字段

### `decom.ads_daily_thirdapp_channel_uv`

| 需求字段 | 落地表 | 落地字段 | 口径 |
|---|---|---|---|
| 日期 | `decom.ads_daily_thirdapp_channel_uv` | `dt` | 按 `America/New_York` 切天 |
| 渠道 | `decom.ads_daily_thirdapp_channel_uv` | `channel` | 真实渠道值 + `overall` 汇总行 |
| 分渠道曝光 UV | `decom.ads_daily_thirdapp_channel_uv` | `exposure_uv` | `channel != 'overall'` 时，按 `dt + channel` 对设备去重 |
| overall 曝光 UV | `decom.ads_daily_thirdapp_channel_uv` | `exposure_uv` | `channel = 'overall'` 时，按 `dt` 对全渠道设备全局去重 |
| 视频详情页曝光 PV | `decom.ads_daily_thirdapp_channel_uv` | `video_detail_exposure_pv` | `event_name = 'v_kol_post_detail_page'` 的计数；`channel = 'overall'` 时为同日全渠道求和 |
| 商品点击个数 | `decom.ads_daily_thirdapp_channel_uv` | `product_click_count` | `event_name = 'c_third_party_goods'` 的 `COUNT(product_code)`；`product_code` 为空或 `NULL` 不计；`channel = 'overall'` 时为同日全渠道求和 |
| 设备级中间层 | `decom.dws_thirdapp_channel_device_daily` | `dt + channel + device_id` | 设备级主题事实；同一设备同日同渠道保留一行，沉淀 `thirdapp_open_pv`、`video_detail_exposure_pv`、`product_click_count` 供 ADS 汇总 |

补充说明：

- 当前已验证样本中，`event_name = 'v_thirdapp_open'` 事件时间范围为美东 `2026-05-12` 到 `2026-05-18`
- 当前样本中 `args.app_name` 不存在 `NULL`，只有 6 条空串，且这 6 条全部满足 `sf like '%youtube%'`
- 当前口径要求 `args.sf` 为空串或 `NULL` 的事件全部过滤，不进入渠道映射和 UV 统计
- 为避免未来脏数据丢失，若后续出现 `app_name` 仍为空且 `sf` 不命中 youtube，则 DWS 先兜底记为 `unknown`
- 视频详情页曝光 PV 与商品点击个数使用相同渠道映射：仅纳入 `args.sf` 非空串、非 `NULL` 的事件，并沿用 youtube 特判 + `app_name` 规范化值
- 商品点击个数当前按 `COUNT(product_code)` 实现；若上游未传 `product_code`，该条点击不会进入计数
- `ADS` 不直接读取 `dwd_event_log`，而是只消费设备级 `DWS`

## 依赖

- 上游：`decom.dwd_event_log`
- 中间表：`decom.dws_thirdapp_channel_device_daily`
- 报表表：`decom.ads_daily_thirdapp_channel_uv`
- 脚本：`jobs/dws/dws_thirdapp_channel_device_daily.py`, `jobs/ads/ads_daily_thirdapp_channel_uv.py`
- DDL：`ddl/dws/dws_thirdapp_channel_device_daily.sql`, `ddl/ads/ads_daily_thirdapp_channel_uv.sql`
- 编排：`env/etl_config.json`, `etl_run.py`, `jobs/ads/ads_daily_thirdapp_channel_uv.py`
- Superset：物理 dataset `ads_daily_thirdapp_channel_uv`（id=244），chart `第三方渠道曝光 UV（日表）- 曝光 UV 趋势`（id=607）、`第三方渠道曝光 UV（日表）- 视频详情页曝光 PV 趋势`（id=677）、`第三方渠道曝光 UV（日表）- 商品点击个数趋势`（id=678）、`第三方渠道曝光 UV（日表）- 明细表`（id=608），dashboard `【AI-Fashion】第三方渠道外链跳转数据`（id=111）
