# LTA 数据看板

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | LTA 数据看板 | MyCCH、mycch.me 曝光点击看板 |
| 指标 | 页面曝光 PV / UV | 页面曝光次数、页面曝光人数 |
| 指标 | 商品曝光 UV | 商品曝光人数 |
| 指标 | 商品点击 UV | 商品点击人数 |
| 指标 | 商品点击转化率 | 商品点击率 |

## 适用范围

- 业务线：`MyCCH`
- 适合：查询 `mycch.me` 页面、红人和商品维度的曝光、点击与转化。
- 不适合：Alvin's Club 商详页投放应使用[AI-Fashion 商详页投放](../【AI-Fashion】商详页投放/fact.md)；其他第三方渠道跳转应使用[第三方渠道外链跳转数据](../第三方渠道外链跳转数据/fact.md)。
- 需要澄清：需要确认页面、红人或商品粒度，以及 PV 或 UV 统计主体。

## 报表

### ads_daily_lta_overview

- 表：`my-project-8584-jetonai.decom.ads_daily_lta_overview`
- 状态：`active`
- 粒度：一行代表一个 `dt`
- 时区：`America/New_York`
- 数据范围：`mycch.me` 页面与商品曝光点击的日总览。
- 必要过滤：`args.current_url` 域名为 `mycch.me`。
- 不包含：红人和商品明细。

### ads_daily_lta_kol

- 表：`my-project-8584-jetonai.decom.ads_daily_lta_kol`
- 状态：`active`
- 粒度：一行代表一个 `dt × 红人`
- 时区：`America/New_York`
- 数据范围：`mycch.me` 按红人拆分的曝光点击。
- 必要过滤：`args.current_url` 域名为 `mycch.me`。
- 不包含：商品明细。

### ads_daily_lta_spu

- 表：`my-project-8584-jetonai.decom.ads_daily_lta_spu`
- 状态：`active`
- 粒度：一行代表一个 `dt × 商品`
- 时区：`America/New_York`
- 数据范围：`mycch.me` 按商品拆分的曝光点击。
- 必要过滤：`args.current_url` 域名为 `mycch.me`。
- 不包含：红人明细。

### Dashboard

- Dashboard：[LTA 数据看板](https://bi.alvinclub.ca/superset/dashboard/125/)（id=125）
- Superset：dataset `ads_daily_lta_overview`（id=279）/ `ads_daily_lta_kol`（id=280）/ `ads_daily_lta_spu`（id=281）；chart `LTA 数据看板 - 日期总览`（id=698）/ `LTA 数据看板 - 红人维度`（id=699）/ `LTA 数据看板 - 商品维度`（id=700）；native filter 日期范围 / 红人名称 / 商品名称

## 字段

三张表只统计 `args.current_url` 域名为 `mycch.me` 的三个第三方事件：`v_third_party_page`（页面曝光）、`v_third_party_goods`（商品曝光）、`c_third_party_goods`（商品点击）。日期按 `prop_timezone` 解释后转 `America/New_York` 切美东业务日。PV 统一为 `COUNT(DISTINCT raw_event_id)`，UV 统一为 `COUNT(DISTINCT prop_device_id)`。

### ads_daily_lta_overview

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 页面曝光 PV | `page_exposure_pv` | 事件 | `v_third_party_page` 的 `COUNT(DISTINCT raw_event_id)` | `NET.HOST(args.current_url) = 'mycch.me'` | 可跨天求和；跨红人或跨商品会重复计数，须回 `dws_lta_thirdparty_event_daily` 重算 |
| 页面曝光 UV | `page_exposure_uv` | 设备 | `v_third_party_page` 的 `COUNT(DISTINCT prop_device_id)` | `NET.HOST(args.current_url) = 'mycch.me'` | 不可跨天、跨红人或跨商品求和；须回 `dws_lta_thirdparty_event_daily` 重新去重 |
| 商品曝光 PV | `goods_exposure_pv` | 事件 | `v_third_party_goods` 的 `COUNT(DISTINCT raw_event_id)` | `NET.HOST(args.current_url) = 'mycch.me'` | 可跨天求和；跨红人或跨商品会重复计数，须回 `dws_lta_thirdparty_event_daily` 重算 |
| 商品曝光 UV | `goods_exposure_uv` | 设备 | `v_third_party_goods` 的 `COUNT(DISTINCT prop_device_id)` | `NET.HOST(args.current_url) = 'mycch.me'` | 不可跨天、跨红人或跨商品求和；须回 `dws_lta_thirdparty_event_daily` 重新去重 |
| 商品点击 PV | `goods_click_pv` | 事件 | `c_third_party_goods` 的 `COUNT(DISTINCT raw_event_id)` | `NET.HOST(args.current_url) = 'mycch.me'` | 可跨天求和；跨红人或跨商品会重复计数，须回 `dws_lta_thirdparty_event_daily` 重算 |
| 商品点击 UV | `goods_click_uv` | 设备 | `c_third_party_goods` 的 `COUNT(DISTINCT prop_device_id)` | `NET.HOST(args.current_url) = 'mycch.me'` | 不可跨天、跨红人或跨商品求和；须回 `dws_lta_thirdparty_event_daily` 重新去重 |
| 商品点击转化率（PV） | `goods_click_cvr_pv` | 事件 | `SAFE_DIVIDE(goods_click_pv, goods_exposure_pv)`；分母为 0 时返回 `NULL` | 无 | 不可 `AVG`；跨天或跨维度须分别汇总点击与曝光后重算 |
| 商品点击转化率（UV） | `goods_click_cvr_uv` | 设备 | `SAFE_DIVIDE(goods_click_uv, goods_exposure_uv)`；分母为 0 时返回 `NULL` | 无 | 不可 `AVG`；跨天或跨维度须分别汇总点击与曝光后重算 |

### ads_daily_lta_kol

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 红人 ID | `kol_id` | 红人 | `args.current_url` 的 `kolUserId` 参数值；仅保留非空事件 | `NET.HOST(args.current_url) = 'mycch.me'` | 不聚合 |
| 红人名称 | `kol_nickname` | 红人 | `kol_id` 关联 `v3_decom.kol_rel.user_id` 取 `nickname` | 无 | 不聚合 |
| 页面曝光 PV | `page_exposure_pv` | 事件 | `v_third_party_page` 的 `COUNT(DISTINCT raw_event_id)` | `NET.HOST(args.current_url) = 'mycch.me'` | 可跨天求和；跨红人或跨商品会重复计数，须回 `dws_lta_thirdparty_event_daily` 重算 |
| 页面曝光 UV | `page_exposure_uv` | 设备 | `v_third_party_page` 的 `COUNT(DISTINCT prop_device_id)` | `NET.HOST(args.current_url) = 'mycch.me'` | 不可跨天、跨红人或跨商品求和；须回 `dws_lta_thirdparty_event_daily` 重新去重 |
| 商品曝光 PV | `goods_exposure_pv` | 事件 | `v_third_party_goods` 的 `COUNT(DISTINCT raw_event_id)` | `NET.HOST(args.current_url) = 'mycch.me'` | 可跨天求和；跨红人或跨商品会重复计数，须回 `dws_lta_thirdparty_event_daily` 重算 |
| 商品曝光 UV | `goods_exposure_uv` | 设备 | `v_third_party_goods` 的 `COUNT(DISTINCT prop_device_id)` | `NET.HOST(args.current_url) = 'mycch.me'` | 不可跨天、跨红人或跨商品求和；须回 `dws_lta_thirdparty_event_daily` 重新去重 |
| 商品点击 PV | `goods_click_pv` | 事件 | `c_third_party_goods` 的 `COUNT(DISTINCT raw_event_id)` | `NET.HOST(args.current_url) = 'mycch.me'` | 可跨天求和；跨红人或跨商品会重复计数，须回 `dws_lta_thirdparty_event_daily` 重算 |
| 商品点击 UV | `goods_click_uv` | 设备 | `c_third_party_goods` 的 `COUNT(DISTINCT prop_device_id)` | `NET.HOST(args.current_url) = 'mycch.me'` | 不可跨天、跨红人或跨商品求和；须回 `dws_lta_thirdparty_event_daily` 重新去重 |
| 商品点击转化率（PV） | `goods_click_cvr_pv` | 事件 | `SAFE_DIVIDE(goods_click_pv, goods_exposure_pv)`；分母为 0 时返回 `NULL` | 无 | 不可 `AVG`；跨天或跨维度须分别汇总点击与曝光后重算 |
| 商品点击转化率（UV） | `goods_click_cvr_uv` | 设备 | `SAFE_DIVIDE(goods_click_uv, goods_exposure_uv)`；分母为 0 时返回 `NULL` | 无 | 不可 `AVG`；跨天或跨维度须分别汇总点击与曝光后重算 |

### ads_daily_lta_spu

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 商品 ID | `spu` | 商品 | `args.spu`；商品曝光是 feeds 数组（展开到商品级），商品点击是标量；取值可能是链接 URL 或 SP 编码，一律原样当商品 ID | `NET.HOST(args.current_url) = 'mycch.me'`，且 `spu` 非空 | 不聚合 |
| 商品名称 | `spu_name` | 商品 | `args.spu_name`，与 `spu` 在 feeds 数组中同位对应；商品点击事件不带该字段 | 无 | 不聚合 |
| 带货红人数 | `kol_count` | 红人 | 曝光该商品的不同红人数 `COUNT(DISTINCT kol_id)`，只统计商品曝光事件 | 无 | 不可跨天、跨红人或跨商品求和；须回 `dws_lta_thirdparty_event_daily` 重新去重 |
| 商品曝光 PV | `goods_exposure_pv` | 事件 | `v_third_party_goods` 的 `COUNT(DISTINCT raw_event_id)` | `NET.HOST(args.current_url) = 'mycch.me'` | 可跨天求和；跨红人或跨商品会重复计数，须回 `dws_lta_thirdparty_event_daily` 重算 |
| 商品曝光 UV | `goods_exposure_uv` | 设备 | `v_third_party_goods` 的 `COUNT(DISTINCT prop_device_id)` | `NET.HOST(args.current_url) = 'mycch.me'` | 不可跨天、跨红人或跨商品求和；须回 `dws_lta_thirdparty_event_daily` 重新去重 |
| 商品点击 PV | `goods_click_pv` | 事件 | `c_third_party_goods` 的 `COUNT(DISTINCT raw_event_id)` | `NET.HOST(args.current_url) = 'mycch.me'` | 可跨天求和；跨红人或跨商品会重复计数，须回 `dws_lta_thirdparty_event_daily` 重算 |
| 商品点击 UV | `goods_click_uv` | 设备 | `c_third_party_goods` 的 `COUNT(DISTINCT prop_device_id)` | `NET.HOST(args.current_url) = 'mycch.me'` | 不可跨天、跨红人或跨商品求和；须回 `dws_lta_thirdparty_event_daily` 重新去重 |
| 商品点击转化率（PV） | `goods_click_cvr_pv` | 事件 | `SAFE_DIVIDE(goods_click_pv, goods_exposure_pv)`；分母为 0 时返回 `NULL` | 无 | 不可 `AVG`；跨天或跨维度须分别汇总点击与曝光后重算 |
| 商品点击转化率（UV） | `goods_click_cvr_uv` | 设备 | `SAFE_DIVIDE(goods_click_uv, goods_exposure_uv)`；分母为 0 时返回 `NULL` | 无 | 不可 `AVG`；跨天或跨维度须分别汇总点击与曝光后重算 |

中间层 `decom.dws_lta_thirdparty_event_daily` 按 `dt / event_name / raw_event_id / device_id / kol_id / kol_nickname / spu / spu_name` 落三事件明细：先按 `raw_event_id` 去重，再把商品曝光按 `args.spu[]` / `args.spu_name[]` 展开到商品级；页面曝光与商品点击保持事件级。总览表对缺数据日期补 0 值行。

补充说明：

- 域名过滤仅保留 `NET.HOST(args.current_url) = 'mycch.me'` 精确匹配（无 `www` 子域）；`current_url` 为空、`127.0.0.1`、`alvinclub.*` 等一律剔除
- `v_third_party_goods` / `c_third_party_goods` 在 DWD 中同一 `raw_event_id` 会被拆成多行且 `args` 完全一致，`PV = COUNT(DISTINCT raw_event_id)` 天然收敛，DWS 也先按 `raw_event_id` 去重
- `args.spu` 取值形态混杂（链接 URL 为主、少量 SP 编码），曝光与点击共用同一取值空间、可在同一 `spu` 上对齐；表3 中多数商品只落在曝光或点击一侧，是"一次 feed 曝光带大量商品、用户仅点击少数"的长尾自然现象，非口径问题
- 商品维度转化率可能 >100%：`PV = COUNT(DISTINCT raw_event_id)` 下，某商品点击事件数可能多于当天包含该商品的曝光事件数（一次曝光引发多次点击，或点击当天无对应曝光），属既定口径的自然结果，未做截顶
- 部分 `mycch.me` 事件不带 `kolUserId`（非 vibe 页），仍计入表1 总量，但不进入表2 红人维度

## 依赖

- 上游：`decom.dwd_event_log`、`v3_decom.kol_rel`
- 中间表：`decom.dws_lta_thirdparty_event_daily`
- 报表表：`decom.ads_daily_lta_overview`、`decom.ads_daily_lta_kol`、`decom.ads_daily_lta_spu`
- 脚本：`jobs/dws/dws_lta_thirdparty_event_daily.py`、`jobs/ads/ads_daily_lta_overview.py`、`jobs/ads/ads_daily_lta_kol.py`、`jobs/ads/ads_daily_lta_spu.py`
- DDL：`ddl/dws/dws_lta_thirdparty_event_daily.sql`、`ddl/ads/ads_daily_lta_overview.sql`、`ddl/ads/ads_daily_lta_kol.sql`、`ddl/ads/ads_daily_lta_spu.sql`
- 编排：`env/etl_config.json`、`etl_run.py`
