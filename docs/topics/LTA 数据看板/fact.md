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

只看 `args.current_url` 域名为 `mycch.me` 的三个第三方事件：`v_third_party_page`（页面曝光）、`v_third_party_goods`（商品曝光）、`c_third_party_goods`（商品点击）。日期按 `prop_timezone` 解释后转 `America/New_York` 切美东业务日。

- 表1：`decom.ads_daily_lta_overview`，粒度 `dt`
- 表2：`decom.ads_daily_lta_kol`，粒度 `dt × 红人`
- 表3：`decom.ads_daily_lta_spu`，粒度 `dt × 商品`
- Dashboard：[LTA 数据看板](https://bi.alvinclub.ca/superset/dashboard/125/)（id=125）
- Superset：dataset `ads_daily_lta_overview`（id=279）/ `ads_daily_lta_kol`（id=280）/ `ads_daily_lta_spu`（id=281）；chart `LTA 数据看板 - 日期总览`（id=698）/ `LTA 数据看板 - 红人维度`（id=699）/ `LTA 数据看板 - 商品维度`（id=700）；native filter 日期范围 / 红人名称 / 商品名称

通用指标口径：

- `PV = COUNT(DISTINCT raw_event_id)`
- `UV = COUNT(DISTINCT prop_device_id)`
- 商品点击转化率(PV) = 商品点击 PV / 商品曝光 PV；转化率(UV) = 商品点击 UV / 商品曝光 UV（`SAFE_DIVIDE`）

## 字段

### 表1 `ads_daily_lta_overview`（`dt`）

| 需求字段 | 定义 |
|---|---|
| 日期 | 美东业务日 |
| 页面曝光 PV / UV | `v_third_party_page` 的 PV / UV |
| 商品曝光 PV / UV | `v_third_party_goods` 的 PV / UV |
| 商品点击 PV / UV | `c_third_party_goods` 的 PV / UV |
| 商品点击转化率(PV) / (UV) | 商品点击 / 商品曝光 |

### 表2 `ads_daily_lta_kol`（`dt × 红人`）

| 需求字段 | 定义 |
|---|---|
| 红人 ID | `args.current_url` 的 `kolUserId` 参数值 |
| 红人名称 | `kolUserId` 关联 `v3_decom.kol_rel.user_id`，取 `nickname` |
| 页面曝光 / 商品曝光 / 商品点击 PV / UV | 同表1，按红人拆分 |
| 商品点击转化率(PV) / (UV) | 该红人商品点击 / 商品曝光 |

### 表3 `ads_daily_lta_spu`（`dt × 商品`）

| 需求字段 | 定义 |
|---|---|
| 商品 ID | `args.spu`；商品曝光为 feeds 数组（展开到商品级），商品点击为标量。取值可能是链接 URL 或 SP 编码，一律原样当商品 ID，不做区分 |
| 商品名称 | `args.spu_name`；feeds 数组与 `spu` 同位对应，商品点击无 `spu_name` |
| 带货红人数 | 曝光该商品的不同红人数 = `COUNT(DISTINCT kol_id)`（仅商品曝光事件）|
| 商品曝光 / 商品点击 PV / UV | 同表1，按商品拆分（无页面曝光）|
| 商品点击转化率(PV) / (UV) | 该商品点击 / 曝光 |

## 已落地字段

| 落地表 | 落地字段 | 口径 |
|---|---|---|
| `decom.dws_lta_thirdparty_event_daily` | `dt / event_name / raw_event_id / device_id / kol_id / kol_nickname / spu / spu_name` | 三事件明细中间层；商品曝光按 `args.spu[]/args.spu_name[]` 展开到商品级，页面/点击为事件级；先按 `raw_event_id` 去重再展开 |
| `decom.ads_daily_lta_overview` | 表1 全部字段 | `dt` 粒度；缺数据日期补 0 值行 |
| `decom.ads_daily_lta_kol` | 表2 全部字段 | `dt × kol_id`；仅保留 `kol_id` 非空事件 |
| `decom.ads_daily_lta_spu` | 表3 全部字段 | `dt × spu`；仅保留 `spu` 非空事件 |

补充说明：

- 域名过滤仅保留 `NET.HOST(args.current_url) = 'mycch.me'` 精确匹配（无 `www` 子域）；`current_url` 为空、`127.0.0.1`、`alvinclub.*` 等一律剔除
- `v_third_party_goods` / `c_third_party_goods` 在 DWD 中同一 `raw_event_id` 会被拆成多行且 `args` 完全一致，`PV = COUNT(DISTINCT raw_event_id)` 天然收敛，DWS 也先按 `raw_event_id` 去重
- `args.spu` 取值形态混杂（链接 URL 为主、少量 SP 编码），曝光与点击共用同一取值空间、可在同一 `spu` 上对齐；表3 中多数商品只落在曝光或点击一侧，是"一次 feed 曝光带大量商品、用户仅点击少数"的长尾自然现象，非口径问题
- 商品维度转化率可能 >100%：`PV = COUNT(DISTINCT raw_event_id)` 下，某商品点击事件数可能多于当天包含该商品的曝光事件数（一次曝光引发多次点击，或点击当天无对应曝光），属既定口径的自然结果，未做截顶
- 部分 `mycch.me` 事件不带 `kolUserId`（非 vibe 页），仍计入表1 总量，但不进入表2 红人维度
- `kol_id / device_id / raw_event_id` 去重计数跨维度、跨日期不可加，各表在自身粒度上直接聚合，不做上卷求和
- 商品点击转化率分母为 0 时 `SAFE_DIVIDE` 返回 `NULL`；跨日或跨红人、商品聚合时必须分别汇总点击与曝光后重算

## 依赖

- 上游：`decom.dwd_event_log`、`v3_decom.kol_rel`
- 中间表：`decom.dws_lta_thirdparty_event_daily`
- 报表表：`decom.ads_daily_lta_overview`、`decom.ads_daily_lta_kol`、`decom.ads_daily_lta_spu`
- 脚本：`jobs/dws/dws_lta_thirdparty_event_daily.py`、`jobs/ads/ads_daily_lta_overview.py`、`jobs/ads/ads_daily_lta_kol.py`、`jobs/ads/ads_daily_lta_spu.py`
- DDL：`ddl/dws/dws_lta_thirdparty_event_daily.sql`、`ddl/ads/ads_daily_lta_overview.sql`、`ddl/ads/ads_daily_lta_kol.sql`、`ddl/ads/ads_daily_lta_spu.sql`
- 编排：`env/etl_config.json`、`etl_run.py`
