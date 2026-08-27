# 【Social Content】TikTok 广告投放数据

## 报表

- Dashboard：[【Social Content】TikTok 广告投放数据](https://bi.alvinclub.ca/superset/dashboard/133/)（id=133，slug `social-content-tiktok-ad`）
- 报表表：`decom.ads_daily_tiktok_ad`，粒度 `dt × advertiser_id × ad_id`
- Superset 物理 dataset：`ads_daily_tiktok_ad`（id=297），主时间列 `dt`
- Superset chart：`723`（TikTok 广告投放日汇总，aggregate）、`724`（TikTok 广告投放明细（广告维度），raw）
- 数据源：TikTok Business API `/report/integrated/get/`，`service_type=AUCTION` / `report_type=BASIC` / `data_level=AUCTION_AD`
- 广告主：`7667456906528735239`（德赢-05），当前只接这一个广告主
- 店铺过滤：两个 chart 都固定 `store_id = '7494736378860701060'`（makepod 店铺），非该店铺与无店铺的广告不进看板
- 指标口径与 TikTok 后台 AUCTION_AD 报表逐列对齐，已用 `2026-08-19 ~ 2026-08-26` 全量逐行比对，重算率值与 API 原始返回 0 差异

## 字段

### 需求字段

| 需求字段 | 落地字段 | API metric | 口径 |
|------|------|------|------|
| 消耗 | `spend` | `spend` | 直接取 |
| 平均点击成本（目标页面） | `cpc` | `cpc` | `消耗 / 点击量` |
| 千次展示成本（CPM） | `cpm` | `cpm` | `消耗 / 展示量 × 1000` |
| 展示量 | `impressions` | `impressions` | 直接取 |
| 点击量（目标页面） | `clicks` | `clicks` | 直接取 |
| 点击率（目标页面） | `ctr` | `ctr` | `点击量 / 展示量`，单位 % |
| 转化量 | `conversion` | `conversion` | 直接取 |
| 平均转化成本 | `cost_per_conversion` | `cost_per_conversion` | `消耗 / 转化量` |
| 转化率（CVR） | `conversion_rate` | `conversion_rate` | `转化量 / 点击量`，单位 % |
| 成效数 | `result` | `result` | 直接取 |
| 平均成效成本 | `cost_per_result` | `cost_per_result` | `消耗 / 成效数` |
| 成效率 | `result_rate` | `result_rate` | `成效数 / 展示量`，单位 % |
| 合格转化 | `secondary_goal_result` | `secondary_goal_result` | 深度转化目标；广告组未配置时为空值 |
| 合格转化平均成本 | `cost_per_secondary_goal_result` | `cost_per_secondary_goal_result` | `消耗 / 合格转化` |
| 合格转化率 | `secondary_goal_result_rate` | `secondary_goal_result_rate` | `合格转化 / 点击量`，单位 % |

### 维度字段

| 字段 | 口径 |
|------|------|
| `dt` | 统计日，取 API `stat_time_day`，按广告主时区切日 |
| `advertiser_id` | 广告主 ID |
| `campaign_id` / `campaign_name` | 广告计划 |
| `adgroup_id` / `adgroup_name` | 广告组 |
| `ad_id` / `ad_name` | 广告 |
| `store_id` | 投放店铺，取 `/ad/get/` 的 `showcase_products[].store_id`；报表接口不返回这个字段 |
| `currency` | 货币单位，取 `/advertiser/info/` 的 `currency`；报表接口同样不返回。当前恒为 `USD` |

### 可加与不可加

`ADS` 表同时落可加的分子分母（`spend` / `impressions` / `clicks` / `conversion` / `result` / `secondary_goal_result`）与单行派生率值。

- 明细 chart（724）直接展示派生列，只用于逐行看单个广告
- 汇总 chart（723）的所有率值都是 `SAFE_DIVIDE(SUM(分子), SUM(分母))` 现算
- **新增任何汇总 chart 时不得对派生列求和或求平均**，否则 CPC / CTR / CVR 等会算错

### 货币

`spend` 与所有成本类字段都以广告主账户货币计价，货币本身不在报表接口里，来自 `/advertiser/info/` 的 `currency`。

看板不把货币写死进列名，而是把 `currency` 作为**分组维度**：汇总 chart 按 `dt × currency` 分组，明细 chart 单独出一列「货币」。
这样将来接入非 USD 广告主时会自动分行，不会把不同币种的金额悄悄加在一起。跨广告主汇总金额前必须先按 `currency` 分组，
或用 `decom.currency_conversion_dict` 换算。

三层脚本都有 `currency IS NULL` 的校验：金额有值但没有货币标注，直接判为失败。

### 数据边界

| 边界 | 说明 |
|------|------|
| 报表起点 | `2026-08-07`，即广告主首次产生投放数据的日期。账户 `create_time` 是 `2026-07-28`，`2026-07-25 ~ 2026-08-06` 已回填确认全部无数据 |
| 时区 | `dt` 是广告主时区 `Etc/GMT+5`（固定 UTC-5，**不含夏令时**）自然日，取自 `/advertiser/info/` 的 `timezone`。项目其他表用 `America/Toronto`，夏令时期间两者相差 1 小时，跨主题对齐要按这个差异处理。**这个时区改不了**：报表接口的 `timezone` 传参会被静默忽略，广告账户时区开户后也不可修改，详见 `patterns.md` |
| 当天数据 | 默认窗口含当天，当天分区数据尚未收口，每轮调度都会重刷 |
| 归因回溯 | TikTok 会在归因窗口内修正历史数据，ODS 默认每次重刷 `CURRENT_DATE-3 ~ CURRENT_DATE` 覆盖 |
| 合格转化 | `2026-08-27` 核对时全部广告组都没配置深度转化目标，API 与 TikTok 后台该三列均为空；一旦有广告组配置了深度转化目标，字段会自动有值，不需要改代码 |
| 无店铺广告 | `objective_type = VIDEO_VIEWS` 的 Smart+ 广告不是带货广告，campaign / adgroup / ad 三层都没有店铺概念，`store_id` 为空。这类广告的展示量与成效数占比很高（`2026-08-19 ~ 08-26` 窗口内 10/57 个广告，贡献了绝大部分成效数），被店铺过滤整体排除，看板上的成效数会明显低于 TikTok 后台不过滤时的口径 |
| 其他店铺 | 同一广告主下还存在 `8649419153924916852`、`8650948738904786782` 两个店铺的少量广告，同样被过滤 |
| 货币 | 当前唯一广告主账户货币为 `USD`，全部 279 行均为 `USD`。TikTok 广告账户货币与时区一样，开户后不可修改 |
| 多店铺广告 | 单条广告的 `showcase_products` 理论上可以横跨多个店铺，当前实测没有出现；真出现时 ODS 取字典序最小的 `store_id` 并打 warning |

## 依赖

- 原始层：`decom.ods_tiktok_ad_daily`（TikTok Business API 直连）
- 事实层：`decom.dws_tiktok_ad_daily`（只固化可加分子分母，率值不落库）
- 报表层：`decom.ads_daily_tiktok_ad`
- 脚本：`jobs/ods/ods_tiktok_ad_daily/ods_tiktok_ad_daily.py`, `jobs/dws/dws_tiktok_ad_daily.py`, `jobs/ads/ads_daily_tiktok_ad.py`
- DDL：`ddl/ods/ods_tiktok_ad_daily.sql`, `ddl/dws/dws_tiktok_ad_daily.sql`, `ddl/ads/ads_daily_tiktok_ad.sql`
- 编排：`env/etl_config.json` 中的独立表级入口 `ods_tiktok_ad_daily`、`dws_tiktok_ad_daily`、`ads_daily_tiktok_ad`
- 凭据：Secret Manager `prod-ads-runner-tiktok-access-token`，脚本运行时读取，不落仓库
- 性能：单广告主一年只有两万行量级，`DWS` / `ADS` 走全表重建，没有水位判定，也就没有漏刷分区的路径；`ODS` 只替换 4 天窗口分区

## 运行

```bash
# 默认回刷 CURRENT_DATE-3 ~ CURRENT_DATE（广告主时区）
python3 jobs/ods/ods_tiktok_ad_daily/ods_tiktok_ad_daily.py
# 回填历史窗口
python3 jobs/ods/ods_tiktok_ad_daily/ods_tiktok_ad_daily.py --start-date 2026-08-01 --end-date 2026-08-22
python3 jobs/dws/dws_tiktok_ad_daily.py
python3 jobs/ads/ads_daily_tiktok_ad.py
```
