# 【Social Content】Makepod 店铺 TK 广告投放数据

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | Makepod 店铺 TK 广告投放数据 | Social Content TikTok 广告投放数据、TikTok 投放看板 |
| 投放类型 | GMV Max | 智能高效计划 |
| 指标 | 消耗 | 广告花费、广告成本、Spend、Cost |
| 指标 | 点击率 | CTR、目标页面点击率 |
| 指标 | 转化率 | CVR |
| 指标 | ROAS | 普通投放回报、广告投产比 |
| 指标 | ROI | GMV Max 投放回报、GMV Max 投产比 |

本主题下有**两套互不相通的投放数据**，同一个 dashboard（id=133）分区展示：

| | 普通竞价投放 | GMV Max（智能高效计划） |
|------|------|------|
| 接口 | `/report/integrated/get/` | `/gmv_max/report/get/` |
| 最细层级 | 广告（`ad_id`） | 广告计划（`campaign_id`） |
| 消耗字段 | `spend` | `cost` |
| 收入口径 | 广告归因转化 | 商品全渠道归因收入（付费 + 自然 + 达人） |
| 有曝光点击 | 有 | 无 |

**两套的金额可以分别看，收入绝对不能相加**：GMV Max 的总收入把自然流量和达人订单也算进去了，
和广告归因收入不是同一个业务指标。详见下方「GMV Max 投放」章节。

## 适用范围

- 业务线：`TikTok Makepod 小店`
- 适合：查询指定 Makepod TikTok 店铺的广告消耗、成交、回报率和广告维度明细。
- 不适合：Makepod 独立站自然流量与订单应使用[Makepod 独立站日报](../Makepod 独立站日报/fact.md)；橱窗号自然内容表现应使用[Social Content 橱窗号数据](../【Social Content】橱窗号数据/fact.md)；橱窗号投流对照应使用[【Affiliate】投放数据看板](../【Affiliate】投放数据看板/fact.md)。
- 需要澄清：需要确认日期、广告、投放类型，以及金额或率值指标。

## 报表

### ads_daily_tiktok_ad

- 表：`my-project-8584-jetonai.decom.ads_daily_tiktok_ad`
- 状态：`active`
- 粒度：一行代表一个 `dt × advertiser_id × ad_id`
- 时区：`Etc/GMT+5`（固定 UTC-5）
- 数据范围：指定广告主下 Makepod 店铺的普通 TikTok 广告表现。
- 必要过滤：`store_id = '7494736378860701060'`。
- 不包含：GMV Max 自动投放和其他店铺。

- Dashboard：[【Social Content】Makepod 店铺 TK 广告投放数据](https://bi.alvinclub.ca/superset/dashboard/133/)（id=133，slug `social-content-tiktok-ad`）
- 报表表：`decom.ads_daily_tiktok_ad`，粒度 `dt × advertiser_id × ad_id`
- Superset 物理 dataset：`ads_daily_tiktok_ad`（id=297），主时间列 `dt`
- Superset chart：`723`（TikTok 广告投放日汇总，aggregate）、`724`（TikTok 广告投放明细（广告维度），raw）
- 看板顶部有 `MARKDOWN-tiktok-ad-note` 说明组件，写明时区口径、店铺过滤范围、金额单位与率值算法；改口径时要同步改这里
- 数据源：TikTok Business API `/report/integrated/get/`，`service_type=AUCTION` / `report_type=BASIC` / `data_level=AUCTION_AD`
- 广告主：`7667456906528735239`（德赢-05），当前只接这一个广告主
- 店铺过滤：两个 chart 都固定 `store_id = '7494736378860701060'`（makepod 店铺），非该店铺与无店铺的广告不进看板
- 指标口径与 TikTok 后台 AUCTION_AD 报表逐列对齐，已用 `2026-08-19 ~ 2026-08-26` 全量逐行比对，重算率值与 API 原始返回 0 差异

### ads_daily_tiktok_gmv_max

- 表：`my-project-8584-jetonai.decom.ads_daily_tiktok_gmv_max`
- 状态：`active`
- 粒度：一行代表一个 `dt × advertiser_id × store_id × campaign_id`
- 时区：`Etc/GMT+5`（固定 UTC-5）
- 数据范围：Makepod 店铺 GMV Max 自动投放的计划日表现。
- 必要过滤：无；接口按可用店铺逐个查询。
- 不包含：普通广告层级数据；GMV Max 与普通投放不可相加。
- Dashboard：与普通投放同一个（id=133），GMV Max 部分在下半区
- 报表表：`decom.ads_daily_tiktok_gmv_max`，粒度 `dt × advertiser_id × store_id × campaign_id`
- Superset 物理 dataset：`ads_daily_tiktok_gmv_max`（id=298），主时间列 `dt`
- Superset chart：`725`（TikTok GMV Max 投放日汇总，aggregate）、`726`（TikTok GMV Max 投放明细（计划维度），raw）
- 说明文案与普通投放共用看板顶部的 `MARKDOWN-tiktok-ad-note`，其中写明了两套数据不能相加、总收入的全渠道归因口径与 ROI 是倍数；改口径时要同步改这里，不要再单独加说明组件
- 数据源：TikTok Business API `/gmv_max/report/get/`，`dimensions=["campaign_id","stat_time_day"]`
- 店铺：`/gmv_max/store/list/` 返回的 `store_id`，当前只有 `7494736378860701060`（MAKEPOD，`is_gmv_max_available=true`）
- 不加店铺过滤：GMV Max 报表本身按店铺逐个查，表里只有可投 GMV Max 的店铺

## 字段

### ads_daily_tiktok_ad

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 统计日 | `dt` | 自然日 | API `stat_time_day`，按广告主时区切日 | 无 | 不聚合 |
| 广告主 | `advertiser_id` | 广告主 | 广告主 ID | 无 | 不聚合 |
| 广告计划 | `campaign_id` / `campaign_name` | 广告计划 | 计划层级标识 | 按目标计划过滤 | 不聚合 |
| 广告组 | `adgroup_id` / `adgroup_name` | 广告组 | 组层级标识 | 按目标组过滤 | 不聚合 |
| 广告 | `ad_id` / `ad_name` | 广告 | 广告层级标识 | 按目标广告过滤 | 不聚合 |
| 投放店铺 | `store_id` | 店铺 | 取 `/ad/get/` 的 `showcase_products[].store_id`；报表接口不返回该字段。单条广告理论上可横跨多店铺，真出现时 ODS 取字典序最小的 `store_id` 并打 warning | 按目标店铺过滤 | 不聚合 |
| 货币 | `currency` | 广告主 | 取 `/advertiser/info/` 的 `currency`，报表接口不返回；当前恒为 `USD`。入库但不上看板，既不作展示列也不作分组维度 | 无 | 不聚合 |
| 消耗 | `spend` | 广告 | API `spend` 直接取，以广告主账户货币计价 | 无 | 可加；跨天或跨维度求和前先按目标范围过滤 |
| 展示量 | `impressions` | 广告 | API `impressions` 直接取 | 无 | 可加；跨天或跨维度求和前先按目标范围过滤 |
| 点击量（目标页面） | `clicks` | 广告 | API `clicks` 直接取 | 无 | 可加；跨天或跨维度求和前先按目标范围过滤 |
| 转化量 | `conversion` | 广告 | API `conversion` 直接取 | 无 | 可加；跨天或跨维度求和前先按目标范围过滤 |
| 成效数 | `result` | 广告 | API `result` 直接取 | 无 | 可加；跨天或跨维度求和前先按目标范围过滤 |
| 合格转化 | `secondary_goal_result` | 广告 | 深度转化目标；广告组未配置时为空值 | 无 | 可加；跨天或跨维度求和前先按目标范围过滤 |
| 平均点击成本 | `cpc` | 广告 | `spend / clicks` | 无 | 不可求和或平均；汇总须用 `SAFE_DIVIDE(SUM(分子), SUM(分母))` 现算 |
| 千次展示成本 | `cpm` | 广告 | `spend / impressions × 1000` | 无 | 不可求和或平均；汇总须用 `SAFE_DIVIDE(SUM(分子), SUM(分母))` 现算 |
| 点击率（目标页面） | `ctr` | 广告 | `clicks / impressions`，单位 % | 无 | 不可求和或平均；汇总须用 `SAFE_DIVIDE(SUM(分子), SUM(分母))` 现算 |
| 平均转化成本 | `cost_per_conversion` | 广告 | `spend / conversion` | 无 | 不可求和或平均；汇总须用 `SAFE_DIVIDE(SUM(分子), SUM(分母))` 现算 |
| 转化率（CVR） | `conversion_rate` | 广告 | `conversion / clicks`，单位 % | 无 | 不可求和或平均；汇总须用 `SAFE_DIVIDE(SUM(分子), SUM(分母))` 现算 |
| 平均成效成本 | `cost_per_result` | 广告 | `spend / result` | 无 | 不可求和或平均；汇总须用 `SAFE_DIVIDE(SUM(分子), SUM(分母))` 现算 |
| 成效率 | `result_rate` | 广告 | `result / impressions`，单位 % | 无 | 不可求和或平均；汇总须用 `SAFE_DIVIDE(SUM(分子), SUM(分母))` 现算 |
| 合格转化平均成本 | `cost_per_secondary_goal_result` | 广告 | `spend / secondary_goal_result` | 无 | 不可求和或平均；汇总须用 `SAFE_DIVIDE(SUM(分子), SUM(分母))` 现算 |
| 合格转化率 | `secondary_goal_result_rate` | 广告 | `secondary_goal_result / clicks`，单位 % | 无 | 不可求和或平均；汇总须用 `SAFE_DIVIDE(SUM(分子), SUM(分母))` 现算 |

`ADS` 表同时落可加的分子分母（`spend` / `impressions` / `clicks` / `conversion` / `result` / `secondary_goal_result`）与单行派生率值：明细 chart（724）直接展示派生列，只用于逐行看单个广告；汇总 chart（723）的所有率值都是 `SAFE_DIVIDE(SUM(分子), SUM(分母))` 现算。**新增任何汇总 chart 时不得对派生列求和或求平均**，否则 CPC / CTR / CVR 等会算错。

#### 货币

`spend` 与所有成本类字段都以广告主账户货币计价，货币本身不在报表接口里，来自 `/advertiser/info/` 的 `currency`。

`currency` 入库但**不上看板**：既不作为展示列，也不作为分组维度。看板改用两种方式表达货币：

- 金额列的 `d3NumberFormat` 统一为 `$.2f`，数值自带 `$` 前缀
- 顶部说明组件写明「金额单位为 USD」

三层脚本都有 `currency IS NULL` 的校验：金额有值但没有货币标注，直接判为失败。

**这里有一个明确的取舍：** 去掉货币分组维度后，如果将来接入非 USD 广告主，两种币种的金额会被直接相加而不会自动分行，
`$` 前缀也会变成错的。届时必须做两件事之一：把 `currency` 加回分组维度，或用 `decom.currency_conversion_dict` 统一换算到单一币种。
当前看板固定单广告主单店铺、`currency` 恒为 `USD`，所以这个风险尚未触发，但不能当成永久成立的前提。

#### 数据边界

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

### ads_daily_tiktok_gmv_max

TikTok GMV Max（后台叫「智能高效计划」，页面 `ads.tiktok.com/i18n/gmv-max/dashboard`）是自动化带货投放：
平台自己选素材、自己创建和暂停底层广告，所以对外没有广告组 / 广告层级。

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 统计日 | `dt` | 自然日 | API `stat_time_day`，按广告主时区切日 | 无 | 不聚合 |
| 广告主与店铺 | `advertiser_id` / `store_id` | 广告主 / 店铺 | 与普通投放同源 | 按目标店铺过滤 | 不聚合 |
| 广告计划 | `campaign_id` / `campaign_name` | 广告计划 | GMV Max 由平台自动创建和暂停底层广告，对外没有广告组 / 广告层级 | 按目标计划过滤 | 不聚合 |
| 货币 | `currency` | 广告主 | GMV Max 报表接口直接返回，不需要另查 `/advertiser/info/`；入库但不上看板 | 无 | 不聚合 |
| 成本 | `cost` | 广告计划 | API `cost` 直接取（注意不叫 `spend`） | 无 | 可加；跨天或跨维度求和前先按目标范围过滤 |
| SKU 订单数 | `orders` | 订单 | API `orders` 直接取 | 无 | 可加；跨天或跨维度求和前先按目标范围过滤 |
| 总收入 | `gross_revenue` | 订单 | **商品全渠道归因**：只要商品在 GMV Max 计划里，来自该商品的订单都会计入，含付费广告、自然流量与达人订单，用户完全没看过广告也可能被算进来；等于客户支付 − 销售税 + 平台折扣，**不扣退款** | 无 | 可加；跨天或跨维度求和前先按目标范围过滤；与普通投放的 ROAS 不是同一指标，两者收入不可相加 |
| 平均下单成本 | `cost_per_order` | 订单 | `cost / orders` | 无 | 不可求和或平均；汇总须用 `SAFE_DIVIDE(SUM(分子), SUM(分母))` 现算 |
| ROI | `roi` | 广告计划 | `gross_revenue / cost`，**是倍数不是百分比** | 无 | 不可求和或平均；汇总须用 `SAFE_DIVIDE(SUM(分子), SUM(分母))` 现算 |

#### 接口支持但故意不落库的字段

看板只要上面五个指标，以下字段接口给得出来，需要时再加回三层：

| 字段 | 说明 |
|------|------|
| `net_cost` | 调整后成本，**仅 campaign 级可用**，商品级 / 素材级报表不支持。与 `cost` 实测差别不小（`2026-08-27` 是 63.91 vs 28.79），TikTok 未公开差额构成，不适合直接上看板 |
| `roas_bid` | 目标 ROI，**计划当前配置快照**，每天返回同一个值，跨天聚合只能取 `MAX` / `ANY_VALUE`，求和一定错 |
| `target_roi_budget` | 广告计划预算，同样是当前配置快照 |
| `operation_status` | 计划当前启停状态，不是当天的历史状态 |

#### 实测不可用的 metric

以下名字在 campaign 级都被接口拒绝（`Invalid metric(s)`），**不要再加进 `REPORT_METRICS`**：

- 曝光点击类：`impressions`、`clicks`、`spend`、`product_impressions`、`product_clicks`、`product_click_rate`、`ad_click_rate`、`ad_conversion_rate`
- 退款类：`refund_gmv`、`net_sales`、`refunded_ad_cost`
- 其他：`gmv`、`sku_orders`、`cost_per_sku_order`、`product_name`、`item_group_id`、`objective_type`、`gmv_max_promotion_type`、`budget`、`create_time`、`campaign_status`、`secondary_status`

可用但当前无值、故意不落库的：`bid_type`（恒为 `CUSTOM`）、`max_delivery_budget`（恒为 0）、
`schedule_type` / `schedule_start_time` / `schedule_end_time` / `tt_account_name`（返回空串或 `0`）、`live_views`（无直播计划）。

`ADS` 表同时落可加的分子分母（`cost` / `orders` / `gross_revenue`）与单行派生值：明细 chart（726）直接展示派生列；汇总 chart（725）的 `平均下单成本` 与 `ROI` 都是 `SAFE_DIVIDE(SUM(分子), SUM(分母))` 现算。**新增任何汇总 chart 时不得对 `cost_per_order` / `roi` 求和或求平均**。

#### 货币

`cost`、`gross_revenue`、`cost_per_order` 都以广告主账户货币计价。与普通投放不同，GMV Max 报表接口
直接返回 `currency`，不需要额外查 `/advertiser/info/`。

三层脚本都有 `currency IS NULL` 的致命校验：金额有值但没有货币标注，直接判为失败。

`currency` 入库但**不上看板**，与普通投放一致：既不作为展示列，也不作为分组维度。货币改用两种方式表达：

- 金额列的 `d3NumberFormat` 统一为 `$.2f`，数值自带 `$` 前缀（`ROI` 是倍数用 `.2f`，`SKU 订单数` 用 `,d`）
- 顶部 `MARKDOWN-tiktok-ad-note` 说明组件写明金额单位

改 chart 格式时注意 `column_config` 的值必须是 `{"d3NumberFormat": "$.2f"}` 这样的对象，
写成字符串 `"$.2f"` 不会报错，但格式静默失效，看板上就是裸数字。`params` 与 `query_context.form_data`
两处都要写。

**这里有一个明确的取舍：** 与普通投放同源——当前固定单广告主单店铺、`currency` 恒为 `USD`，所以直接用 `$` 前缀成立。
一旦接入非 USD 广告主，两种币种的金额会被直接相加而不会自动分行，`$` 前缀也会变成错的。届时必须做两件事之一：
把 `currency` 加回分组维度，或用 `decom.currency_conversion_dict` 统一换算到单一币种。

#### 数据边界

| 边界 | 说明 |
|------|------|
| 总收入归因 | **商品全渠道归因**：只要商品在 GMV Max 计划里，来自该商品的订单都会计入，包含付费广告、自然流量与达人订单；用户完全没看过广告也可能被算进来。所以 GMV Max 的 ROI 与普通投放的 ROAS 不是同一指标，收入不能相加 |
| 退款 | `gross_revenue` = 客户支付 − 销售税 + 平台折扣，**不扣退款**。退款后净额 API 拿不到（`refund_gmv` / `net_sales` / `refunded_ad_cost` 全部被拒），要就得另接 TikTok Shop 订单 / 财务接口 |
| 查询窗口 | 带 `stat_time_day` 时**单次最多 30 天**，超出直接 `code=40002`。ODS 内部按 30 天切片，回填 41 天窗口会自动切成两段 |
| 翻页 | `page_size` 上限 1000，30 天 campaign 级就有 1050 行，必须翻页 |
| store 参数 | `store_ids` 单次只接受一个店铺，多店铺要逐个循环 |
| 全零填充行 | 报表接口按 campaign × 天返回笛卡尔积，计划未投放的日子也给一行 `cost=0 / orders=0`。`ODS` 原样保留（保真），`DWS` 过滤掉。实测回填 32 天：ODS 1120 行 → DWS 83 行 |
| 报表起点 | `2026-07-28`，与普通投放账户 `create_time` 同一天 |
| 时区 | `dt` 是广告主时区 `Etc/GMT+5`（固定 UTC-5，不含夏令时）自然日，与 `ods_tiktok_ad_daily` 同源。后台页面显示的「(UTC-05:00) 北美东部标准时间」与此一致 |
| 归因回溯 | 订单与收入会在归因窗口内继续回填，计划停投后当天的 `gross_revenue` 仍可能变化。ODS 默认重刷 `CURRENT_DATE-6 ~ CURRENT_DATE`（7 天），比普通投放的 4 天窗口更长 |
| 无广告层 | `adgroup_id` / `ad_id` 作为 dimension 会被拒绝（`Invalid dimension(s)`）。商品级是 `item_group_id`，素材级是 `item_id`（TikTok Post ID，**不是 ad_id**），当前都没落库 |
| 直播计划 | `LIVE_GMV_MAX` 类型的计划数为 0，当前只有 `PRODUCT_GMV_MAX`。有直播计划时 `room_id` 维度需要额外传 `campaign_ids` |
| 后台对账口径 | 后台「广告计划数据分析」页的折线图包含计划运行期**前后各 ≤30 天**的商品全渠道订单，属于对比视图，不是该计划的归因数据；API 只返回运行期数据。对账要看页面顶部卡片，不要看折线图 |

## 依赖

### ads_daily_tiktok_ad

- 原始层：`decom.ods_tiktok_ad_daily`（TikTok Business API 直连）
- 事实层：`decom.dws_tiktok_ad_daily`（只固化可加分子分母，率值不落库）
- 报表层：`decom.ads_daily_tiktok_ad`
- 脚本：`jobs/ods/ods_tiktok_ad_daily/ods_tiktok_ad_daily.py`, `jobs/dws/dws_tiktok_ad_daily.py`, `jobs/ads/ads_daily_tiktok_ad.py`
- DDL：`ddl/ods/ods_tiktok_ad_daily.sql`, `ddl/dws/dws_tiktok_ad_daily.sql`, `ddl/ads/ads_daily_tiktok_ad.sql`
- 编排：`env/etl_config.json` 中的独立表级入口 `ods_tiktok_ad_daily`、`dws_tiktok_ad_daily`、`ads_daily_tiktok_ad`
- 凭据：Secret Manager `prod-ads-runner-tiktok-access-token`，脚本运行时读取，不落仓库
- 性能：单广告主一年只有两万行量级，`DWS` / `ADS` 走全表重建，没有水位判定，也就没有漏刷分区的路径；`ODS` 只替换 4 天窗口分区

### ads_daily_tiktok_gmv_max

- 原始层：`decom.ods_tiktok_gmv_max_daily`（TikTok Business API 直连）
- 事实层：`decom.dws_tiktok_gmv_max_daily`（只固化可加分子分母，派生值不落库；同时过滤全零填充行）
- 报表层：`decom.ads_daily_tiktok_gmv_max`
- 脚本：`jobs/ods/ods_tiktok_gmv_max_daily/ods_tiktok_gmv_max_daily.py`, `jobs/dws/dws_tiktok_gmv_max_daily.py`, `jobs/ads/ads_daily_tiktok_gmv_max.py`
- DDL：`ddl/ods/ods_tiktok_gmv_max_daily.sql`, `ddl/dws/dws_tiktok_gmv_max_daily.sql`, `ddl/ads/ads_daily_tiktok_gmv_max.sql`
- 编排：`env/etl_config.json` 中的独立表级入口 `ods_tiktok_gmv_max_daily`、`dws_tiktok_gmv_max_daily`、`ads_daily_tiktok_gmv_max`
- 凭据：与普通投放共用 Secret Manager `prod-ads-runner-tiktok-access-token`，无需额外授权（`Reporting > GMV MAX Report` scope 实测已具备）
- 省略 `DWD`：源数据已是平台聚合好的日报，无去重与多源关联需求，类型清洗在 `ODS` 落库时完成
- 省略 `DIM`：店铺与计划映射只有本主题使用
- 性能：单店铺一年万行量级，`DWS` / `ADS` 走全表重建；`ODS` 只替换窗口分区

## 运行

### ads_daily_tiktok_ad

```bash
# 默认回刷 CURRENT_DATE-3 ~ CURRENT_DATE（广告主时区）
python3 jobs/ods/ods_tiktok_ad_daily/ods_tiktok_ad_daily.py
# 回填历史窗口
python3 jobs/ods/ods_tiktok_ad_daily/ods_tiktok_ad_daily.py --start-date 2026-08-01 --end-date 2026-08-22
python3 jobs/dws/dws_tiktok_ad_daily.py
python3 jobs/ads/ads_daily_tiktok_ad.py
```

### ads_daily_tiktok_gmv_max

```bash
# 默认回刷 CURRENT_DATE-6 ~ CURRENT_DATE（广告主时区，7 天窗口）
python3 jobs/ods/ods_tiktok_gmv_max_daily/ods_tiktok_gmv_max_daily.py
# 回填历史窗口，超过 30 天会自动切片
python3 jobs/ods/ods_tiktok_gmv_max_daily/ods_tiktok_gmv_max_daily.py --start-date 2026-07-28 --end-date 2026-08-28
python3 jobs/dws/dws_tiktok_gmv_max_daily.py
python3 jobs/ads/ads_daily_tiktok_gmv_max.py
```

## 未落地字段

第一版只做 campaign 级。以下需要时再加：

| 粒度 | dimensions | 额外要求 |
|------|------|------|
| 商品级 | `item_group_id` + `stat_time_day` | 必须传 `filtering.campaign_ids`，且不能带 `net_cost` |
| 素材级 | `item_id` + `stat_time_day` | 必须同时传 `filtering.campaign_ids` 与 `item_group_ids`；`item_id` 会出现 `-1`（无归属素材的消耗） |
| 小时级 | `stat_time_hour` | 单次窗口最多 1 天 |

计划配置（Max Delivery / Creative Boost 时段）在 `/campaign/gmv_max/info/` 与 `/campaign/gmv_max/session/*`，当前未接。
