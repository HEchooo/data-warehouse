# 【Social Content】TikTok 广告投放数据 · 踩坑记录

## TikTok 后台列名与 API metric 的对应靠 URL 参数反查

后台报表 URL 的 `columns=` 参数里是内部列名（`stat_cost`、`time_attr_effect_cnt`、`time_attr_deep_convert_cnt_v2`），
和 Business API 的 metric 名完全不同。对应关系只能逐个试，不能凭命名猜：

| 后台列 | URL 列名 | API metric |
|------|------|------|
| 消耗 | `stat_cost` | `spend` |
| 成效数 | `time_attr_effect_cnt` | `result` |
| 合格转化 | `time_attr_deep_convert_cnt_v2` | `secondary_goal_result` |

「合格转化」这一组尤其容易踩坑：URL 里叫 `deep_convert`，但 API 完全不认 `deep_convert` / `total_deep_convert` /
`cost_per_deep_convert` / `deep_convert_rate` 这几个名字，报 `Invalid metric fields`，正确名字是 `secondary_goal_result` 系列。

探测方法：逐个 metric 单独发一次 `/report/integrated/get/`，看返回 `code` 是否为 0。属性指标（`campaign_name`、`ad_name` 等）
不能单独请求，会报 `All requested metrics are Attribute Metrics`，必须搭配至少一个性能指标。

## `-` 是「不适用」，不是 0

广告组没配置深度转化目标时，`secondary_goal_result` 这类指标返回字符串 `"-"` 而不是 `"0"`。
落库统一转空值，不能补 0：补 0 会让「合格转化率」在汇总时把分母算进去，得到假的 0%。

## 报表接口拿不到店铺，要另外关联 `/ad/get/`

`/report/integrated/get/` 没有任何店铺相关的 dimension 或 metric。店铺只能从 `/ad/get/` 的
`showcase_products[].store_id` 取，`filtering.ad_ids` 单次上限 100 个，要分批。

`objective_type = VIDEO_VIEWS` 的 Smart+ 广告在 campaign / adgroup / ad 三层都查不到店铺 —— 这不是接口漏返回，
而是这类广告本身不是带货广告，没有店铺概念。排查时不要往「权限不足」或「字段名不对」方向找。

## 广告主时区是 `Etc/GMT+5`，不是 `America/Toronto`

`/advertiser/info/` 同时返回 `timezone`（`Etc/GMT+5`）和 `display_timezone`（`America/Toronto`）。
报表的 `stat_time_day` 按 `timezone` 切日，是固定 UTC-5 **不含夏令时**的自然日。

拉数窗口必须用同一个时区算「今天」，否则夏令时期间会算错一天，表现为当天分区拉不到或多拉一天。

**这个时区改不了，不要再试**（`2026-08-27` 已实测）：

- 给 `/report/integrated/get/` 传 `timezone` 或 `time_zone` 参数**不报错，但也不生效**。用 `America/Toronto` / `UTC` / `Asia/Shanghai` 三种传参拉 `2026-08-25` 全量，返回的 `spend=15.00`、`impressions=351` 与不传参数完全一致 —— 参数被静默忽略。静默忽略比报错更危险：看起来「设置成功了」，实际口径没变。
- `/advertiser/update/` 返回 `40001 advertiser does not grant you /advertiser/update/:POST permission`；即便有权限，TikTok 广告账户的时区也是开户时设定、之后不可修改的。

结论：下游要对齐美东自然日只能自己在 SQL 里平移，不要指望在采集端解决。

## 率值不能存了就直接汇总

API 返回的 `cpc` / `ctr` / `conversion_rate` 都是单个广告单天的值。多行相加或求平均都是错的。
所以 `DWS` 只固化可加的分子分母，`ADS` 虽然也落派生列但只供明细行展示，
Superset 汇总 chart 一律用 `SAFE_DIVIDE(SUM(分子), SUM(分母))` 现算。

用 `2026-08-19 ~ 2026-08-26` 全量逐行比对过：重算值与 API 原始返回在 0.02 容差内 0 差异，
包括 `result_rate = 成效数 / 展示量`、`conversion_rate = 转化量 / 点击量` 这两个不那么直觉的口径。

## `etl_run.py` 不支持 `--help`

`etl_run.py` 没有 `--help` 处理分支，传任何参数都会直接开始跑整条生产 ETL 链路。
想看入口清单要读 `env/etl_config.json`，不要用 `--help` 试探：

```bash
python3 -c "import json;[print(t['table_name'], t['script']) for t in json.load(open('env/etl_config.json'))['tasks']]"
```

## 货币和时区一样，都不在报表接口里

`/report/integrated/get/` 返回的 `spend`、`cpc`、`cpm`、各类 `cost_per_*` 全是纯数字，没有任何货币标注。
货币只能从 `/advertiser/info/` 的 `currency` 取，跟着广告主走，和 `timezone` 是同一个来源、同样开户后不可改。

金额落库不带货币是真实的数据质量缺陷，不是过度设计：`spend=561.62` 这个数字脱离货币没有意义。
三层脚本都加了 `currency IS NULL` 校验兜底。

看板上不要把货币写进列名（`消耗（USD）`）。写死之后接入非 USD 广告主时列名就是错的，而且不同币种的金额会被
默默加在一起。

> 补充（2026-08-28）：当前看板最终选择的不是「`currency` 当分组维度」，而是金额列用 `d3NumberFormat = "$.2f"`
> 加顶部说明组件写明单位，`currency` 入库但不上看板。取舍与触发条件见 `fact.md` 的「货币」章节——
> 接入非 USD 广告主时必须把 `currency` 加回分组维度或统一换算，否则两种币种会被直接相加。

## 加字段后要顺带检查下游校验有没有覆盖

给三层加 `currency` 时，ODS 因为偶发 409 失败、DWS 跟着校验失败，但 **ADS 却「成功」了** ——
因为 ADS 表已经 `ALTER` 加好了空的 `currency` 列，而 ADS 的 `validate()` 当时没有货币非空检查，
一整列 `NULL` 就这么通过了。

教训：新增字段时，每一层的 `validate()` 都要同步补上对应检查，否则上游断链会在下游静默通过。

## `load_table_from_json` 会撞 409 Already Exists

网络抖动触发 SDK 内部重试时，会用同一个 job id 重发，报
`409 Already Exists: Job my-project-8584-jetonai:us-east4.<uuid>`。

传 `job_id_prefix` 让每次自带随机后缀即可规避。

## GMV Max 的数据在另一套接口里，不是「漏了识别」

现有链路用 `service_type=AUCTION` + `report_type=BASIC` + `data_level=AUCTION_AD` 查不到任何 GMV Max 数据，
但这不是筛选条件写错，而是 GMV Max 有自己的一整套接口：
`/gmv_max/store/list/` → `/gmv_max/campaign/get/` → `/gmv_max/report/get/`。

`/report/integrated/get/` 里的 `report_type=TT_SHOP` 是 GMV Max 的**旧**报表路径，官方已标 To-be-deprecated，不要用它。
被标废弃的只有 `TT_SHOP` 这一条，`BASIC + AUCTION_AD` 不受影响。

## GMV Max 的 metric 名不能照社区 SDK 抄

调研时从第三方 Go SDK（`bububa/tiktok-business`）反推出的一批字段，实测在 campaign 级全部被拒：
`product_impressions`、`product_clicks`、`product_click_rate`、`ad_click_rate`、`ad_conversion_rate`、
`refund_gmv`、`net_sales`、`refunded_ad_cost`、`gmv`、`sku_orders`、`product_name`。

同时 `spend` / `impressions` / `clicks` 这些普通投放的常用 metric 在 GMV Max 里也不存在——
消耗叫 `cost`，而且 GMV Max 报表根本没有曝光和点击。

定字段前先写一个只读探针把候选 metric 逐个打一遍，比读文档快，也比抄 SDK 准。
整批失败时逐个重试就能定位是哪个名字不被接受，接口的 `Invalid metric(s): '[xxx]'` 会直接点名。

## 和后台对账要按单个广告计划，别按店铺全量

第一次对账时按 store 全量扫了 20 个窗口（1/3/7/14/30 天 × 4 个结束日）都对不上后台卡片，
原因是后台那个页面是**单个广告计划**的「广告计划数据分析」，不是店铺汇总。
按 `filtering.campaign_ids` 限定到那一个计划、窗口取页面上显示的日期范围，`cost` / `orders` / `gross_revenue` / `roi` 立刻逐位一致。

另一个坑：该页面的折线图包含计划运行期**前后各 ≤30 天**的商品全渠道订单（图例有「无广告计划 / 其他广告计划 / 当前广告计划」），
属于对比视图；API 只返回运行期数据。所以对账只看页面顶部卡片，不要拿折线图上的点去核 API。

## `store_id` 恰好等于 seller_id，但不能因此硬编码

`/gmv_max/store/list/` 返回的 `store_id` 实测就等于 TikTok Shop 的 `seller_id`（都是 `7494736378860701060`），
但这两个 ID 在 TikTok 的体系里是不同概念，官方要求用 store list 返回值。
脚本仍然走 `/gmv_max/store/list/` 取，并且只保留 `is_gmv_max_available=true` 的店铺。

## 报表接口会返回 campaign × 天的笛卡尔积

`/gmv_max/report/get/` 对窗口内每个 campaign 的每一天都给一行，未投放的日子是 `cost=0 / orders=0 / gross_revenue=0`。
实测回填 32 天窗口：ODS 1120 行，其中只有 83 行有实际数据。

零行留在 `ODS` 里（保真），`DWS` 过滤掉。不过滤的话「在投计划数」这类 `COUNT(DISTINCT campaign_id)` 口径直接错。

## 单次窗口 30 天是硬限制

带 `stat_time_day` 时超过 30 天直接 `code=40002 Invalid 'end_date': max time span is 30 days`。
另外 `store_ids` 单次只接受一个店铺，`page_size` 上限 1000 而 30 天 campaign 级就有 1050 行，翻页必须做。

回填 41 天窗口时切成 `07-18~08-16` + `08-17~08-27` 两段加总，结果与后台完全一致，切片本身不影响口径。
