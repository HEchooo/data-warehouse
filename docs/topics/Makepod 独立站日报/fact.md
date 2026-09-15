# Makepod 独立站日报

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | Makepod 独立站日报 | Makepod 站、Makepod 日报、独立站日报、Makepod 平台看板、AI-Fashion Makepod 独立站日报 |
| 指标 | 全量 DAU | 日活、日活设备数、活跃设备数 |
| 指标 | 新用户 DAU | 新用户日活、新设备日活 |
| 指标 | 老用户 DAU | 老用户日活、老设备日活 |
| 指标 | 支付用户数 | 付费人数、付费用户、购买用户 |
| 指标 | 支付订单数 | 付费订单数、成交订单数 |
| 指标 | 支付 GMV | 支付金额、GMV、成交额、销售额 |

## 适用范围

- 业务线：`Makepod 独立站`
- 适合：查询 Makepod 独立站按天、渠道统计的流量与交易，或按自然周、渠道、订单分组统计的支付表现。
- 不适合：Alvin's Club App 的活跃与留存应使用[全量报表](../全量报表/fact.md)或[用增报表](../用增报表/fact.md)；Makepod 原始事件字段应使用[Makepod 事件明细](../Makepod 事件明细/fact.md)。
- 需要澄清：问题未说明日报或周报、渠道、是否取全站 `Overall`、日期范围或订单分组时，需要先确认。

## 报表

### ads_daily_makepod

- 表：`my-project-8584-jetonai.decom.ads_daily_makepod`
- 状态：`active`
- 粒度：一行代表一个 `dt × channel`
- 时区：`Asia/Shanghai`
- 数据范围：Makepod 独立站流量与交易的日报汇总。
- 必要过滤：查询全站时使用 `channel = 'Overall'`。
- 不包含：App 业务；周内支付用户去重和订单分组。
- Dashboard：[【Makepod】平台看板](https://bi.alvinclub.ca/superset/dashboard/115/)（id=115）

### ads_weekly_makepod_paid_user_order

- 表：`my-project-8584-jetonai.decom.ads_weekly_makepod_paid_user_order`
- 状态：`active`
- 粒度：一行代表一个周一开始的自然周 `week_start_dt × channel × order_group`
- 时区：`Asia/Shanghai`
- 数据范围：Makepod 独立站周维度支付用户、订单与 GMV 分布。
- 必要过滤：查询全站总计时使用 `channel = 'Overall' AND order_group = '全部分组'`。
- 不包含：流量指标；当前进行中自然周不保证周期完整。
- Dashboard：[【Makepod】平台看板](https://bi.alvinclub.ca/superset/dashboard/115/)（id=115）

## 字段

### ads_daily_makepod

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 渠道 | `channel` | 设备 / 用户 | 固定枚举 `TikTok / Google / Facebook / TK-ELSA-AI / YOUTUBE-ELSA-AI / Reddit / Other Channel / Organic / Overall`；映射见 `docs/rules/makepod.md` | 无 | `Overall` 与渠道明细不可一起聚合 |
| 全量 DAU | `dau_uv_total` | 设备 | 渠道行按 `channel × dt` 去重 `prop_device_id`；`Overall` 按全站去重 | 全站取 `Overall` | 不可跨天或跨渠道直接求和 |
| 新用户 DAU | `dau_new_uv` | 设备 | `first_visit_date = dt` 的去重设备；`Overall` 按全站去重 | 全站取 `Overall` | 不可跨天或跨渠道直接求和 |
| 老用户 DAU | `dau_old_uv` | 设备 | `first_visit_date < dt` 的去重设备；`Overall` 按全站去重 | 全站取 `Overall` | 不可跨天或跨渠道直接求和 |
| 注册人数 | `register_user_count` | 用户 | 按 `register_date × channel` 去重注册用户；`Overall` 按注册日全站去重 | 全站取 `Overall` | 不可跨天或跨渠道直接求和 |
| 商详页访问人数 | `product_detail_view_uv` | 设备 | 渠道行按 `channel × dt` 去重 `prop_device_id`；`Overall` 按全站去重 | 全站取 `Overall` | 不可跨天或跨渠道直接求和 |
| 加购人数 | `add_cart_uv` | 设备 | 渠道行按 `channel × dt` 去重 `prop_device_id`；`Overall` 按全站去重 | 全站取 `Overall` | 不可跨天或跨渠道直接求和 |
| 下单人数 | `order_user_count` | 用户 | 按 `dt × channel` 去重 `user_id`；未命中渠道维表回退 `Organic`；`Overall` 按全站去重 | 全站取 `Overall` | 不可跨天或跨渠道直接求和 |
| 支付人数 | `pay_user_count` | 用户 | 按 `dt × channel` 去重 `user_id`；未命中渠道维表回退 `Organic`；`Overall` 按全站去重 | `pay_status = 4`；全站取 `Overall` | 不可跨天或跨渠道直接求和 |
| 支付订单数 | `pay_order_count` | 订单 | 按 `dt × channel` 去重 `order_id`；未命中渠道维表回退 `Organic`；`Overall` 按全站去重 | `pay_status = 4`；全站取 `Overall` | 可跨天求和；不可混合 `Overall` 与渠道明细 |
| 支付 GMV | `pay_gmv_usd` | 订单 | `SUM((pay_amount + pay_fee) * usd_rate)`；`pay_fee` 保留负数，汇率来自 `currency_conversion_dict` | `pay_status = 4`；全站取 `Overall` | 可跨天求和；不可混合 `Overall` 与渠道明细 |
| 支付手续费 | `pay_fee_amount` | 订单 | `SUM(pay_fee * usd_rate)`；USD 汇率为 1，其他币种按汇率表折算并保留负号 | `pay_status = 4`；全站取 `Overall` | 可跨天求和；不可混合 `Overall` 与渠道明细 |
| 支付 SKU 件数 | `pay_sku_quantity` | SKU | 支付订单关联订单明细后 `SUM(sku_quantity)` | `pay_status = 4`；全站取 `Overall` | 可跨天求和；不可混合 `Overall` 与渠道明细 |
| 采购成本 | `purchase_cost_usd` | SKU | `purchase_price × sku_quantity`，按采购币种汇率转换为 USD | `pay_status = 4`；全站取 `Overall` | 可跨天求和；不可混合 `Overall` 与渠道明细 |
| 物流成本 | `logistics_cost_usd` | 订单 | 去重支付订单数 × `5 USD` | `pay_status = 4`；全站取 `Overall` | 可跨天求和；不可混合 `Overall` 与渠道明细 |
| 毛利 | `gross_profit_usd` | 订单 | `pay_gmv_usd - purchase_cost_usd - logistics_cost_usd` | 全站取 `Overall` | 可跨天求和；不可混合 `Overall` 与渠道明细 |
| 毛利率 | `gross_profit_rate` | 订单 | `gross_profit_usd / pay_gmv_usd`；支付 GMV 为 0 时返回 `NULL` | 全站取 `Overall` | 不可求和或平均；跨日须用汇总后的毛利和 GMV 重算 |

### ads_weekly_makepod_paid_user_order

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 周范围 | `week_range` | 自然周 | 周一至周日，展示为 `YYYY-MM-DD ~ YYYY-MM-DD` | 无 | 不聚合 |
| 渠道 | `channel` | 用户 | 与日报相同，含全站去重的 `Overall` | 全站取 `Overall` | `Overall` 与渠道明细不可一起聚合 |
| 订单分组 | `order_group` | 用户 | 按用户周内支付成功订单数分为 `1单 / 2单 / 3单及以上`，另有 `全部分组` | 总计取 `全部分组` | `全部分组` 与分桶明细不可一起聚合 |
| 支付用户数 | `pay_user_count` | 用户 | 周窗口内去重 `user_id` | `pay_status = 4` | 不可由日报相加，也不可跨周直接求和 |
| 支付用户数周环比 | `pay_user_count_wow` | 用户 | `(本周 - 上周) / 上周`；首周或上周为 0 时为 `NULL` | 同一 `channel + order_group` | 不可求和或平均 |
| 支付订单数 | `pay_order_count` | 订单 | 周内去重 `order_id` | `pay_status = 4` | 可跨周求和；不可混合总计与明细 |
| 支付订单数周环比 | `pay_order_count_wow` | 订单 | `(本周 - 上周) / 上周`；首周或上周为 0 时为 `NULL` | 同一 `channel + order_group` | 不可求和或平均 |
| 支付 GMV（USD） | `pay_gmv_usd` | 订单 | 周内 `SUM((pay_amount + pay_fee) * usd_rate)`，与日报同口径 | `pay_status = 4` | 可跨周求和；不可混合总计与明细 |
| 支付 GMV 周环比 | `pay_gmv_usd_wow` | 订单 | `(本周 - 上周) / 上周`；首周或上周为 0 时为 `NULL` | 同一 `channel + order_group` | 不可求和或平均 |
| 用户数占比 | `pay_user_count_share` | 用户 | 该行支付用户数 / 当周全站全部分组支付用户数 | 无 | 不可求和或平均 |
| GMV 占比 | `pay_gmv_share` | 订单 | 该行 GMV / 当周全站全部分组 GMV | 无 | 不可求和或平均 |

补充说明：

- 出口只保留有支付数据的 `周 × 渠道 × 订单分组` 组合，不做固定枚举补零展示行；`dws_makepod_order_weekly` 中间层仍按固定 9 渠道 × 4 订单分组补齐周骨架，保证 `ads` 层算周环比时 `LAG` 不会跨过缺失周
- 最新一周若尚未完整（当前进行中的自然周），仍照常产出，环比数值会因周期不完整而偏低
- `order_group` 分桶口径（`1单/2单/3单及以上`）源自一次针对高价值用户的临时取数需求（同一份用户周内去重订单数逻辑），该临时取数主题文档已于 2026-07-31 归档删除；本周报在此基础上新增了 `全部分组`（不分桶汇总）与渠道交叉，以及周环比/占比派生指标
- 内部一致性：同一 `channel` 下 `1单 + 2单 + 3单及以上` 三行的 `pay_user_count`（及 GMV）之和应等于该 `channel` 的 `全部分组` 行；`Overall` 渠道下各 `order_group` 之和同理；`channel='Overall', order_group='全部分组'` 是唯一的当周全站总计行，也是占比分母
- Superset chart 展示层：三个周环比列（支付用户数周环比 / 支付订单数周环比 / 支付GMV周环比）配置了 `conditional_formatting`，值 `> 0` 显示 Superset 内置 `success` 浅绿色（`#ACE1C4`）、`< 0` 显示内置 `error` 浅红色（`#EFA1AA`），`= 0` 不着色

## Lark 推送

两个推送脚本都不新建表，只读 `decom.ads_daily_makepod` 与 `decom.ads_weekly_makepod_paid_user_order`；`remote` 模式读 `lark_webhook.remote_makepod`，本地测试读 `lark_webhook.local`。卡片均附【Makepod】平台看板跳转按钮，源表当日/当周 `Overall` 行缺失时推红色告警卡而非静默退出。

### 日报（`ads_daily_makepod_lark_push.py`）

- 触发：每日 `UTC 02:00`（= `Asia/Shanghai 10:00`），推送 `Asia/Shanghai` 昨日、`channel='Overall'` 数据
- 结构：单个 3 列 table（指标 / 当日 / 较前日），10 个指标依次为 `dau_uv_total`、`dau_new_uv`、`dau_old_uv`、`pay_user_count`、`pay_order_count`、`order_user_count`、`pay_gmv_usd`、`pay_sku_quantity`、`gross_profit_usd`、`gross_profit_rate`
- 波动口径：`(昨日 - 前日) / 前日` 相对变化率，两位小数；前日为 0 或缺行显示 `-`；涨绿跌红。`gross_profit_rate` 也按相对变化率而非百分点差

### 周报（`ads_weekly_makepod_lark_push.py`）

- 触发：每周一 `UTC 02:30`（= `Asia/Shanghai 周一 10:30`）；默认周为 `DATE_SUB(DATE_TRUNC(CURRENT_DATE('Asia/Shanghai'), WEEK(MONDAY)), INTERVAL 7 DAY)`，即上一个完整自然周，另有 `--week-start` 参数指定周一
- 四块内容：① 数据总览（3 列 table，日均 DAU / 总支付订单数 / 总 GMV / 总毛利 / 总毛利率 + 较上上周）② 趋势峰谷（3 张 `chart` 折线图 + markdown 峰谷摘要）③ 渠道明细（6 列 table）④ 订单分布（5 列 table）
- ①②③ 由 `ads_daily_makepod` 现场聚合（两周 × 9 渠道 = 126 行）：日均 DAU 取 `AVG(dau_uv_total)`（按实际有数据的天数，不足 7 天在卡片标注「仅 N 天数据」），交易类指标累加，总毛利率按 `SUM(gross_profit_usd) / SUM(pay_gmv_usd)` 重算而非日毛利率取均值
- ③ 渠道 DAU 用日均口径与 ① 保持一致（不是周去重 WAU，后者需新建周流量中间层）；只展示日均 DAU 或 GMV 非 0 的渠道，按 GMV 降序
- ④ 直接读周表 `channel='Overall'` 的 `1单 / 2单 / 3单及以上` 三行，占比与环比复用现成的 `pay_user_count_share` / `pay_gmv_share` / `pay_gmv_usd_wow`，不在推送层重算；周表出口过滤掉的分组在推送层补零成行，保证三层始终齐全
- ①与④互为校验：`Overall` 层面日报 `SUM(pay_order_count)`、`SUM(pay_gmv_usd)` 应等于周表 `全部分组` 行的对应值
- 峰谷取最大/最小值所在日期，数值并列时取最早那天

## 依赖

- 上游：`decom.dwd_makepod_event_log`、`v3_decom.user_info`、`v3_decom.user_order`、`v3_decom.pay_order`、`v3_decom.user_order_item`、`v3_decom.sku`、`decom.currency_conversion_dict`
- 维表：`decom.dim_makepod_device_first_visit`、`decom.dim_makepod_user_channel`
- 中间表：`decom.dws_makepod_traffic_daily`、`decom.dws_makepod_order_daily`、`decom.dws_makepod_order_weekly`
- ADS：`decom.ads_daily_makepod`、`decom.ads_weekly_makepod_paid_user_order`
- DDL：`ddl/dim/dim_makepod_device_first_visit.sql`、`ddl/dim/dim_makepod_user_channel.sql`、`ddl/dws/dws_makepod_traffic_daily.sql`、`ddl/dws/dws_makepod_order_daily.sql`、`ddl/dws/dws_makepod_order_weekly.sql`、`ddl/ads/ads_daily_makepod.sql`、`ddl/ads/ads_weekly_makepod_paid_user_order.sql`
- 脚本：`jobs/dim/dim_makepod_device_first_visit.py`、`jobs/dim/dim_makepod_user_channel.py`、`jobs/dws/dws_makepod_traffic_daily.py`、`jobs/dws/dws_makepod_order_daily.py`、`jobs/dws/dws_makepod_order_weekly.py`、`jobs/ads/ads_daily_makepod.py`、`jobs/ads/ads_weekly_makepod_paid_user_order.py`、`jobs/ads/ads_daily_makepod_lark_push.py`、`jobs/ads/ads_weekly_makepod_lark_push.py`
- 编排：`env/etl_config.json`（ETL 表任务）；两个 Lark 推送走 `env/cron.txt` 独立 cron，不随 pipeline 调度
- Lark webhook：`env/lark_webhook.json` 的 `remote_makepod`（正式群）、`local`（测试群）
- Superset：物理 dataset `ads_daily_makepod`（id=261），chart `Makepod 独立站日报（日表）- 流量趋势`（id=633）、`Makepod 独立站日报（日表）- 交易人数趋势`（id=634）、`Makepod 独立站日报（日表）- 交易金额趋势`（id=635）、`Makepod 独立站日报（日表）- 明细表`（id=636），dashboard `【Makepod】平台看板`（id=115，默认日期 filter=`Last month`）
- 周报 Superset 资产：物理 dataset `ads_weekly_makepod_paid_user_order`（id=293，`main_dttm_col=week_start_dt`），chart `Makepod 独立站周报（周表）- 支付用户订单分布`（id=719），同挂 dashboard `【Makepod】平台看板`（id=115）；`日期` 时间 filter 已排除该 chart，`渠道` filter 已新增指向 dataset 293 的 target 同时生效，新增 `周范围（周报）` 取值型 filter（仅作用于该 chart，指向 dataset 293 的 `week_range` 列）
