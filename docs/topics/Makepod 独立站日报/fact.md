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

## 报表

- 表：`decom.ads_daily_makepod`（日报）、`decom.ads_weekly_makepod_paid_user_order`（周报）
- Dashboard：[【Makepod】平台看板](https://bi.alvinclub.ca/superset/dashboard/115/)（id=115）
- 粒度：日报 `dt × channel`；周报 `week_start_dt（周一起自然周） × channel`
- 时区：`Asia/Shanghai`
- 主题：Makepod 独立站流量与交易日汇总 / 周维度支付用户订单分布
- Lark 推送：独立脚本 `jobs/ads/ads_daily_makepod_lark_push.py`（每日推送上海时区昨日 `Overall` 数据）、`jobs/ads/ads_weekly_makepod_lark_push.py`（每周一推送上一个完整自然周数据）；两者均不新建表，只读上述两张 ADS 表，`remote` 模式使用 `lark_webhook.remote_makepod`

## 字段（日报）

| 需求字段 | 落地字段 | 口径 |
|---|---|---|
| 渠道 | `channel` | 固定枚举 `TikTok / Google / Facebook / TK-ELSA-AI / YOUTUBE-ELSA-AI / Reddit / Other Channel / Organic / Overall`；映射关系见 `docs/rules/makepod.md` |
| 全量 DAU | `dau_uv_total` | `TikTok / Google / Facebook / TK-ELSA-AI / YOUTUBE-ELSA-AI / Reddit / Other Channel / Organic` 按 `channel × dt` 去重 `prop_device_id`；`Overall` 按全站 `dt` 去重 `prop_device_id`，跨渠道重复设备只记 1 次 |
| 新用户 DAU | `dau_new_uv` | `TikTok / Google / Facebook / TK-ELSA-AI / YOUTUBE-ELSA-AI / Reddit / Other Channel / Organic` 按 `channel × dt` 去重后的新设备统计；`Overall` 按全站去重新设备统计，判定条件为 `dim_makepod_device_first_visit.first_visit_date = dt` |
| 老用户 DAU | `dau_old_uv` | `TikTok / Google / Facebook / TK-ELSA-AI / YOUTUBE-ELSA-AI / Reddit / Other Channel / Organic` 按 `channel × dt` 去重后的老设备统计；`Overall` 按全站去重老设备统计，判定条件为 `dim_makepod_device_first_visit.first_visit_date < dt` |
| 注册人数 | `register_user_count` | `TikTok / Google / Facebook / TK-ELSA-AI / YOUTUBE-ELSA-AI / Reddit / Other Channel / Organic` 按 `dim_makepod_user_channel.register_date × channel` 统计去重注册用户数；`Overall` 按注册日全站去重注册用户数 |
| 商详页访问人数 | `product_detail_view_uv` | `TikTok / Google / Facebook / TK-ELSA-AI / YOUTUBE-ELSA-AI / Reddit / Other Channel / Organic` 按 `channel × dt` 去重 `prop_device_id`；`Overall` 按全站去重 `prop_device_id`，跨渠道重复设备只记 1 次 |
| 加购人数 | `add_cart_uv` | `TikTok / Google / Facebook / TK-ELSA-AI / YOUTUBE-ELSA-AI / Reddit / Other Channel / Organic` 按 `channel × dt` 去重 `prop_device_id`；`Overall` 按全站去重 `prop_device_id`，跨渠道重复设备只记 1 次 |
| 下单人数 | `order_user_count` | `TikTok / Google / Facebook / TK-ELSA-AI / YOUTUBE-ELSA-AI / Reddit / Other Channel / Organic` 按 `dt × channel` 统计去重 `user_id`，渠道来自 `dim_makepod_user_channel`；若用户未命中维表则回退 `Organic`；`Overall` 按下单日全站去重 `user_id` |
| 支付人数 | `pay_user_count` | `TikTok / Google / Facebook / TK-ELSA-AI / YOUTUBE-ELSA-AI / Reddit / Other Channel / Organic` 按 `dt × channel` 统计去重 `user_id`，渠道来自 `dim_makepod_user_channel`；若用户未命中维表则回退 `Organic`；`Overall` 按支付日全站去重 `user_id` |
| 支付订单数 | `pay_order_count` | `TikTok / Google / Facebook / TK-ELSA-AI / YOUTUBE-ELSA-AI / Reddit / Other Channel / Organic` 按 `dt × channel` 统计去重 `order_id`，渠道来自 `dim_makepod_user_channel`；若用户未命中维表则回退 `Organic`；`Overall` 按支付日全站去重 `order_id` |
| 支付 GMV | `pay_gmv_usd` | `TikTok / Google / Facebook / TK-ELSA-AI / YOUTUBE-ELSA-AI / Reddit / Other Channel / Organic` 按 `channel × dt` 汇总 `SUM((pay_amount + pay_fee) * usd_rate)`；`Overall` 按支付日全站汇总同口径 GMV；`pay_fee` 保持源表负数参与实付金额计算；汇率来自 `decom.currency_conversion_dict` |
| 支付手续费 | `pay_fee_amount` | `TikTok / Google / Facebook / TK-ELSA-AI / YOUTUBE-ELSA-AI / Reddit / Other Channel / Organic` 按 `channel × dt` 汇总 `SUM(pay_fee * usd_rate)`；`Overall` 按支付日全站汇总同口径手续费；`currency_type = 'USD'` 时按 1 折算，其他币种使用 `decom.currency_conversion_dict` 转 USD，并保持源表负数符号 |
| 支付 SKU 件数 | `pay_sku_quantity` | `pay_status = 4` 的支付订单，按 `user_order_item.order_id = pay_order.order_id` 汇总 `SUM(sku_quantity)` 后，再按 `dt × channel` 聚合；`Overall` 为支付日全站汇总 |
| 采购成本 | `purchase_cost_usd` | `pay_status = 4` 的支付订单，按 `user_order_item.sku_code = sku.sku_code` 获取 `sku.purchase_price × user_order_item.sku_quantity`，再使用 `sku.purchase_cur` 对应的 `decom.currency_conversion_dict` 汇率转换为 USD；按 `dt × channel` 汇总，`Overall` 为支付日全站汇总 |
| 物流成本 | `logistics_cost_usd` | `pay_status = 4` 的去重支付订单数 × `5 USD`；按 `dt × channel` 汇总，`Overall` 为支付日全站汇总 |
| 毛利 | `gross_profit_usd` | `pay_gmv_usd - purchase_cost_usd - logistics_cost_usd` |
| 毛利率 | `gross_profit_rate` | `gross_profit_usd / pay_gmv_usd`；支付 GMV 为 0 时返回 `NULL` |

## 字段（周报，`ads_weekly_makepod_paid_user_order`）

| 需求字段 | 落地字段 | 口径 |
|---|---|---|
| week_range | `week_range` | 按自然周分组，周一 ~ 周日，展示为 `2026-07-13 ~ 2026-07-19` |
| 渠道 | `channel` | 与日报一致的固定枚举，含 `Overall`（全站去重汇总，不是各渠道相加） |
| 订单分组 | `order_group` | 按用户在同一周内的支付成功订单数分桶：`1单` / `2单` / `3单及以上`；另有 `全部分组`（不分桶的原有汇总，兼容既有展示）。渠道对用户是固定属性，按 `user_id` 计数不会因渠道拆分重复放大 |
| 支付用户数 | `pay_user_count` | 周内按 `user_id` 去重的支付成功（`pay_status = 4`）用户数；不能由日报 `pay_user_count` 按 7 天相加得出（跨天多次支付会被重复计数），需回 `v3_decom.pay_order` 明细在周窗口内整体去重 |
| 支付用户数周环比 | `pay_user_count_wow` | `(本周 - 上周) / 上周`，`SAFE_DIVIDE` 实现，按 `channel + order_group` 分区取相邻上一周；首周或上周为 0 时为 `NULL` |
| 支付订单数 | `pay_order_count` | 周内按 `order_id` 去重的支付成功订单数 |
| 支付订单数周环比 | `pay_order_count_wow` | 同上 |
| 支付GMV（USD） | `pay_gmv_usd` | 周内 `SUM((pay_amount + pay_fee) * usd_rate)`，与日报 `pay_gmv_usd` 同口径；Superset chart 中展示为 `支付GMV（USD）`，数值格式 `$,.2f` |
| 支付GMV周环比 | `pay_gmv_usd_wow` | 同上 |
| 用户数占比 | `pay_user_count_share` | `该行 pay_user_count / 当周 channel='Overall' 且 order_group='全部分组' 行的 pay_user_count`（当周全站总支付用户数，不随渠道或订单分组变化） |
| GMV占比 | `pay_gmv_share` | `该行 pay_gmv_usd / 当周 channel='Overall' 且 order_group='全部分组' 行的 pay_gmv_usd` |

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

## 当前状态

- 截至 `2026-06-10`，已完成建表、4 张 ETL 脚本生产全量初始化、手续费 USD 口径更新与 Superset 看板创建；当时已初始化 `2026-05-27` 到 `2026-06-10` 的日报分区，订单主题已初始化 `2026-05-29` 到 `2026-06-10`
- 截至 `2026-06-26`，已完成分渠道代码与 DDL 上线，并对 `decom.dws_makepod_traffic_daily`、`decom.dws_makepod_order_daily`、`decom.ads_daily_makepod` 做历史全量回刷；订单与 ADS 的金额类历史值按当前 `decom.currency_conversion_dict` 重新换算
- 截至 `2026-06-26`，仓库代码口径已调整为 `TikTok / Google / Facebook / TK-ELSA-AI / YOUTUBE-ELSA-AI / Reddit / Other Channel / Organic / Overall`；其中 `Other Channel` 表示存在 `share_code` 或 `invite_user_id` 但未命中指定映射，`Organic` 表示两者都缺失，`Overall` 为全站去重汇总
- 截至 `2026-06-26`，Superset 物理 dataset `ads_daily_makepod`（id=261）已 refresh，dashboard `【Makepod】平台看板`（id=115）已新增 `channel` native filter
- 截至 `2026-07-17`，已完成采购成本、物流成本、毛利、毛利率的 DWS/ADS 历史全量回填；`dws_makepod_order_daily` 共 50 个日期、400 行，`ads_daily_makepod` 共 52 个日期、416 行，每日期均补齐 8 个渠道
- 截至 `2026-07-17`，Superset 物理 dataset `ads_daily_makepod`（id=261）已 refresh，明细图表（id=636）已新增 `采购成本（USD）/物流成本（USD）/毛利（USD）/毛利率` 四列
- 截至 `2026-07-31`，周报 `dws_makepod_order_weekly` / `ads_weekly_makepod_paid_user_order` 已完成建表、ETL 脚本与编排上线；用户确认后已完成生产历史全量初始化（`dws_makepod_order_weekly` 9 个周 × 9 渠道，`ads_weekly_makepod_paid_user_order` 出口过滤后 22 行）与 Superset dataset（id=293）/chart（id=719）创建、dashboard 115 挂载与三个 filter 的 scope 调整；抽查历史周的支付用户数/订单数与【Makepod】高价值用户监控主题的一次性取数（2026-07-27 取数结果，该主题文档已于 2026-07-31 归档删除）逐格一致（GMV 因汇率字典更新有合理小幅漂移）
- 截至 `2026-07-31`（同日追加），根据用户反馈完成两处调整：① Superset chart 的三个周环比列改用内置 `success`/`error` 预设配色（`#ACE1C4` / `#EFA1AA`），比初版自定义深色更柔和；② 新增 `order_group`（订单分组）维度，粒度扩展为 `周 × 渠道 × 订单分组`，`dws_makepod_order_weekly` / `ads_weekly_makepod_paid_user_order` 均已 `ALTER TABLE ADD COLUMN order_group STRING` 并完成生产历史全量重建（`dws_makepod_order_weekly` 现为 9 周 × 9 渠道 × 4 订单分组 = 324 行，`ads_weekly_makepod_paid_user_order` 出口过滤后 65 行）；chart 719 新增「订单分组」展示列并调整排序；已校验同渠道下 3 个分桶行之和等于 `全部分组` 行，`Overall` 渠道同理成立
- 截至 `2026-07-31`（同日再追加），根据用户反馈订正周口径：`dws_makepod_order_weekly` 原先误用 `WEEK(SUNDAY)`（周日起），与看板标注的 `Asia/Shanghai` 时区自然周不符，已改为 `WEEK(MONDAY)`（周一起），与仓库其他周表口径一致；`week_start_dt` 整体位移，已清空 `dws_makepod_order_weekly` 生产表并全量重跑（仍为 9 周 × 9 渠道 × 4 订单分组 = 324 行），`ads_weekly_makepod_paid_user_order` 走 staging+swap 全量重建（出口过滤后 67 行，因周边界位移与数据分布变化，行数较订正前的 65 行略有出入，属正常现象）；详见 `patterns.md`
- 截至 `2026-07-31`（同日再追加），新增 Lark 日报与周报推送脚本 `jobs/ads/ads_daily_makepod_lark_push.py`、`jobs/ads/ads_weekly_makepod_lark_push.py`，均不新建表、不改 ETL，只读现成两张 ADS 表；已用 `lark_webhook.local` 测试群完成样式验证（日报 `2026-07-30`、周报 `2026-07-20 ~ 2026-07-26`），并按用户反馈完成三轮微调：三列全部居中、第二列表头由日期改为固定文案「当日」、周报删除「环比口径」说明行改为「更新时间」（取日报表与周表两者较晚的 `update_time`）、顶部说明行标签统一为 4 字且去掉反引号代码块；`env/cron.txt` 已加入两条独立 cron（`0 2 * * *` 与 `30 2 * * 1`，UTC），正式群 webhook `remote_makepod` 已配置但尚未实际推送过
