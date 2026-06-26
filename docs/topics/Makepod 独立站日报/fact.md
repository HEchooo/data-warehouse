# Makepod 独立站日报

## 报表

- 表：`decom.ads_daily_makepod`
- Dashboard：[【Makepod】平台看板](https://bi.alvinclub.ca/superset/dashboard/115/)（id=115）
- 粒度：`dt × channel`
- 时区：`Asia/Shanghai`
- 主题：Makepod 独立站流量与交易日汇总

## 字段

| 需求字段 | 落地字段 | 口径 |
|---|---|---|
| 渠道 | `channel` | 固定枚举 `Facebook / Google / TikTok / Other`；映射关系见 `docs/rules/makepod.md` |
| 全量 DAU | `dau_uv_total` | 当日按 `channel` 去重后的 `prop_device_id`；同一设备同日可在多个渠道各记 1 次 UV |
| 新用户 DAU | `dau_new_uv` | 当日按 `channel` 去重后的 `prop_device_id` 中，`dim_makepod_device_first_visit.first_visit_date = dt`；同一设备同日可在多个渠道各记 1 次 UV |
| 老用户 DAU | `dau_old_uv` | 当日按 `channel` 去重后的 `prop_device_id` 中，`dim_makepod_device_first_visit.first_visit_date < dt`；同一设备同日可在多个渠道各记 1 次 UV |
| 注册人数 | `register_user_count` | `v3_decom.user_info.tenant_code = 'T18002707'`，按 `DATE(TIMESTAMP(gmt_create, 'UTC'), 'Asia/Shanghai')` 和 `user_info.invite_user_id` 映射渠道统计去重用户数；用户键优先取 `user_id`，缺失时回退 `id` |
| 商详页访问人数 | `product_detail_view_uv` | `event_name = 'view' AND args.page_key = 'product_detail'`，按 `channel` 去重 `prop_device_id`；同一设备同日可在多个渠道各记 1 次 UV |
| 加购人数 | `add_cart_uv` | `event_name = 'click' AND args.page_key = 'product_detail' AND args.module = 'customization_panel'`，按 `channel` 去重 `prop_device_id`；同一设备同日可在多个渠道各记 1 次 UV |
| 下单人数 | `order_user_count` | `v3_decom.user_order.tenant_code = 'T18002707'`，按 `DATE(TIMESTAMP(gmt_create, 'UTC'), 'Asia/Shanghai')` 和 `user_info.invite_user_id` 映射渠道统计去重 `user_id` |
| 支付人数 | `pay_user_count` | `v3_decom.pay_order.tenant_code = 'T18002707' AND pay_status = 4`，按 `DATE(TIMESTAMP(pay_time, 'UTC'), 'Asia/Shanghai')` 和 `user_info.invite_user_id` 映射渠道统计去重 `user_id` |
| 支付订单数 | `pay_order_count` | `v3_decom.pay_order.tenant_code = 'T18002707' AND pay_status = 4`，按 `DATE(TIMESTAMP(pay_time, 'UTC'), 'Asia/Shanghai')` 和 `user_info.invite_user_id` 映射渠道统计去重 `order_id`；未命中映射时归 `Other` |
| 支付 GMV | `pay_gmv_usd` | 按 `channel` 汇总 `SUM((pay_amount + pay_fee) * usd_rate)`；`pay_fee` 保持源表负数参与实付金额计算；汇率来自 `decom.currency_conversion_dict`；未命中映射时归 `Other` |
| 支付手续费 | `pay_fee_amount` | 按 `channel` 汇总 `SUM(pay_fee * usd_rate)`；`currency_type = 'USD'` 时按 1 折算，其他币种使用 `decom.currency_conversion_dict` 转 USD，并保持源表负数符号；未命中映射时归 `Other` |
| 支付 SKU 件数 | `pay_sku_quantity` | `pay_status = 4` 的支付订单，按 `user_order_item.order_id = pay_order.order_id` 汇总 `SUM(sku_quantity)` 后再按用户渠道聚合；未命中映射时归 `Other` |
| 达人带来的访客数 | `kol_invited_visitor_uv` | 当日按 `channel` 去重后的 `prop_device_id` 中，存在 `invite_user_id = v3_decom.kol_rel.user_id` 的 Makepod 事件；同一设备同日可在多个渠道各记 1 次 UV |

## 依赖

- 上游：`decom.dwd_makepod_event_log`、`v3_decom.user_info`、`v3_decom.user_order`、`v3_decom.pay_order`、`v3_decom.user_order_item`、`v3_decom.kol_rel`、`decom.currency_conversion_dict`
- 维表：`decom.dim_makepod_device_first_visit`
- 中间表：`decom.dws_makepod_traffic_daily`、`decom.dws_makepod_order_daily`
- ADS：`decom.ads_daily_makepod`
- DDL：`ddl/dim/dim_makepod_device_first_visit.sql`、`ddl/dws/dws_makepod_traffic_daily.sql`、`ddl/dws/dws_makepod_order_daily.sql`、`ddl/ads/ads_daily_makepod.sql`
- 脚本：`jobs/dim/dim_makepod_device_first_visit.py`、`jobs/dws/dws_makepod_traffic_daily.py`、`jobs/dws/dws_makepod_order_daily.py`、`jobs/ads/ads_daily_makepod.py`
- 编排：`env/etl_config.json`
- Superset：物理 dataset `ads_daily_makepod`（id=261），chart `Makepod 独立站日报（日表）- 流量趋势`（id=633）、`Makepod 独立站日报（日表）- 交易人数趋势`（id=634）、`Makepod 独立站日报（日表）- 交易金额趋势`（id=635）、`Makepod 独立站日报（日表）- 明细表`（id=636），dashboard `【Makepod】平台看板`（id=115，默认日期 filter=`Last month`）

## 当前状态

- 截至 `2026-06-10`，已完成建表、4 张 ETL 脚本生产全量初始化、手续费 USD 口径更新与 Superset 看板创建；当时已初始化 `2026-05-27` 到 `2026-06-10` 的日报分区，订单主题已初始化 `2026-05-29` 到 `2026-06-10`
- 截至 `2026-06-26`，仓库已补充分渠道代码与 DDL：流量侧按 `prop_share_code` 映射渠道，注册/下单/支付侧按 `user_info.invite_user_id` 映射渠道；历史分区是否回填需另行确认
