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
| 渠道 | `channel` | 固定枚举 `TikTok / Google / Facebook / Other Channel / Organic / Overall`；映射关系见 `docs/rules/makepod.md` |
| 全量 DAU | `dau_uv_total` | `TikTok / Google / Facebook / Other Channel / Organic` 按 `channel × dt` 去重 `prop_device_id`；`Overall` 按全站 `dt` 去重 `prop_device_id`，跨渠道重复设备只记 1 次 |
| 新用户 DAU | `dau_new_uv` | `TikTok / Google / Facebook / Other Channel / Organic` 按 `channel × dt` 去重后的新设备统计；`Overall` 按全站去重新设备统计，判定条件为 `dim_makepod_device_first_visit.first_visit_date = dt` |
| 老用户 DAU | `dau_old_uv` | `TikTok / Google / Facebook / Other Channel / Organic` 按 `channel × dt` 去重后的老设备统计；`Overall` 按全站去重老设备统计，判定条件为 `dim_makepod_device_first_visit.first_visit_date < dt` |
| 注册人数 | `register_user_count` | `TikTok / Google / Facebook / Other Channel / Organic` 按 `dim_makepod_user_channel.register_date × channel` 统计去重注册用户数；`Overall` 按注册日全站去重注册用户数 |
| 商详页访问人数 | `product_detail_view_uv` | `TikTok / Google / Facebook / Other Channel / Organic` 按 `channel × dt` 去重 `prop_device_id`；`Overall` 按全站去重 `prop_device_id`，跨渠道重复设备只记 1 次 |
| 加购人数 | `add_cart_uv` | `TikTok / Google / Facebook / Other Channel / Organic` 按 `channel × dt` 去重 `prop_device_id`；`Overall` 按全站去重 `prop_device_id`，跨渠道重复设备只记 1 次 |
| 下单人数 | `order_user_count` | `TikTok / Google / Facebook / Other Channel / Organic` 按 `dt × channel` 统计去重 `user_id`，渠道来自 `dim_makepod_user_channel`；若用户未命中维表则回退 `Organic`；`Overall` 按下单日全站去重 `user_id` |
| 支付人数 | `pay_user_count` | `TikTok / Google / Facebook / Other Channel / Organic` 按 `dt × channel` 统计去重 `user_id`，渠道来自 `dim_makepod_user_channel`；若用户未命中维表则回退 `Organic`；`Overall` 按支付日全站去重 `user_id` |
| 支付订单数 | `pay_order_count` | `TikTok / Google / Facebook / Other Channel / Organic` 按 `dt × channel` 统计去重 `order_id`，渠道来自 `dim_makepod_user_channel`；若用户未命中维表则回退 `Organic`；`Overall` 按支付日全站去重 `order_id` |
| 支付 GMV | `pay_gmv_usd` | `TikTok / Google / Facebook / Other Channel / Organic` 按 `channel × dt` 汇总 `SUM((pay_amount + pay_fee) * usd_rate)`；`Overall` 按支付日全站汇总同口径 GMV；`pay_fee` 保持源表负数参与实付金额计算；汇率来自 `decom.currency_conversion_dict` |
| 支付手续费 | `pay_fee_amount` | `TikTok / Google / Facebook / Other Channel / Organic` 按 `channel × dt` 汇总 `SUM(pay_fee * usd_rate)`；`Overall` 按支付日全站汇总同口径手续费；`currency_type = 'USD'` 时按 1 折算，其他币种使用 `decom.currency_conversion_dict` 转 USD，并保持源表负数符号 |
| 支付 SKU 件数 | `pay_sku_quantity` | `pay_status = 4` 的支付订单，按 `user_order_item.order_id = pay_order.order_id` 汇总 `SUM(sku_quantity)` 后，再按 `dt × channel` 聚合；`Overall` 为支付日全站汇总 |

## 依赖

- 上游：`decom.dwd_makepod_event_log`、`v3_decom.user_info`、`v3_decom.user_order`、`v3_decom.pay_order`、`v3_decom.user_order_item`、`decom.currency_conversion_dict`
- 维表：`decom.dim_makepod_device_first_visit`、`decom.dim_makepod_user_channel`
- 中间表：`decom.dws_makepod_traffic_daily`、`decom.dws_makepod_order_daily`
- ADS：`decom.ads_daily_makepod`
- DDL：`ddl/dim/dim_makepod_device_first_visit.sql`、`ddl/dim/dim_makepod_user_channel.sql`、`ddl/dws/dws_makepod_traffic_daily.sql`、`ddl/dws/dws_makepod_order_daily.sql`、`ddl/ads/ads_daily_makepod.sql`
- 脚本：`jobs/dim/dim_makepod_device_first_visit.py`、`jobs/dim/dim_makepod_user_channel.py`、`jobs/dws/dws_makepod_traffic_daily.py`、`jobs/dws/dws_makepod_order_daily.py`、`jobs/ads/ads_daily_makepod.py`
- 编排：`env/etl_config.json`
- Superset：物理 dataset `ads_daily_makepod`（id=261），chart `Makepod 独立站日报（日表）- 流量趋势`（id=633）、`Makepod 独立站日报（日表）- 交易人数趋势`（id=634）、`Makepod 独立站日报（日表）- 交易金额趋势`（id=635）、`Makepod 独立站日报（日表）- 明细表`（id=636），dashboard `【Makepod】平台看板`（id=115，默认日期 filter=`Last month`）

## 当前状态

- 截至 `2026-06-10`，已完成建表、4 张 ETL 脚本生产全量初始化、手续费 USD 口径更新与 Superset 看板创建；当时已初始化 `2026-05-27` 到 `2026-06-10` 的日报分区，订单主题已初始化 `2026-05-29` 到 `2026-06-10`
- 截至 `2026-06-26`，已完成分渠道代码与 DDL 上线，并对 `decom.dws_makepod_traffic_daily`、`decom.dws_makepod_order_daily`、`decom.ads_daily_makepod` 做历史全量回刷；订单与 ADS 的金额类历史值按当前 `decom.currency_conversion_dict` 重新换算
- 截至 `2026-06-26`，仓库代码口径已调整为 `TikTok / Google / Facebook / Other Channel / Organic / Overall`；其中 `Other Channel` 表示存在 `share_code` 或 `invite_user_id` 但未命中三条指定映射，`Organic` 表示两者都缺失，`Overall` 为全站去重汇总
- 截至 `2026-06-26`，Superset 物理 dataset `ads_daily_makepod`（id=261）已 refresh，dashboard `【Makepod】平台看板`（id=115）已新增 `channel` native filter
