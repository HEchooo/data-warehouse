# makepod 独立站支持用户在游客态的状态下下单

## makpod 相关数据时区需要用 `Asia/Shanghai`

## 货币单位转换表

- `decom.currency_conversion_dict`

## 日志相关

- `decom.dwd_makepod_event_log` 表，也就是 `ods_event_log` 中 `prop_version_type = 'makepod'`
- `invite_user_id` 是邀请用户 id，其中包含达人
## 渠道映射

- 当前 Makepod 已知映射渠道固定为：`Facebook`、`Google`、`TikTok`、`TK-ELSA-AI`、`YOUTUBE-ELSA-AI`、`Reddit`
- 报表展示渠道固定为：`TikTok`、`Google`、`Facebook`、`TK-ELSA-AI`、`YOUTUBE-ELSA-AI`、`Reddit`、`Other Channel`、`Organic`、`Overall`
- `Other Channel`：未命中已知映射，但存在 `share_code` 或 `invite_user_id`
- `Organic`：不存在 `share_code` 且不存在 `invite_user_id`
- `Overall`：全站去重汇总；不是前面各渠道直接相加，跨渠道重复主体在 `Overall` 中只记 1 次
- `prop_share_code -> invite_user_id -> channel` 映射：
  - `AAACXvNZSVQ -> 2606093799415 -> Facebook`
  - `AAACXvMF8vw -> 2606090368095 -> Google`
  - `AAACXvMzgK4 -> 2606091027981 -> TikTok`
  - `AAACXzUeDVA -> 2607062764531 -> TK-ELSA-AI`
  - `AAACXzUDkV8 -> 2607063062524 -> YOUTUBE-ELSA-AI`
  - `600jur9wxV -> 2607226173754 -> Reddit`
- 邀请码解码坑：`600jur9wxV` 以 `6` 开头，服务端真实解码（`invitecode_mapping.py`，含 `6` 前缀 Base62 分支）得 `2607226173754`，与 `user_info.invite_user_id` 一致；但 `dwd_makepod_event_log` 的 BigQuery JS UDF `invite_code_to_user_id` 未处理 `6` 前缀，对该码解码错误。因此：
  - 服务端口径（`dim_makepod_user_channel`，基于 `user_info.invite_user_id`）用 `2607226173754` 匹配
  - 事件流量口径（`dws_makepod_traffic_daily`）必须靠 `prop_share_code = '600jur9wxV'` 匹配，不能依赖事件表 `invite_user_id`

## 注册相关

- `v3_decom.user_info` 表，`tenant_code = 'T18002707'`
- 注册时间：`v3_decom.user_info.gmt_create`

## 订单相关

- `v3_decom.pay_order` 表，`tenant_code = 'T18002707'`
- `pay_status` 枚举：
  - `1` 待支付
  - `2` 已取消
  - `3` 超时关闭
  - `4` 支付成功
- 下单时间：`v3_decom.user_order.gmt_create`
  - 关联条件：`pay_order.order_id = user_order.order_id AND pay_order.tenant_code = user_order.tenant_code`
- 实付金额：`v3_decom.pay_order.pay_amount + v3_decom.pay_order.pay_fee`
  - 原因：`pay_fee` 是负数
- 手续费数据：`v3_decom.pay_order.pay_fee`
- 售出商品：`SUM(v3_decom.user_order_item.sku_quantity)`
  - 关联条件：`user_order_item.order_id = pay_order.order_id`

## 采购相关
- 采购价：`v3_decom.sku.purchase_price`
- 采购价单位：`v3_decom.sku.purchase_cur`
- 关联条件：`user_order_item.sku_code = sku.sku_code`，并限定 `sku.tenant_code = 'T18002707'`
- 采购成本（USD）：支付成功订单的 `SUM(sku.purchase_price * user_order_item.sku_quantity * usd_rate)`
- 物流成本（USD）：支付成功的去重订单数乘以 `5 USD`
