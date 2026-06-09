# makepod 独立站支持用户在游客态的状态下下单

## makpod 相关数据时区需要用 `Asia/Shanghai`

## 货币单位转换表

- `decom.currency_conversion_dict`

## 日志相关

- `decom.dwd_makepod_event_log` 表，也就是 `ods_event_log` 中 `prop_version_type = 'makepod'`
- `invite_user_id` 是邀请用户 id，其中包含达人
- 达人指的是 `v3_decom.kol_rel.user_id = decom.dwd_makepod_event_log.invite_user_id` 的记录

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
