# 【Makepod】高价值用户监控

## 按周去重不能直接复用 ads_daily_makepod 日表加总

- `ads_daily_makepod`／`dws_makepod_order_daily` 是 `dt × channel` 每日聚合表，`pay_user_count` 已经是当天去重结果
- 如果为了拿"周维度支付用户数"直接把 7 天的 `pay_user_count` 相加，跨天多次支付的同一用户会被重复计数，数值偏高
- 正确做法：回到 `v3_decom.pay_order` 明细，按 `user_id` 在目标周期内整体去重后再统计，不能用日表反推周维度用户数
