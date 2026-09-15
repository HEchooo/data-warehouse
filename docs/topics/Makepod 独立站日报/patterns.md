# Makepod 独立站日报

## 采购价异常值需要在回填前复查

- `v3_decom.sku.purchase_price` 在 SKU 配置未完成时可能短暂出现 `99999`；如果直接参与 CNY 到 USD 换算，会产生远高于 GMV 的采购成本
- 回填前应只针对支付成功订单涉及的 SKU 检查采购价范围、`99999` 数量、空采购价、空币种和未匹配 SKU
- 如果异常 SKU 正在被业务侧修正，应等待源表更新完成后重新查询；不能在 ETL 中擅自把 `99999` 固化为 0 或其他采购价

## 周报周口径曾误用 WEEK(SUNDAY)，已订正为 WEEK(MONDAY)

- 症状：周报范围是周日到周六，与同看板其他自然周图表错开一天。
- 根因：`dws_makepod_order_weekly` 最初复用了临时取数的 `WEEK(SUNDAY)`，与看板的 `Asia/Shanghai` 自然周口径不一致。
- 正确规则：周报统一使用 `WEEK(MONDAY)`；周支付用户数直接查询周表，不能把日报支付用户数相加。
- 影响范围：`dws_makepod_order_weekly`、`ads_weekly_makepod_paid_user_order`；2026-07-31 订正时需清理旧周日分区后全量重跑。
- 证据：`docs/topics/Makepod 独立站日报/fact.md`、`jobs/dws/dws_makepod_order_weekly.py`、`jobs/ads/ads_weekly_makepod_paid_user_order.py`。
- 回归案例：`analytics-makepod-weekly-paying-users`、`analytics-makepod-daily-sum`。

## Lark 卡片 `chart` 组件可用，且不占 table 的 5 个配额

- webhook 机器人发送 `schema 2.0` 卡片的 `chart` 组件（`chart_spec` 为 VChart 规范）可正常渲染，`type: line` + `xField` / `yField` + `label.visible` 即可出折线图
- 价值：`table` 组件单卡片上限 5 个（见【Social Content】疑似限流账号监控 `patterns.md`），趋势类内容改用 `chart` 能省下 table 配额。Makepod 周报四块内容因此只占 3 个 table
- 多指标趋势不要塞进同一张 chart：Makepod 周报的 DAU（百级）、支付订单数（个位）、GMV（百级）量级差三个数量级，同图会把订单数压成贴底直线，应拆成独立的多张 chart

## 卡片顶部说明行的「视觉不对齐」通常不是 `text_align` 的问题

- 现象：卡片顶部三行 `**标签**：值` 已显式设置 `"text_align": "left"`，但用户反馈看起来不是左对齐
- 两个真实原因：① 值用反引号包成代码块（`` `2026-07-30` ``）时，Lark 会给代码块加背景色和内边距，该行视觉起点比其他行右移；② 各行标签字数不等（如「统计周」3 字 vs「日期口径」4 字），全角冒号后的值起始位置随之错位
- 处理：说明行的值不用反引号，标签统一成相同字数；正文里需要强调的字段名保留 `**加粗**` 即可
