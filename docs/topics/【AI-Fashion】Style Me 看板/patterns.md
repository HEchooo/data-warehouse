# 【AI-Fashion】Style Me 看板 - 踩坑记录

## 1. 场景明细表不要直接展示宽表字段

- 问题：
  `Quick / Plus Item / Plus Outfit` 如果直接横向铺成字段，Superset table 最终会出现大量 `quick_* / plus_*` 列，可读性很差。
- 处理：
  保留物理 ADS 宽表 `ads_daily_ai_fashion_styleme` 作为报表层单点来源，在 Superset 侧补一个虚拟 dataset `ads_daily_ai_fashion_styleme_scenario_detail`，把宽表转成 `dt + scenario + 指标` 的长表。
- 原因：
  这只是展示建模问题，不值得为此新增一张物理 ADS 表并触发生产回刷。
- 注意：
  Superset raw table 不能稳定按“未展示的隐藏排序列”排序；如果又要固定场景顺序、又不想显示可见编号，可以把 `CHR(1) / CHR(2) / CHR(3)` 这类不可见控制字符并入场景值。
