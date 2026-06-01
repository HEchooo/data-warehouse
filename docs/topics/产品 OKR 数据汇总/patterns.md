# 产品 OKR 数据汇总经验

## 周级 OKR 口径不要直接平均率

- 周级率指标（如试穿渗透率）不能直接对每日率做 `AVG`
- 正确做法是先聚合周内分子和分母，再做 `SAFE_DIVIDE(周分子, 周分母)`
- 这次已在 `ads_weekly_ai_fashion_tryon_okr.py` 固化该规则
