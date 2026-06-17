# 产品 OKR 数据汇总经验

## 周级 OKR 口径不要直接平均率

- 周级率指标（如试穿渗透率）不能直接对每日率做 `AVG`
- 正确做法是先聚合周内分子和分母，再做 `SAFE_DIVIDE(周分子, 周分母)`
- 这次已在 `ads_weekly_ai_fashion_tryon_okr.py` 固化该规则

## 临时取数的 DAU 不要直接扫未过滤事件表

- 如果临时 SQL 直接从 `decom.dwd_event_log` 按 `prop_user_id` 去重算 DAU，会把 `send_push`、`send_email` 这类被动触达事件也算成活跃用户
- 对齐【AI-Fashion】全量报表“到访注册用户”时，应优先复用 `decom.dws_user_daily`，该表已排除被动触达事件
- 需要按 `prop_app_version` 限定用户时，也应先限定 `dws_user_daily` 活跃用户集合，再用同日 `dwd_event_log.prop_app_version` 命中关系做版本过滤
