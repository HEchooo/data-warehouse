# 产品 OKR 数据汇总经验

## 周级 OKR 口径不要直接平均率

- 周级率指标（如试穿渗透率）不能直接对每日率做 `AVG`
- 正确做法是先聚合周内分子和分母，再做 `SAFE_DIVIDE(周分子, 周分母)`
- 这次已在 `ads_weekly_ai_fashion_tryon_okr.py` 固化该规则

## 临时取数的 DAU 不要直接扫未过滤事件表

- 如果临时 SQL 直接从 `decom.dwd_event_log` 按 `prop_user_id` 去重算 DAU，会把 `send_push`、`send_email` 这类被动触达事件也算成活跃用户
- 对齐【AI-Fashion】全量报表“到访注册用户”时，应优先复用 `decom.dws_user_daily`，该表已排除被动触达事件
- 需要按 `prop_app_version` 限定用户时，也应先限定 `dws_user_daily` 活跃用户集合，再用同日 `dwd_event_log.prop_app_version` 命中关系做版本过滤

## v1/v2 当前周比对要区分口径差异和刷新滞后

- v2 宽表从 DWS 现算回刷后，当前周可能和线上 v1 长表出现少数字段差异
- 先检查 v1、v2 与上游 DWS 的 `update_time`，如果 v1 早于 DWS 更新，而 v2 晚于 DWS 更新，差异通常是 v1 表未刷新到最新中层，不是 v2 口径漂移
- 判断 v2 逻辑一致性时，不要直接用滞后的线上 v1 当前周下结论；应把 v1 逻辑跑到临时校验表，再和 v2 宽表逐字段比对

## styleme 渗透率异常先判断是否为分母埋点稀疏

- `styleme试穿渗透率` 当前口径的分母是前端 `v_styleme_popup` / `v_styleme_page` UV
- 在 `2026-06-08` 之前，这两个事件的历史量级明显偏低，容易出现周级分母很小、渗透率异常放大的现象
- 排查时先同时核对周级 `styleme_uv` 与 `styleme_exposure_uv`，不要先怀疑 Superset 百分比格式

## 周分布图改口径时先验收“是否已部署”再验数

- 这次 AI-Fashion 3 个周分布图虽然代码已经改成“按用户分桶 + 周去重 DAU 分母”，但线上验收时发现新 DWS 表和 ADS 新列都还没有落库
- 如果直接拿当前 dashboard 数字和新口径预期对比，很容易把“未部署”误判成“实现错误”
- 正确顺序应是：
  - 先查 `INFORMATION_SCHEMA.TABLES`，确认 `dws_ai_fashion_tryon_okr_user_daily` 已建表
  - 再查 `INFORMATION_SCHEMA.COLUMNS`，确认 `ads_weekly_ai_fashion_tryon_okr_v2` 新增的商品点击 ratio 列已存在
  - 最后再用源表重算一周，和 `ads_weekly_ai_fashion_tryon_okr_v2` 做桶人数/占比对比
