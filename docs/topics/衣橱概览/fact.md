# 衣橱概览

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 指标 | 衣橱首页曝光 UV | 衣橱曝光人数 |
| 指标 | 新增衣橱使用用户 | 新增衣橱用户、首次使用衣橱用户 |
| 指标 | 新增 item 数量 | 新增单品数、新增衣橱商品数 |
| 指标 | 渗透率 | 衣橱渗透率 |
| 指标 | 总计使用用户 | 累计衣橱用户数 |

## 适用范围

- 业务线：`Alvin's Club App`
- 适合：查询衣橱首页曝光、新增使用用户、新增单品和衣橱使用表现。
- 不适合：普通商品商详与试穿不属于本主题；应使用对应商详或试穿 topic。
- 需要澄清：需要确认日期范围、用户或单品统计主体，以及新增或全量口径。

## 报表

### ads_daily_closet_overview

- 表：`my-project-8584-jetonai.decom.ads_daily_closet_overview`
- 状态：`active`
- 粒度：`dt`
- 时区：`America/Toronto`
- 数据范围：App 衣橱首页曝光、用户使用和单品新增的日汇总。
- 必要过滤：无。
- 不包含：普通商品商详和非衣橱试穿。
- Dashboard：[【AI-Fashion】衣橱概览](https://bi.alvinclub.ca/superset/dashboard/106/)（id=106）
- Superset：物理 dataset `ads_daily_closet_overview`（id=235），chart `衣橱概览（日表）-明细表`（id=584）

## 字段

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 衣橱首页曝光 UV | `closet_home_exposure_uv` | 已登录用户 | `event_name IN ('v_closet', 'v_smart_closet_home')` 的 `COUNT(DISTINCT prop_user_id)`；2026-07-07 前后曝光埋点由 `v_closet` 迁移到 `v_smart_closet_home`，口径兼容新旧 | `prop_user_id IS NOT NULL AND prop_user_id != ''` | 不可跨天求和 |
| 新增衣橱使用用户 | `new_closet_user_count` | 已登录用户 | 用户首次发生 `type IN ('SINGLE', 'OUTFIT')` 的业务日记 1 次；首次日以 `v3_decom.user_fashion_closet.gmt_create` 转 `America/Toronto` 业务日计算 | 无 | 可跨天求和；每个用户只在首次使用日计 1 次 |
| 新增 item 数量 | `new_item_count` | 单品 | 当日新增 item 数量；`SINGLE` 按非空 `uuid` 计数，`OUTFIT` 按 `item_uuids` 拆数组后的有效元素个数求和 | 无 | 可跨天求和 |
| 总计使用用户 | `total_closet_user_count` | 已登录用户 | 历史累计去重用户数；按每个用户首次 `type IN ('SINGLE', 'OUTFIT')` 的业务日累计 | 无 | 已是累计值，取目标日当天一行，不可跨天求和 |
| 渗透人数 | `penetration_user_count` | 已登录用户 | 同日衣橱首页曝光用户集合与同日 `type IN ('SINGLE', 'OUTFIT')` 用户集合的交集 UV | `prop_user_id IS NOT NULL AND prop_user_id != ''` | 不可跨天求和 |
| 渗透率分母 | `all_event_uv` | 已登录用户 | 当日日志所有事件的 `COUNT(DISTINCT prop_user_id)` | `prop_user_id IS NOT NULL AND prop_user_id != ''` | 不可跨天求和 |
| 渗透率 | `penetration_rate` | 已登录用户 | `penetration_user_count / all_event_uv`；`all_event_uv = 0` 时返回 `0` | 无 | 不可 `AVG`；跨天须按 `SUM(penetration_user_count) / SUM(all_event_uv)` 重算 |

补充说明：

- 本主题所有 UV 相关指标均统一使用已登录用户口径：`COUNT(DISTINCT prop_user_id)`，并过滤 `prop_user_id IS NOT NULL AND prop_user_id != ''`
- `user_fashion_closet.gmt_create` 视为 UTC `DATETIME`，统一按 `DATE(TIMESTAMP(gmt_create, 'UTC'), 'America/Toronto')` 切业务日
- 用户首次衣橱使用口径已单独沉到 `decom.dim_user_closet_first_use`，不再由 `decom.dws_closet_daily` 每次全量重扫服务端表计算
- 服务端相关口径统一按 `type IN ('SINGLE', 'OUTFIT')` 处理；不再只看 `SINGLE`
- `新增item数量` 当前按 item 数量统计，不再附着“首日用户”条件；其中 `OUTFIT.item_uuids` 若为空或包含异常空元素，会按过滤后的有效元素个数计算

## 依赖

- 上游：`decom.dws_closet_daily`
- 中间表：`decom.dim_user_closet_first_use`, `decom.dws_closet_daily`, `decom.dws_user_daily`, `decom.dwd_event_log`, `v3_decom.user_fashion_closet`
- 报表表：`decom.ads_daily_closet_overview`
- 脚本：`jobs/ads/ads_daily_closet_overview.py`
- DIM 脚本：`jobs/dim/dim_user_closet_first_use.py`
- DWS 脚本：`jobs/dws/dws_closet_daily.py`
- DIM DDL：`ddl/dim/dim_user_closet_first_use.sql`
- DDL：`ddl/ads/ads_daily_closet_overview.sql`
- DWS DDL：`ddl/dws/dws_closet_daily.sql`
- 编排：独立表级入口 `jobs/ads/ads_daily_closet_overview.py`
- Superset：物理 dataset `ads_daily_closet_overview`（id=235），chart `衣橱概览（日表）-明细表`（id=584），dashboard `【AI-Fashion】衣橱概览`（id=106）
