# 试穿结果转化日报 (ads_daily_tryon_add_cart_conversion)

## 报表定义

按日期（dt）统计试穿 UV、加购 UV，并计算加购率（加购UV / 试穿UV）。

**粒度**：`dt`（每日一行）

## Filters（筛选维度）

- 日期：`dt`

## 事件范围

- 试穿：`c_tryon`
- 加购：`c_add_cart`

## UV 去重口径（visitor_id）

`visitor_id = COALESCE(NULLIF(prop_user_id,''), NULLIF(prop_device_id,''))`

## 表结构

| 字段 | 类型 | 说明 |
|------|------|------|
| dt | DATE | 日期（多伦多时间） |
| tryon_uv | INT64 | 试穿用户数（UV，visitor_id 去重） |
| add_cart_uv | INT64 | 加购用户数（UV，visitor_id 去重；全量加购UV，不做试穿归因/不要求先试穿） |
| add_cart_rate | NUMERIC | 加购率（add_cart_uv / tryon_uv，四舍五入 4 位；tryon_uv=0 时为 0） |
| update_time | TIMESTAMP | 更新时间（UTC） |

**分区**：`dt`

## 指标口径说明

### 试穿 UV（tryon_uv）

- **事件范围**：`c_tryon`
- **去重**：`visitor_id`（先按 action_event_id 去重事件后，再按 visitor_id 统计 UV）

### 加购 UV（add_cart_uv）

- **事件范围**：`c_add_cart`
- **去重**：`visitor_id`（先按 action_event_id 去重事件后，再按 visitor_id 统计 UV）
- **口径**：全量加购UV（不做试穿归因/不要求先试穿），因此 `add_cart_rate` 可能 > 1（口径决定，非异常）。

### 加购率（add_cart_rate）

`add_cart_rate = CASE WHEN tryon_uv=0 THEN 0 ELSE ROUND(add_cart_uv/tryon_uv, 4) END`

## 去重与重复行说明

`dwd_event_log` 可能因 `args_*`（如 `args_post/args_spu`）展开导致同一事件出现多行。

本表在聚合前先做事件去重：

- `action_event_id = COALESCE(raw_event_id, hash_id)`
- 在 `dt × event_name × action_event_id` 粒度下聚合为一行，再进行 UV 统计

## 时区说明

| 字段 | 时区 |
|------|------|
| dt | America/Toronto（多伦多时间） |
| update_time | UTC |

## 更新机制

- **调度周期**：T+1（每日更新）
- **更新方式**：DELETE + INSERT（幂等）
- **增量判断**：由 `jobs/ads/ads_daily/ads_daily.py` 统一识别日期与编排

## 相关文件

- **DDL**：`ddl/ads/ads_daily_tryon_add_cart_conversion.sql`
- **任务脚本**：`jobs/ads/ads_daily_tryon_add_cart_conversion/ads_daily_tryon_add_cart_conversion.py`（函数 `run_ads_daily_tryon_add_cart_conversion`）
- **入口脚本**：`jobs/ads/ads_daily/ads_daily.py`
