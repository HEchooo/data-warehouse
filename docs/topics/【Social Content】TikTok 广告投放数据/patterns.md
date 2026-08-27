# 【Social Content】TikTok 广告投放数据 · 踩坑记录

## TikTok 后台列名与 API metric 的对应靠 URL 参数反查

后台报表 URL 的 `columns=` 参数里是内部列名（`stat_cost`、`time_attr_effect_cnt`、`time_attr_deep_convert_cnt_v2`），
和 Business API 的 metric 名完全不同。对应关系只能逐个试，不能凭命名猜：

| 后台列 | URL 列名 | API metric |
|------|------|------|
| 消耗 | `stat_cost` | `spend` |
| 成效数 | `time_attr_effect_cnt` | `result` |
| 合格转化 | `time_attr_deep_convert_cnt_v2` | `secondary_goal_result` |

「合格转化」这一组尤其容易踩坑：URL 里叫 `deep_convert`，但 API 完全不认 `deep_convert` / `total_deep_convert` /
`cost_per_deep_convert` / `deep_convert_rate` 这几个名字，报 `Invalid metric fields`，正确名字是 `secondary_goal_result` 系列。

探测方法：逐个 metric 单独发一次 `/report/integrated/get/`，看返回 `code` 是否为 0。属性指标（`campaign_name`、`ad_name` 等）
不能单独请求，会报 `All requested metrics are Attribute Metrics`，必须搭配至少一个性能指标。

## `-` 是「不适用」，不是 0

广告组没配置深度转化目标时，`secondary_goal_result` 这类指标返回字符串 `"-"` 而不是 `"0"`。
落库统一转空值，不能补 0：补 0 会让「合格转化率」在汇总时把分母算进去，得到假的 0%。

## 报表接口拿不到店铺，要另外关联 `/ad/get/`

`/report/integrated/get/` 没有任何店铺相关的 dimension 或 metric。店铺只能从 `/ad/get/` 的
`showcase_products[].store_id` 取，`filtering.ad_ids` 单次上限 100 个，要分批。

`objective_type = VIDEO_VIEWS` 的 Smart+ 广告在 campaign / adgroup / ad 三层都查不到店铺 —— 这不是接口漏返回，
而是这类广告本身不是带货广告，没有店铺概念。排查时不要往「权限不足」或「字段名不对」方向找。

## 广告主时区是 `Etc/GMT+5`，不是 `America/Toronto`

`/advertiser/info/` 同时返回 `timezone`（`Etc/GMT+5`）和 `display_timezone`（`America/Toronto`）。
报表的 `stat_time_day` 按 `timezone` 切日，是固定 UTC-5 **不含夏令时**的自然日。

拉数窗口必须用同一个时区算「今天」，否则夏令时期间会算错一天，表现为当天分区拉不到或多拉一天。

**这个时区改不了，不要再试**（`2026-08-27` 已实测）：

- 给 `/report/integrated/get/` 传 `timezone` 或 `time_zone` 参数**不报错，但也不生效**。用 `America/Toronto` / `UTC` / `Asia/Shanghai` 三种传参拉 `2026-08-25` 全量，返回的 `spend=15.00`、`impressions=351` 与不传参数完全一致 —— 参数被静默忽略。静默忽略比报错更危险：看起来「设置成功了」，实际口径没变。
- `/advertiser/update/` 返回 `40001 advertiser does not grant you /advertiser/update/:POST permission`；即便有权限，TikTok 广告账户的时区也是开户时设定、之后不可修改的。

结论：下游要对齐美东自然日只能自己在 SQL 里平移，不要指望在采集端解决。

## 率值不能存了就直接汇总

API 返回的 `cpc` / `ctr` / `conversion_rate` 都是单个广告单天的值。多行相加或求平均都是错的。
所以 `DWS` 只固化可加的分子分母，`ADS` 虽然也落派生列但只供明细行展示，
Superset 汇总 chart 一律用 `SAFE_DIVIDE(SUM(分子), SUM(分母))` 现算。

用 `2026-08-19 ~ 2026-08-26` 全量逐行比对过：重算值与 API 原始返回在 0.02 容差内 0 差异，
包括 `result_rate = 成效数 / 展示量`、`conversion_rate = 转化量 / 点击量` 这两个不那么直觉的口径。

## `etl_run.py` 不支持 `--help`

`etl_run.py` 没有 `--help` 处理分支，传任何参数都会直接开始跑整条生产 ETL 链路。
想看入口清单要读 `env/etl_config.json`，不要用 `--help` 试探：

```bash
python3 -c "import json;[print(t['table_name'], t['script']) for t in json.load(open('env/etl_config.json'))['tasks']]"
```
