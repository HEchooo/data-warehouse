# 模块追踪明细经验

## 关注指标已切服务端，不再读 `c_follow`

- 当前 `follow_total_count / follow_uv / new_follow_count` 统一来自 `v3_decom.community_star_follow`
- `follow_rate` 与 `点赞关注率` 的关注相关分子分母都按服务端关注事实计算
- `c_follow` 事件仅保留在历史排查中作为对账参考，不再作为正式口径

## 月×模块不能从月×专栏 ADS 直接聚合

- `ads_monthly_column_performance` 已经是 `month × module × column_id` 粒度的终层表
- 像 `column_exposure_uv`、`product_click_uv`、`tryon_click_uv`、`like_rate` 这类月级去重指标，不能再按模块简单 `SUM` / `AVG`
- 正确做法是直接从原始事件和服务端事实重算 `dws_module_interaction_monthly`，再由 `ads_monthly_module_performance` 计算终层率指标

## 模块主题的帖子月快照可以复用专栏日快照，但行为月事实不能复用专栏月报

- `post_total_count` 是月末时点存量，可以从 `dws_column_post_daily` 取每月最后一个快照日，再按 `module` 汇总成 `dws_module_post_monthly`
- 但曝光、商品点击、试穿、点赞、新增关注这类月内去重指标，必须在模块粒度直接重算，不能从 `dws_column_interaction_monthly` 或 `ads_monthly_column_performance` 二次汇总

## 当前模块主题只覆盖可映射专栏的 3 个模块

- 这条链路的模块来自 `dim_column.module`
- 因此当前只会产出 `star`、`magazine`、`brand`
- `feeds` 不在专栏主题映射链路里，不能为了补齐四模块而在月×模块主题里硬补一行空数据

## 历史月的帖子总数要先确认有没有历史日快照

- `dws_module_post_monthly` 不是从行为事件推导，而是从 `dws_column_post_daily` 取每月最后一个快照日再按模块汇总
- 如果历史月份当时没有落过 `dws_column_post_daily`，就无法从当前在线帖子状态准确反推出当月月末存量
- 这时可以先上线模块月报的行为指标，但要明确说明历史月份的 `post_total_count` 及其派生率指标会暂时为 0
