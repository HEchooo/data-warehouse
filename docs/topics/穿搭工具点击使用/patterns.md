# 穿搭工具点击使用经验

## 不要把汇总表和帖子明细表混着用

- `ads_daily_tryon_confirm` 是汇总层
- `ads_daily_post_tryon_confirm` 才是帖子维度和 dashboard 96 的直接来源

## Batch 5 后先查 DWS，再看终层 ADS

- 汇总异常先查 `dws_tryon_confirm_daily`
- 帖子异常先查 `dws_tryon_confirm_post_daily`
- 如果帖子名称、creator、post_id 异常，再查 `dim_post`

## 日×帖子表新增字段时，也要回填历史分区

- `ads_daily_post_tryon_confirm` 是 dashboard 96 的唯一帖子级数据源
- 只等后续增量跑批，会让默认 `Last week` 混入没有新字段的旧分区
- 只要新增字段会直接影响 BI 筛选或明细展示，就要把历史分区回填一并纳入交付范围

## 看板直观异常时，先检查筛选器和当前数据源

- 排查"数据少了 / 没有某个帖子"时，先确认默认时间范围和原生筛选器
- 同时确认自己查的是帖子明细表，而不是汇总表

## 想把 UV 改成已登录用户时，不要直接在 dashboard 96 的 chart 层硬改

- `ads_daily_post_tryon_confirm` 是 dashboard 96 的唯一 datasource，而且已经是 `dt × post_id` 聚合表
- 这张表没有 `prop_user_id` 明细列，chart 层无法直接写 `COUNT(DISTINCT prop_user_id)`
- 正确做法是先把 ADS 表产出逻辑改成：事件侧过滤 `prop_user_id IS NOT NULL AND prop_user_id != ''`，再按 `COUNT(DISTINCT prop_user_id)` 生成 `click_use_uv`
- `avg_click_use_count_per_user` 这类分母依赖 UV 的指标也要同步收口到已登录用户口径
