# 模块追踪明细

## 报表

### 月报

- 表：`ads_monthly_module_performance`
- 粒度：`month × module`
- Dashboard：[【AI-Fashion】模块追踪明细（月×模块）](https://bi.alvinclub.ca/superset/dashboard/105/)（id=105）
- Superset：物理 dataset `ads_monthly_module_performance`（id=234），chart `模块追踪明细（月×模块）-明细表`（id=583）
- 时间口径：`month` 为 `America/New_York` 自然月，每月存为当月 1 号

## 字段

| 字段 | 口径 |
|------|------|
| column_exposure_uv | 模块曝光 UV；事件：`v_star_post_feeds`, `v_brand_post_feeds`, `v_star_post_detail`, `v_brand_post_detail`, `v_magazine_post_detail`；仅统计 `prop_user_id IS NOT NULL AND prop_user_id != ''` 的事件，并按自然月 `COUNT(DISTINCT prop_user_id)` |
| follow_total_count | 新增关注次数（PV）；服务端表：`v3_decom.community_star_follow`；按 `gmt_create` 切月后在模块维度聚合 |
| follow_rate | `follow_uv / column_exposure_uv`；其中 `follow_uv` 为服务端新增关注去重用户数（`v3_decom.community_star_follow.user_id`） |
| product_click_uv | 商品点击 UV；事件：`c_go_product_detail`, `c_open_sku_popup`, `c_third_party_goods`；仅统计 `product_code` 非空、且 `prop_user_id` 非空的事件；模块归因只允许使用上报的 `args_post`，经 `dim_post -> dim_column` 映射到 `module`；按自然月 `COUNT(DISTINCT prop_user_id)` |
| product_click_pv | 商品点击 PV；事件同 `product_click_uv`；仅统计 `product_code` 非空、且 `prop_user_id` 非空的事件；模块归因只允许使用上报的 `args_post`；按 `raw_event_id` 去重后，在自然月内按模块聚合 |
| product_click_penetration_rate | 商品点击渗透率；`product_click_uv / mau_uv`；其中 `mau_uv` 为当月任意已登录事件去重用户数 |
| product_card_exposure_uv | 商品卡片曝光 UV；事件：`v_shop_product`, `v_post_product`, `v_category_product`, `v_creator_home_shop_product`, `v_closet_feeds`, `v_home_feeds`, `v_shop_feeds`, `v_tryon_result_detail`；仅统计 `product_code` 非空、且 `prop_user_id` 非空的事件；模块归因只允许使用上报的 `args_post`；按自然月 `COUNT(DISTINCT prop_user_id)` |
| product_exposure_click_rate | 商品曝光点击率；`product_click_uv / product_card_exposure_uv` |
| read_post_count | 去重阅读帖子数；仅统计已登录曝光事件，按自然月 `COUNT(DISTINCT CONCAT(prop_user_id, '-', post_code))` 后按模块聚合 |
| avg_read_post_count_per_user | `read_post_count / column_exposure_uv` |
| post_total_count | 帖子总数；取该月最后一个已落地快照日的在线帖子存量，并将专栏快照按 `module` 汇总；当前月若未结束，则取该月最新快照日 |
| tryon_post_count | 试穿帖子数；`v3_decom.common_ext_ai_task.task_type IN ('TRY_ON','OUTFIT')` 且可映射模块后的当月去重帖子数 |
| tryon_click_uv | 试穿点击人数；`v3_decom.common_ext_ai_task.task_type IN ('TRY_ON','OUTFIT')` 的当月去重用户数 |
| tryon_uv_rate | 试穿率 UV 口径；`tryon_click_uv / column_exposure_uv` |
| ai_tryon_penetration_rate | AI 试穿渗透率（内容维度）；`tryon_post_count / post_total_count` |
| like_post_count | 新增点赞数（帖子口径）；同一模块中当月获得过点赞的去重帖子数 |
| like_rate | 点赞率；`like_uv / column_exposure_uv`；其中 `like_uv` 为服务端当月已登录点赞去重用户数 |
| new_follow_count | 新增关注数；`v3_decom.community_star_follow` 当月新增关注该模块下专栏的去重用户数 |
| 点赞关注率 | `follow_like_uv / new_follow_count`；其中 `follow_like_uv` 为同月同模块“真实新增关注用户 ∩ 点赞用户”的去重人数，分母 `new_follow_count` 为服务端真实新增关注去重用户数 |

补充说明：
- 指标口径与 `【AI-Fashion】专栏追踪明细（月×专栏）` 保持一致，仅维度从 `month × module × column_id` 改为 `month × module`
- 月报中的 UV / 人数类指标按自然月在模块粒度直接去重，不从专栏月报简单汇总
- 模块值来源于专栏维表映射，当前主题只覆盖可映射专栏的模块：`star`、`magazine`、`brand`
- 月报不包含 `follow_conversion_rate_7d`
- UV 相关指标统一为“已登录用户”口径：
  - 事件侧仅保留 `prop_user_id IS NOT NULL AND prop_user_id != ''`
  - 人数类统一按 `COUNT(DISTINCT prop_user_id)` 统计
- 商品相关事件新增口径统一要求 `product_code` 非空
- 商品相关事件的模块归因只允许使用上报的 `args_post`
  - `args_post -> dim_post.post_code -> dim_post.column_id -> dim_column.module`
  - `args_post` 无法映射到帖子或模块的商品事件，不进入该模块明细表
- `follow_total_count`、`like_post_count`、`tryon_post_count` 仍保留次数口径，不属于 UV 人数指标
- `点赞关注率` 的分母使用服务端真实新增关注人数 `new_follow_count`
- `dws_module_interaction_monthly` 承接月级去重后的模块曝光、商品事件、试穿、点赞、新增关注以及点赞关注交集人数
- `dws_module_post_monthly` 从 `dws_column_post_daily` 衍生每个月最后一个快照日的模块在线帖子存量
- `ads_monthly_module_performance` 只负责消费月级 `DWS` 并计算终层率指标
- 当前初始化状态：
  - 月级行为事实当前已落地 `2026-02` 至 `2026-04`
  - 月级帖子快照当前仅能落地 `2026-04`，因为 `dws_column_post_daily` 只保留当前可回溯的在线帖子日快照
  - 因此 `2026-02`、`2026-03` 的 `post_total_count` 以及依赖它的 `ai_tryon_penetration_rate` 当前为 0；若后续补齐历史日快照，需要重刷 `dws_module_post_monthly` 与 `ads_monthly_module_performance`

## 依赖

- 月报直接消费：`dws_module_interaction_monthly`, `dws_module_post_monthly`
- 行为事实来源：`dwd_event_log`, `v3_decom.community_post_like`, `v3_decom.community_star_follow`, `v3_decom.common_ext_ai_task`
- 已落地实体层：`dim_post`, `dim_column`
- 实体脚本：`jobs/dim/dim_post.py`, `jobs/dim/dim_column.py`
- 实体 DDL：`ddl/dim/dim_post.sql`, `ddl/dim/dim_column.sql`
- 月行为事实脚本：`jobs/dws/dws_module_interaction_monthly.py`
- 月行为事实 DDL：`ddl/dws/dws_module_interaction_monthly.sql`
- 月快照脚本：`jobs/dws/dws_module_post_monthly.py`
- 月快照 DDL：`ddl/dws/dws_module_post_monthly.sql`
- 月报脚本：`jobs/ads/ads_monthly_module_performance.py`
- 月报 DDL：`ddl/ads/ads_monthly_module_performance.sql`
- 编排：`env/etl_config.json`（`dws_module_interaction_monthly`、`dws_module_post_monthly`、`ads_monthly_module_performance`）
