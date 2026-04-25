# 专栏追踪明细

## 报表

- 表：`ads_daily_column_performance`
- 粒度：`dt × module × column_id`
- Dashboard：[【AI-Fashion】专栏追踪明细（日×专栏）](https://bi.alvinclub.ca/superset/dashboard/93/)（id=93）

## 字段

| 字段 | 口径 |
|------|------|
| column_exposure_uv | 专栏曝光 UV；事件：`v_star_post_feeds`, `v_brand_post_feeds`, `v_star_post_detail`, `v_brand_post_detail`, `v_magazine_post_detail`；仅统计 `prop_user_id IS NOT NULL AND prop_user_id != ''` 的事件，并按 `COUNT(DISTINCT prop_user_id)` |
| follow_total_count | 关注点击次数；事件：`c_follow`；按 `raw_event_id` 去重 |
| follow_rate | `follow_uv / column_exposure_uv`；其中 `follow_uv` 仅统计 `prop_user_id IS NOT NULL AND prop_user_id != ''` 的 `c_follow` 事件，并按 `COUNT(DISTINCT prop_user_id)` |
| product_click_uv | 商品点击 UV；事件：`c_go_product_detail`, `c_open_sku_popup`, `c_third_party_goods`；仅统计 `product_code` 非空、且 `prop_user_id` 非空的事件；专栏归因只允许使用上报的 `args_post`，经 `community_post.poster_user_id -> kol_rel.user_id` 映射到 `column_id`；按 `COUNT(DISTINCT prop_user_id)` |
| product_click_pv | 商品点击 PV；事件同 `product_click_uv`；仅统计 `product_code` 非空、且 `prop_user_id` 非空的事件；专栏归因只允许使用上报的 `args_post`；按 `raw_event_id` 去重 |
| product_click_penetration_rate | 商品点击渗透率；`product_click_uv / dau_uv`；其中 `dau_uv` 为当日任意已登录事件去重用户数 |
| product_card_exposure_uv | 商品卡片曝光 UV；事件：`v_shop_product`, `v_post_product`, `v_category_product`, `v_creator_home_shop_product`, `v_closet_feeds`, `v_home_feeds`, `v_shop_feeds`, `v_tryon_result_detail`；仅统计 `product_code` 非空、且 `prop_user_id` 非空的事件；专栏归因只允许使用上报的 `args_post`；按 `COUNT(DISTINCT prop_user_id)` |
| product_exposure_click_rate | 商品曝光点击率；`product_click_uv / product_card_exposure_uv` |
| read_post_count | 去重帖子数；仅统计已登录曝光事件，`COUNT(DISTINCT CONCAT(prop_user_id, '-', post_code))` |
| avg_read_post_count_per_user | `read_post_count / column_exposure_uv` |
| post_total_count | 帖子总数；`dws_column_post_daily` 在多伦多时间 0 点后回写昨日的在线帖子存量快照；在线帖子定义为 `v3_decom.community_post.status = 1` |
| tryon_post_count | 试穿帖子数；`v3_decom.common_ext_ai_task.task_type IN ('TRY_ON','OUTFIT')` 且可映射专栏后的当日去重帖子数 |
| tryon_click_uv | 试穿点击人数；`v3_decom.common_ext_ai_task.task_type IN ('TRY_ON','OUTFIT')` 的当日已登录去重用户数 |
| tryon_uv_rate | 试穿率 UV 口径；`tryon_click_uv / column_exposure_uv` |
| ai_tryon_penetration_rate | AI 试穿渗透率（内容维度）；`tryon_post_count / post_total_count` |
| like_post_count | 新增点赞数（帖子口径）；同一专栏中当天获得过点赞的去重帖子数 |
| like_rate | 点赞率；`like_uv / column_exposure_uv`；其中 `like_uv` 为服务端当日已登录点赞去重用户数 |
| new_follow_count | 新增关注数；`v3_decom.community_star_follow` 当天新增关注该专栏的去重用户数 |
| follow_conversion_rate_7d | 近7日关注转化率；同日同专栏 `(今日关注用户 ∩ 近7日点赞用户)` 去重人数 / 近7日点赞去重人数（窗口为 `[dt-6, dt]`） |

补充说明：
- UV 相关指标统一为“已登录用户”口径：
  - 事件侧仅保留 `prop_user_id IS NOT NULL AND prop_user_id != ''`
  - 人数类统一按 `COUNT(DISTINCT prop_user_id)` 统计
- 商品相关事件新增口径统一要求 `product_code` 非空
- 商品相关事件的专栏归因只允许使用上报的 `args_post`
  - `args_post -> dim_post.post_code -> dim_post.column_id -> dim_column.column_id`
  - 不再使用事件自带 `args_star / args_magazine / args_brand` 做商品事件专栏归因
  - `args_post` 无法映射到帖子或专栏的商品事件，不进入该专栏明细表
- `follow_total_count`、`like_post_count`、`tryon_post_count` 仍保留原事件/帖子次数口径，不属于 UV 人数指标
- `column_id → column_name` 映射：`dim_column`
- `dws_column_post_daily` 只承载帖子在线存量快照，不承载试穿日发生指标
- `dws_column_interaction_daily` 承接专栏曝光、商品事件、试穿、点赞、新增关注和 7 日关注转化分子分母
- `ads_daily_column_performance` 只负责消费 `DWS + DIM` 并计算终层率指标

## 依赖

- 直接消费：`dim_column`, `dws_column_interaction_daily`, `dws_column_post_daily`
- 行为事实来源：`dwd_event_log`, `v3_decom.community_post_like`, `v3_decom.community_star_follow`, `v3_decom.common_ext_ai_task`
- 已落地实体层：`dim_post`, `dim_column`
- 实体脚本：`jobs/dim/dim_post.py`, `jobs/dim/dim_column.py`
- 实体 DDL：`ddl/dim/dim_post.sql`, `ddl/dim/dim_column.sql`
- 行为事实脚本：`jobs/dws/dws_column_interaction_daily.py`
- 行为事实 DDL：`ddl/dws/dws_column_interaction_daily.sql`
- 快照脚本：`jobs/dws/dws_column_post_daily.py`
- 快照 DDL：`ddl/dws/dws_column_post_daily.sql`
- 脚本：`jobs/ads/ads_daily_column_performance.py`
- DDL：`ddl/ads/ads_daily_column_performance.sql`
- 编排：`jobs/dws/dws_column_interaction_daily.py` / `jobs/dws/dws_column_post_daily.py` / `jobs/ads/ads_daily_column_performance.py` 均为独立表级任务
- 当前状态：Batch 1 已完成实体层；Batch 3 已新增 `dws_column_interaction_daily`，并已完成全量初始化、并跑对账与正式换表；当前正式链路已切到 `dim_column + dws_column_interaction_daily + dws_column_post_daily`
