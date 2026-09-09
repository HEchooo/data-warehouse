# 专栏追踪明细

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | 专栏追踪明细 | 专栏追踪明细（日×专栏）、专栏追踪明细（月×专栏） |
| 指标 | 专栏曝光 UV | 专栏曝光人数 |
| 指标 | read_post_count | 帖子阅读数 |
| 指标 | 试穿渗透率 | 试穿率 |

## 报表

### 日报

- 表：`ads_daily_column_performance`
- 粒度：`dt × module × column_id`
- Dashboard：[【AI-Fashion】专栏追踪明细（日×专栏）](https://bi.alvinclub.ca/superset/dashboard/93/)（id=93）

### 月报

- 表：`ads_monthly_column_performance`
- 粒度：`month × module × column_id`
- Dashboard：[【AI-Fashion】专栏追踪明细（月×专栏）](https://bi.alvinclub.ca/superset/dashboard/104/)（id=104）
- Superset：物理 dataset `ads_monthly_column_performance`（id=233），chart `专栏追踪明细（月×专栏）-明细表`（id=582）
- 时间口径：`month` 为 `America/New_York` 自然月，每月存为当月 1 号

## 字段

| 字段 | 口径 |
|------|------|
| column_exposure_uv | 专栏曝光 UV；事件：`v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail`, `v_star_post_feeds`, `v_magazine_post_feeds`, `v_brand_post_feeds`；仅统计 `prop_user_id IS NOT NULL AND prop_user_id != ''` 的事件，并按 `COUNT(DISTINCT prop_user_id)` |
| follow_total_count | 新增关注次数（PV）；服务端表：`v3_decom.community_star_follow`；按 `gmt_create` 切天/切月后计数 |
| follow_rate | `follow_uv / column_exposure_uv`；其中 `follow_uv` 为服务端新增关注去重用户数（`v3_decom.community_star_follow.user_id`） |
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
| like_follow_rate | 点赞关注率；仅月报产出，`follow_like_uv / new_follow_count`；其中 `follow_like_uv` 为同月同专栏“真实新增关注用户 ∩ 点赞用户”的去重人数 |
| follow_conversion_rate_7d | 近7日关注转化率；同日同专栏 `(今日关注用户 ∩ 近7日点赞用户)` 去重人数 / 近7日点赞去重人数（窗口为 `[dt-6, dt]`） |

月报补充说明：
- 除时间粒度外，其余维度保持与日报一致
- 月报中的 UV / 人数类指标按自然月去重，不从日报简单求和
- 月报 `post_total_count` 取该月最后一个已落地快照日的在线帖子存量；当前月若未结束，则取该月最新快照日
- 月报不包含 `follow_conversion_rate_7d`
- 月报新增 `like_follow_rate`，分母使用服务端真实新增关注人数 `new_follow_count`
- 月报 `product_click_penetration_rate` 使用当月任意已登录事件去重用户数作为分母

补充说明：
- UV 相关指标统一为“已登录用户”口径：
  - 事件侧仅保留 `prop_user_id IS NOT NULL AND prop_user_id != ''`
  - 人数类统一按 `COUNT(DISTINCT prop_user_id)` 统计
- `dau_uv` / `mau_uv` 分母都会排除 `send_push`、`send_email` 这类被动触达事件
- 商品相关事件新增口径统一要求 `product_code` 非空
- 商品相关事件的专栏归因只允许使用上报的 `args_post`
  - `args_post -> dim_post.post_code -> dim_post.column_id -> dim_column.column_id`
  - 不再使用事件自带 `args_star / args_magazine / args_brand` 做商品事件专栏归因
  - `args_post` 无法映射到帖子或专栏的商品事件，不进入该专栏明细表
- `follow_total_count`、`like_post_count`、`tryon_post_count` 仍保留次数口径，不属于 UV 人数指标
- `column_id → column_name` 映射：`dim_column`
- `dws_column_post_daily` 只承载帖子在线存量快照，不承载试穿日发生指标
- `dws_column_interaction_daily` 承接专栏曝光、商品事件、试穿、点赞、关注和 7 日关注转化分子分母
- `ads_daily_column_performance` 只负责消费 `DWS + DIM` 并计算终层率指标
- `dws_column_interaction_monthly` 承接月级去重后的专栏曝光、商品事件、试穿、点赞、关注以及点赞关注交集人数
- `dws_column_post_monthly` 从 `dws_column_post_daily` 衍生每个月最后一个快照日的在线帖子存量
- `ads_monthly_column_performance` 只负责消费月级 `DWS + DIM` 并计算终层率指标

## 依赖

- 日报直接消费：`dim_column`, `dws_column_interaction_daily`, `dws_column_post_daily`
- 月报直接消费：`dim_column`, `dws_column_interaction_monthly`, `dws_column_post_monthly`
- 行为事实来源：`dwd_event_log`, `v3_decom.community_post_like`, `v3_decom.community_star_follow`, `v3_decom.common_ext_ai_task`
- 已落地实体层：`dim_post`, `dim_column`
- 实体脚本：`jobs/dim/dim_post.py`, `jobs/dim/dim_column.py`
- 实体 DDL：`ddl/dim/dim_post.sql`, `ddl/dim/dim_column.sql`
- 日行为事实脚本：`jobs/dws/dws_column_interaction_daily.py`
- 日行为事实 DDL：`ddl/dws/dws_column_interaction_daily.sql`
- 月行为事实脚本：`jobs/dws/dws_column_interaction_monthly.py`
- 月行为事实 DDL：`ddl/dws/dws_column_interaction_monthly.sql`
- 日快照脚本：`jobs/dws/dws_column_post_daily.py`
- 日快照 DDL：`ddl/dws/dws_column_post_daily.sql`
- 月快照脚本：`jobs/dws/dws_column_post_monthly.py`
- 月快照 DDL：`ddl/dws/dws_column_post_monthly.sql`
- 日报脚本：`jobs/ads/ads_daily_column_performance.py`
- 日报 DDL：`ddl/ads/ads_daily_column_performance.sql`
- 月报脚本：`jobs/ads/ads_monthly_column_performance.py`
- 月报 DDL：`ddl/ads/ads_monthly_column_performance.sql`
- 编排：日报与月报链路均为独立表级任务
- 当前状态：Batch 1 已完成实体层；Batch 3 已新增 `dws_column_interaction_daily`，并已完成全量初始化、并跑对账与正式换表；当前正式链路已切到 `dim_column + dws_column_interaction_daily + dws_column_post_daily`
