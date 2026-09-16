# 模块追踪明细

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | 模块追踪明细 | 模块追踪明细（月×模块） |
| 指标 | read_post_count | 阅读帖子数 |
| 指标 | 试穿渗透率 | 试穿率 |

## 适用范围

- 业务线：`Alvin's Club App`
- 适合：按内容模块查看自然月曝光、阅读与试穿表现。
- 不适合：首页模块按日曝光点击应使用[首页模块曝光与点击明细](../首页模块曝光与点击明细/fact.md)；专栏月汇总应使用[专栏追踪明细](../专栏追踪明细/fact.md)。
- 需要澄清：需要确认模块、月份和指标。

## 报表

### ads_monthly_module_performance

- 表：`my-project-8584-jetonai.decom.ads_monthly_module_performance`
- 状态：`active`
- 粒度：`month × module`
- 时区：`America/New_York`
- 数据范围：App 内容模块的自然月表现。
- 必要过滤：按目标 `module` 过滤。
- 不包含：首页按日模块点击明细和专栏维度。
- Dashboard：[【AI-Fashion】模块追踪明细（月×模块）](https://bi.alvinclub.ca/superset/dashboard/105/)（id=105）
- Superset：物理 dataset `ads_monthly_module_performance`（id=234），chart `模块追踪明细（月×模块）-明细表`（id=583）
- 时间口径：`month` 为 `America/New_York` 自然月，每月存为当月 1 号

## 字段

指标口径与[专栏追踪明细](../专栏追踪明细/fact.md)一致，维度从 `month × module × column_id` 收敛为 `month × module`；UV / 人数类指标在模块粒度直接按自然月去重，不从专栏月报汇总。本主题不产出 `follow_conversion_rate_7d`。

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 模块 | `module` | 内容模块 | 来自专栏维表映射，当前只覆盖可映射专栏的 `star`、`magazine`、`brand` | 按目标模块过滤 | 不聚合 |
| 模块曝光 UV | `column_exposure_uv` | 已登录用户 | 事件 `v_star_post_feeds`, `v_brand_post_feeds`, `v_star_post_detail`, `v_brand_post_detail`, `v_magazine_post_detail` 按自然月 `COUNT(DISTINCT prop_user_id)` | `prop_user_id IS NOT NULL AND prop_user_id != ''` | 不可跨月或跨模块求和 |
| 新增关注次数 | `follow_total_count` | 关注记录 | 服务端表 `v3_decom.community_star_follow`，按 `gmt_create` 切月后在模块维度聚合 | 无 | 可跨月、跨模块求和 |
| 关注率 | `follow_rate` | 已登录用户 | `follow_uv / column_exposure_uv`；`follow_uv` 为服务端新增关注去重 `user_id`；分母为 0 时返回 `0` | 无 | 不可求和或平均；跨月或跨模块须用相应粒度的去重分子、分母重算 |
| 商品点击 UV | `product_click_uv` | 已登录用户 | 事件 `c_go_product_detail`, `c_open_sku_popup`, `c_third_party_goods` 按自然月 `COUNT(DISTINCT prop_user_id)`；模块归因只允许用上报的 `args_post`，经 `args_post -> dim_post.post_code -> dim_post.column_id -> dim_column.module` 映射 | `product_code` 非空且 `prop_user_id` 非空 | 不可跨月或跨模块求和 |
| 商品点击 PV | `product_click_pv` | 事件 | 事件同 `product_click_uv`，按 `raw_event_id` 去重后在自然月内按模块聚合 | 同上 | 可跨月、跨模块求和 |
| 商品点击渗透率 | `product_click_penetration_rate` | 已登录用户 | `product_click_uv / mau_uv`；`mau_uv` 为当月任意已登录事件去重用户数，排除 `send_push`、`send_email` 等被动触达事件；分母为 0 时返回 `0` | 无 | 不可求和或平均；跨月或跨模块须用相应粒度的去重分子、分母重算 |
| 商品卡片曝光 UV | `product_card_exposure_uv` | 已登录用户 | 事件 `v_shop_product`, `v_post_product`, `v_category_product`, `v_creator_home_shop_product`, `v_closet_feeds`, `v_home_feeds`, `v_shop_feeds`, `v_tryon_result_detail` 按自然月 `COUNT(DISTINCT prop_user_id)`；归因同 `product_click_uv` | `product_code` 非空且 `prop_user_id` 非空 | 不可跨月或跨模块求和 |
| 商品曝光点击率 | `product_exposure_click_rate` | 已登录用户 | `product_click_uv / product_card_exposure_uv`；分母为 0 时返回 `0` | 无 | 不可求和或平均；跨月或跨模块须用相应粒度的去重分子、分母重算 |
| 去重阅读帖子数 | `read_post_count` | 用户 × 帖子 | 已登录曝光事件按自然月 `COUNT(DISTINCT CONCAT(prop_user_id, '-', post_code))` 后按模块聚合 | `prop_user_id IS NOT NULL AND prop_user_id != ''` | 不可跨月或跨模块求和 |
| 人均阅读帖子数 | `avg_read_post_count_per_user` | 已登录用户 | `read_post_count / column_exposure_uv`；分母为 0 时返回 `0` | 无 | 不可求和或平均；跨月或跨模块须用相应粒度的去重分子、分母重算 |
| 帖子总数 | `post_total_count` | 帖子 | 该月最后一个已落地快照日的在线帖子存量，按 `module` 汇总；当前月未结束时取该月最新快照日 | 无 | 存量快照，不可跨月求和 |
| 试穿帖子数 | `tryon_post_count` | 帖子 | `v3_decom.common_ext_ai_task.task_type IN ('TRY_ON','OUTFIT')` 且可映射模块后的当月去重帖子数 | 无 | 不可跨月或跨模块求和 |
| 试穿点击人数 | `tryon_click_uv` | 已登录用户 | 同 `task_type` 的当月去重用户数 | 无 | 不可跨月或跨模块求和 |
| 试穿率 | `tryon_uv_rate` | 已登录用户 | `tryon_click_uv / column_exposure_uv`；分母为 0 时返回 `0` | 无 | 不可求和或平均；跨月或跨模块须用相应粒度的去重分子、分母重算 |
| AI 试穿渗透率 | `ai_tryon_penetration_rate` | 帖子 | `tryon_post_count / post_total_count`；分母为 0 时返回 `0` | 无 | 不可求和或平均；跨月或跨模块须用相应粒度的去重分子、分母重算 |
| 新增点赞帖子数 | `like_post_count` | 帖子 | 同一模块中当月获得过点赞的去重帖子数 | 无 | 不可跨月或跨模块求和 |
| 点赞率 | `like_rate` | 已登录用户 | `like_uv / column_exposure_uv`；`like_uv` 为服务端当月已登录点赞去重用户数；分母为 0 时返回 `0` | 无 | 不可求和或平均；跨月或跨模块须用相应粒度的去重分子、分母重算 |
| 新增关注人数 | `new_follow_count` | 已登录用户 | `v3_decom.community_star_follow` 当月新增关注该模块下专栏的去重用户数 | 无 | 不可跨月或跨模块求和 |
| 点赞关注率 | `like_follow_rate` | 已登录用户 | `follow_like_uv / new_follow_count`；`follow_like_uv` 为同月同模块「真实新增关注用户 ∩ 点赞用户」去重人数 | 无 | 不可求和或平均；跨月或跨模块须用相应粒度的去重分子、分母重算 |

商品事件的模块归因只允许使用上报的 `args_post`；无法映射到帖子或模块的商品事件不进入明细表。`follow_total_count`、`like_post_count`、`tryon_post_count` 是次数 / 帖子数口径，不是 UV 人数指标。

补充说明：
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
