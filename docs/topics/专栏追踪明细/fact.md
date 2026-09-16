# 专栏追踪明细

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | 专栏追踪明细 | 专栏追踪明细（日×专栏）、专栏追踪明细（月×专栏） |
| 指标 | 专栏曝光 UV | 专栏曝光人数 |
| 指标 | read_post_count | 帖子阅读数 |
| 指标 | 试穿渗透率 | 试穿率 |

## 适用范围

- 业务线：`Alvin's Club App`
- 适合：按专栏查看日或月曝光、阅读和试穿表现。
- 不适合：帖子明细应使用[内容维度日报](../内容维度日报/fact.md)；模块月汇总应使用[模块追踪明细](../模块追踪明细/fact.md)。
- 需要澄清：需要确认日报或月报、专栏、模块和日期范围。

## 报表

### 日报

- 表：`my-project-8584-jetonai.decom.ads_daily_column_performance`
- 状态：`active`
- 粒度：`dt × module × column_id`
- 时区：`America/Toronto`
- 数据范围：专栏按日、内容模块的曝光、阅读和试穿表现。
- 必要过滤：按目标 `column_id` 和 `module` 过滤。
- 不包含：月汇总行。
- Dashboard：[【AI-Fashion】专栏追踪明细（日×专栏）](https://bi.alvinclub.ca/superset/dashboard/93/)（id=93）

### 月报

- 表：`my-project-8584-jetonai.decom.ads_monthly_column_performance`
- 状态：`active`
- 粒度：`month × module × column_id`
- 时区：`America/New_York`
- 数据范围：专栏自然月、内容模块的曝光、阅读和试穿表现。
- 必要过滤：按目标 `column_id` 和 `module` 过滤。
- 不包含：日明细行。
- Dashboard：[【AI-Fashion】专栏追踪明细（月×专栏）](https://bi.alvinclub.ca/superset/dashboard/104/)（id=104）
- Superset：物理 dataset `ads_monthly_column_performance`（id=233），chart `专栏追踪明细（月×专栏）-明细表`（id=582）
- 时间口径：`month` 为 `America/New_York` 自然月，每月存为当月 1 号

## 字段

日报与月报共用同一套指标，差异只在时间粒度：月报的 UV / 人数类指标按自然月重新去重，不从日报求和；月报的 `product_click_penetration_rate` 分母为当月 `mau_uv`。

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 专栏与模块 | `column_id` / `column_name` / `module` | 专栏 | `column_id` 经 `dim_column` 映射为 `column_name` 和 `module` | 按目标专栏和模块过滤 | 不聚合 |
| 专栏曝光 UV | `column_exposure_uv` | 已登录用户 | 事件 `v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail`, `v_star_post_feeds`, `v_magazine_post_feeds`, `v_brand_post_feeds` 的 `COUNT(DISTINCT prop_user_id)` | `prop_user_id IS NOT NULL AND prop_user_id != ''` | 不可跨天或跨专栏求和 |
| 新增关注次数 | `follow_total_count` | 关注记录 | 服务端表 `v3_decom.community_star_follow`，按 `gmt_create` 切天后计数 | 无 | 可跨天、跨专栏求和 |
| 关注率 | `follow_rate` | 已登录用户 | `follow_uv / column_exposure_uv`；`follow_uv` 为服务端新增关注去重 `user_id`；分母为 0 时返回 `0` | 无 | 不可求和或平均；跨期或跨维度须用相应粒度的去重分子、分母重算 |
| 商品点击 UV | `product_click_uv` | 已登录用户 | 事件 `c_go_product_detail`, `c_open_sku_popup`, `c_third_party_goods` 的 `COUNT(DISTINCT prop_user_id)`；专栏归因只允许用上报的 `args_post`，经 `args_post -> dim_post.post_code -> dim_post.column_id -> dim_column.column_id` 映射 | `product_code` 非空且 `prop_user_id` 非空 | 不可跨天或跨专栏求和 |
| 商品点击 PV | `product_click_pv` | 事件 | 事件同 `product_click_uv`，按 `raw_event_id` 去重 | 同上 | 可跨天、跨专栏求和 |
| 商品点击渗透率 | `product_click_penetration_rate` | 已登录用户 | `product_click_uv / dau_uv`；`dau_uv` 为当日任意已登录事件去重用户数，排除 `send_push`、`send_email` 等被动触达事件；分母为 0 时返回 `0` | 无 | 不可求和或平均；跨期或跨维度须用相应粒度的去重分子、分母重算 |
| 商品卡片曝光 UV | `product_card_exposure_uv` | 已登录用户 | 事件 `v_shop_product`, `v_post_product`, `v_category_product`, `v_creator_home_shop_product`, `v_closet_feeds`, `v_home_feeds`, `v_shop_feeds`, `v_tryon_result_detail` 的 `COUNT(DISTINCT prop_user_id)`；归因同 `product_click_uv` | `product_code` 非空且 `prop_user_id` 非空 | 不可跨天或跨专栏求和 |
| 商品曝光点击率 | `product_exposure_click_rate` | 已登录用户 | `product_click_uv / product_card_exposure_uv`；分母为 0 时返回 `0` | 无 | 不可求和或平均；跨期或跨维度须用相应粒度的去重分子、分母重算 |
| 去重阅读帖子数 | `read_post_count` | 用户 × 帖子 | 已登录曝光事件的 `COUNT(DISTINCT CONCAT(prop_user_id, '-', post_code))` | `prop_user_id IS NOT NULL AND prop_user_id != ''` | 不可跨天或跨专栏求和 |
| 人均阅读帖子数 | `avg_read_post_count_per_user` | 已登录用户 | `read_post_count / column_exposure_uv`；分母为 0 时返回 `0` | 无 | 不可求和或平均；跨期或跨维度须用相应粒度的去重分子、分母重算 |
| 试穿帖子数 | `tryon_post_count` | 帖子 | `v3_decom.common_ext_ai_task.task_type IN ('TRY_ON','OUTFIT')` 且可映射专栏后的当天去重帖子数 | 无 | 不可跨天或跨专栏求和 |
| 试穿点击人数 | `tryon_click_uv` | 已登录用户 | 同 `task_type` 的当天去重用户数 | 无 | 不可跨天或跨专栏求和 |
| 试穿率 | `tryon_uv_rate` | 已登录用户 | `tryon_click_uv / column_exposure_uv`；分母为 0 时返回 `0` | 无 | 不可求和或平均；跨期或跨维度须用相应粒度的去重分子、分母重算 |
| AI 试穿渗透率 | `ai_tryon_penetration_rate` | 帖子 | `tryon_post_count / post_total_count`；分母为 0 时返回 `0` | 无 | 不可求和或平均；跨期或跨维度须用相应粒度的去重分子、分母重算 |
| 新增点赞帖子数 | `like_post_count` | 帖子 | 同一专栏中当天获得过点赞的去重帖子数 | 无 | 不可跨天或跨专栏求和 |
| 点赞率 | `like_rate` | 已登录用户 | `like_uv / column_exposure_uv`；`like_uv` 为服务端当天已登录点赞去重用户数；分母为 0 时返回 `0` | 无 | 不可求和或平均；跨期或跨维度须用相应粒度的去重分子、分母重算 |
| 新增关注人数 | `new_follow_count` | 已登录用户 | `v3_decom.community_star_follow` 当天新增关注该专栏的去重用户数 | 无 | 不可跨天或跨专栏求和 |
| 帖子总数 | `post_total_count` | 帖子 | `dws_column_post_daily` 在多伦多时间 0 点后回写昨日在线帖子存量快照，在线定义为 `v3_decom.community_post.status = 1`；月报取该月最后一个已落地快照日，当前月未结束时取最新快照日 | 无 | 存量快照，不可跨天或跨月求和 |
| 点赞关注率 | `like_follow_rate` | 已登录用户 | 仅月报产出；`follow_like_uv / new_follow_count`，`follow_like_uv` 为同月同专栏「真实新增关注用户 ∩ 点赞用户」去重人数 | 无 | 不可求和或平均；跨期或跨维度须用相应粒度的去重分子、分母重算 |
| 近 7 日关注转化率 | `follow_conversion_rate_7d` | 已登录用户 | 仅日报产出；同日同专栏「今日关注用户 ∩ 近 7 日点赞用户」去重人数 / 近 7 日点赞去重人数，窗口 `[dt-6, dt]` | 无 | 不可求和或平均；跨期或跨维度须用相应粒度的去重分子、分母重算 |

商品事件的专栏归因只允许使用上报的 `args_post`，不使用事件自带的 `args_star / args_magazine / args_brand`；`args_post` 无法映射到帖子或专栏的商品事件不进入明细表。

补充说明：
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
