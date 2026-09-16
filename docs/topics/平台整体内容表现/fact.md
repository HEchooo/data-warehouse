# 平台整体内容表现

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 指标 | DAU | 日活、日活用户数、活跃用户数 |
| 指标 | 内容曝光 UV | 内容曝光人数 |
| 指标 | 内容曝光 PV | 内容曝光次数 |
| 指标 | product_click_rate | 商品点击率 |
| 指标 | 试穿渗透率 | 试穿率 |
| 指标 | follow_rate | 关注率 |

## 适用范围

- 业务线：`Alvin's Club App`
- 适合：查询 App 平台整体内容曝光、点击、试穿、互动和 DAU 日趋势。
- 不适合：帖子明细应使用[内容维度日报](../内容维度日报/fact.md)；首页模块明细应使用[首页模块曝光与点击明细](../首页模块曝光与点击明细/fact.md)。
- 需要澄清：需要确认平台、日期范围和统计主体是用户还是事件。

## 报表

### ads_daily_content_performance

- 表：`my-project-8584-jetonai.decom.ads_daily_content_performance`
- 状态：`active`
- 粒度：`dt + platform`
- 时区：`America/Toronto`
- 数据范围：App 各平台的内容曝光、点击、试穿和互动日汇总。
- 必要过滤：按目标 `platform` 过滤。
- 不包含：帖子、专栏或模块明细。
- Dashboard：[【AI-Fashion】平台整体内容表现](https://bi.alvinclub.ca/superset/dashboard/91/)（id=91）

## 字段

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 平台 | `platform` | 设备 | `prop_url` 非空记为 `H5`，`LOWER(prop_os) = 'ios'` 记为 `iOS`，`LOWER(prop_os) IN ('android', 'harmony')` 记为 `Android`，其余为 `unknown`；服务端行为按同日同用户最近一次登录事件的平台归因，未命中记 `unknown` | 无 | 跨平台汇总 UV 必须回明细重算，不可相加 |
| Home 曝光 UV | `platform_exposure_uv` | 已登录用户 | 事件 `v_home_star`, `v_home_magazine`, `v_home_brand`, `v_home_feeds` 的 `COUNT(DISTINCT prop_user_id)` | 目标 `platform`；仅已登录事件 | 不可跨天或跨平台求和 |
| 帖子内容曝光 UV | `content_exposure_uv` | 已登录用户 | 事件 `v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail`, `v_kol_post_detail` 的 `COUNT(DISTINCT prop_user_id)` | 目标 `platform`；仅已登录事件 | 不可跨天或跨平台求和 |
| 帖子内容曝光 PV | `content_exposure_pv` | 事件 | 事件同 `content_exposure_uv`，按 `hash_id` 去重 | 目标 `platform`；仅已登录事件 | 可跨天、跨平台求和 |
| 商详页曝光 UV | `product_detail_exposure_uv` | 已登录用户 | 事件 `v_product_detail` 的 `COUNT(DISTINCT prop_user_id)` | 目标 `platform`；仅已登录事件 | 不可跨天或跨平台求和 |
| 商详页曝光 PV | `product_detail_exposure_pv` | 事件 | 事件 `v_product_detail`，按 `COALESCE(raw_event_id, hash_id)` 去重 | 目标 `platform`；仅已登录事件 | 可跨天、跨平台求和 |
| 商品点击 UV | `product_click_uv` | 已登录用户 | 事件 `c_go_product_detail`、`c_open_sku_popup`、`c_third_party_goods` 的 `COUNT(DISTINCT prop_user_id)` | 目标 `platform`；仅已登录事件 | 不可跨天或跨平台求和 |
| 商品点击 PV | `product_click_pv` | 事件 | 事件同 `product_click_uv`，按 `COALESCE(raw_event_id, hash_id)` 去重 | 目标 `platform`；仅已登录事件 | 可跨天、跨平台求和 |
| 人均浏览内容数 | `avg_browse_content_count_per_user` | 已登录用户 | `content_exposure_pv / content_exposure_uv`；分母为 0 时返回 `0` | 目标 `platform`；仅已登录事件 | 不可求和或平均；跨天、跨平台须用汇总分子分母重算 |
| 点赞次数 | `like_total_count` | 点赞记录 | 服务端表 `v3_decom.community_post_like`，按 `gmt_create` 切天后计数 | 目标 `platform` | 可跨天、跨平台求和 |
| 点赞 UV | `like_uv` | 已登录用户 | `v3_decom.community_post_like` 按 `user_id` 去重 | 目标 `platform` | 不可跨天或跨平台求和 |
| 点赞率 | `like_rate` | 已登录用户 | `like_uv / content_exposure_uv`；分母为 0 时返回 `0` | 目标 `platform` | 不可 `AVG`；跨天、跨平台须用汇总分子分母重算 |
| 阅读 UV | `read_uv` | 已登录用户 | 事件 `r_star_post_detail`, `r_magazine_post_detail`, `r_brand_post_detail`, `r_kol_post_detail` 的 `COUNT(DISTINCT prop_user_id)` | 目标 `platform`；仅已登录事件 | 不可跨天或跨平台求和 |
| 阅读率 | `read_rate` | 已登录用户 | `read_uv / content_exposure_uv`；字段仍在表中，看板已下线 | 目标 `platform` | 不可 `AVG`；跨天、跨平台须用汇总分子分母重算 |
| 新增关注次数 | `follow_total_count` | 关注记录 | 服务端表 `v3_decom.community_star_follow`，按 `gmt_create` 切天后计数 | 目标 `platform` | 可跨天、跨平台求和 |
| 关注专栏个数 | `follow_column_count` | 专栏 | 当天被新增关注过的去重专栏数，按 `star_user_id` 去重 | 目标 `platform` | 不可跨天或跨平台求和 |
| 关注专栏 UV | `follow_uv` | 已登录用户 | `v3_decom.community_star_follow` 按 `user_id` 去重 | 目标 `platform` | 不可跨天或跨平台求和 |
| 日活 UV | `dau_uv` | 已登录用户 | 当天 `COUNT(DISTINCT prop_user_id)`，排除 `send_push`、`send_email` 这类被动触达事件 | 目标 `platform`；仅已登录事件 | 不可跨天或跨平台求和 |
| 商品曝光点击率 | `product_click_rate` | 已登录用户 | `product_click_uv / dau_uv`；分母为 0 时返回 `0` | 目标 `platform` | 不可 `AVG`；跨天、跨平台须用汇总分子分母重算 |
| 关注率 | `follow_rate` | 已登录用户 | `follow_uv / dau_uv`；分母为 0 时返回 `0`；不与 `column_follow_rate` 混用 | 目标 `platform` | 不可 `AVG`；跨天、跨平台须用汇总分子分母重算 |
| 试穿 UV | `tryon_uv` | 已登录用户 | 服务端表 `v3_decom.common_ext_ai_task`，`task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')`，按试穿用户去重；不限定 `request_param.postId` | 目标 `platform` | 不可跨天或跨平台求和 |
| 试穿渗透率 | `tryon_penetration_rate` | 已登录用户 | `tryon_uv / dau_uv`；分母为 0 时返回 `0` | 目标 `platform` | 不可 `AVG`；跨天、跨平台须用汇总分子分母重算 |
| 试穿总次数 | `tryon_total_count` | 试穿任务 | `v3_decom.common_ext_ai_task` 中同 `task_type` 的任务行计数 | 目标 `platform` | 可跨天、跨平台求和 |
| 关注转化率 | `follow_conversion_rate` | 已登录用户 | `follow_uv / like_uv`；分母为 0 时返回 `0` | 目标 `platform` | 不可 `AVG`；跨天、跨平台须用汇总分子分母重算 |
| 专栏曝光 PV | `column_exposure_pv` | 事件 | 事件 `v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail`, `v_kol_post_detail`, `v_star_post_feeds`, `v_brand_post_feeds`，按 `hash_id` 去重 | 目标 `platform`；仅已登录事件 | 可跨天、跨平台求和 |
| 专栏曝光 UV | `column_follow_exposure_uv` | 已登录用户 | 专栏曝光事件的 `COUNT(DISTINCT prop_user_id)`；`column_follow_rate` 的分母，看板已下线 | 目标 `platform`；仅已登录事件 | 不可跨天或跨平台求和 |
| 专栏关注率 | `column_follow_rate` | 已登录用户 | `follow_uv / column_follow_exposure_uv`；字段仍在表中，看板已下线 | 目标 `platform` | 不可 `AVG`；跨天、跨平台须用汇总分子分母重算 |
| 阅读关注率 | `read_follow_rate` | 已登录用户 / 事件 | 关注次数 / 专栏曝光 PV；历史字段名保留，看板已下线 | 目标 `platform` | 不可 `AVG`；跨天、跨平台须用汇总分子分母重算 |
| 阅读试穿率 | `read_tryon_rate` | 试穿任务 / 事件 | `tryon_total_count / column_exposure_pv`；字段仍在表中，看板已下线 | 目标 `platform` | 不可 `AVG`；跨天、跨平台须用汇总分子分母重算 |

补充说明：
- 事件侧指标只纳入 `prop_user_id IS NOT NULL AND prop_user_id != ''` 的事件；UV 统一按 `COUNT(DISTINCT prop_user_id)`，PV 保持各字段原有去重键
- 该表升级为 `dt + platform` 后，跨平台汇总时 UV / 去重类指标与旧版 `dt` 总览不完全一致；率类指标应以分子分母重新聚合，不直接 `AVG`

## 依赖

- 上游：`dwd_event_log`, `v3_decom.community_post_like`, `v3_decom.community_star_follow`, `v3_decom.common_ext_ai_task`
- 中层事实：`dws_platform_content_daily`
- 中层脚本：`jobs/dws/dws_platform_content_daily.py`
- 中层 DDL：`ddl/dws/dws_platform_content_daily.sql`
- 终层脚本：`jobs/ads/ads_daily_content_performance.py`
- DDL：`ddl/ads/ads_daily_content_performance.sql`
- 编排：`env/etl_config.json`（`dws_platform_content_daily`、`ads_daily_content_performance`）
