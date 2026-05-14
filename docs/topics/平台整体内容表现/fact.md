# 平台整体内容表现

## 报表

- 表：`ads_daily_content_performance`
- 粒度：`dt + platform`
- Dashboard：[【AI-Fashion】平台整体内容表现](https://bi.alvinclub.ca/superset/dashboard/91/)（id=91）

## 字段

| 字段 | 口径 |
|------|------|
| platform | 设备类型；`prop_url` 非空记为 `H5`，`LOWER(prop_os) = 'ios'` 记为 `iOS`，`LOWER(prop_os) IN ('android', 'harmony')` 记为 `Android`，其余为 `unknown` |
| platform_exposure_uv | Home 曝光 UV；事件：`v_home_star`, `v_home_magazine`, `v_home_brand`, `v_home_feeds`；仅统计 `prop_user_id` 非空事件，按 `prop_user_id` 去重 |
| content_exposure_uv | 帖子内容曝光 UV；事件：`v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail`, `v_kol_post_detail`；仅统计 `prop_user_id` 非空事件，按 `prop_user_id` 去重 |
| content_exposure_pv | 帖子内容曝光 PV；事件同 `content_exposure_uv`；仅统计 `prop_user_id` 非空事件；当前按 `hash_id` 去重 |
| product_detail_exposure_uv | 商详页曝光 UV；事件：`v_product_detail`；仅统计 `prop_user_id` 非空事件，按 `prop_user_id` 去重 |
| product_detail_exposure_pv | 商详页曝光 PV；事件：`v_product_detail`；仅统计 `prop_user_id` 非空事件；按 `COALESCE(raw_event_id, hash_id)` 去重 |
| product_click_uv | 商品点击 UV；事件：`c_go_product_detail`、`c_open_sku_popup`、`c_third_party_goods`；仅统计 `prop_user_id` 非空事件，按 `prop_user_id` 去重 |
| product_click_pv | 商品点击 PV；事件同 `product_click_uv`；仅统计 `prop_user_id` 非空事件；按 `COALESCE(raw_event_id, hash_id)` 去重 |
| avg_browse_content_count_per_user | `content_exposure_pv / content_exposure_uv`；分子分母都为已登录用户口径 |
| like_total_count | 点赞成功次数（PV）；服务端表：`v3_decom.community_post_like`；按 `gmt_create` 切天后计数 |
| like_uv | 点赞 UV；服务端表：`v3_decom.community_post_like`；按 `user_id` 去重 |
| like_rate | `like_uv / content_exposure_uv` |
| read_uv | 阅读 UV；阅读事件：`r_star_post_detail`, `r_magazine_post_detail`, `r_brand_post_detail`, `r_kol_post_detail`；仅统计 `prop_user_id` 非空事件，按 `prop_user_id` 去重 |
| read_rate（报表中已删除） | 历史口径为 `read_uv / content_exposure_uv` |
| follow_total_count（报表中已取消） | 新增关注次数（PV）；服务端表：`v3_decom.community_star_follow`；按 `gmt_create` 切天后计数 |
| follow_column_count | 关注专栏个数；当天新增关注过的去重专栏数；服务端表：`v3_decom.community_star_follow`；按 `star_user_id` 去重 |
| follow_uv | 关注专栏 UV；服务端表：`v3_decom.community_star_follow`；按 `user_id` 去重 |
| dau_uv | 当天日活 UV；仅统计 `prop_user_id` 非空事件，按 `COUNT(DISTINCT prop_user_id)` |
| product_click_rate | 商品曝光点击率；`product_click_uv / dau_uv` |
| follow_rate | `follow_uv / dau_uv` |
| tryon_uv | 试穿 UV；服务端表：`v3_decom.common_ext_ai_task`；`task_type IN ('TRY_ON', 'OUTFIT')`；按试穿用户去重 |
| tryon_penetration_rate | 试穿渗透率；`tryon_uv / dau_uv` |
| follow_conversion_rate | 关注转化率；`follow_uv / like_uv` |
| column_follow_rate（报表中已删除） | 历史口径为 `follow_uv / column_follow_exposure_uv` |
| read_follow_rate（报表中已取消） | 关注次数 / 专栏曝光次数；历史字段名保留；当前分母仅统计 `prop_user_id` 非空事件的专栏曝光 PV |
| tryon_total_count | 试穿总次数（PV）；服务端表：`v3_decom.common_ext_ai_task`；`task_type IN ('TRY_ON', 'OUTFIT')`；按任务行计数 |
| column_exposure_pv | 专栏曝光 PV；用于重算曝光试穿率；事件：`v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail`, `v_kol_post_detail`, `v_star_post_feeds`, `v_brand_post_feeds`；仅统计 `prop_user_id` 非空事件；按 `hash_id` 去重 |
| read_tryon_rate（报表中已删除） | 历史口径为 `tryon_total_count / column_exposure_pv` |

补充说明：
- 事件侧指标统一只纳入 `prop_user_id IS NOT NULL AND prop_user_id != ''` 的事件
- `dau_uv` 以及所有以活跃登录用户为分母的率指标，都会排除 `send_push`、`send_email` 这类被动触达事件
- 事件侧 UV 统一按 `COUNT(DISTINCT prop_user_id)` 统计
- 事件侧 PV 保持原字段各自去重键，但同样只统计已登录用户事件
- `follow_rate` 保持 `follow_uv / dau_uv`，不与 `column_follow_rate` 混用
- `点赞 / 关注 / 试穿` 改取服务端事实：`community_post_like`、`community_star_follow`、`common_ext_ai_task`
- 服务端行为的平台维度，按同日同用户最近的登录事件平台归因；未命中记为 `unknown`
- “试穿”统计平台级任务：`task_type IN ('TRY_ON', 'OUTFIT')`，不限定 `request_param.postId`
- 该表升级为 `dt + platform` 后，跨平台汇总时 UV / 去重类指标与旧版 `dt` 总览不完全一致；率类指标应以分子分母重新聚合，不直接 `AVG`

## 依赖

- 上游：`dwd_event_log`, `v3_decom.community_post_like`, `v3_decom.community_star_follow`, `v3_decom.common_ext_ai_task`
- 中层事实：`dws_platform_content_daily`
- 中层脚本：`jobs/dws/dws_platform_content_daily.py`
- 中层 DDL：`ddl/dws/dws_platform_content_daily.sql`
- 终层脚本：`jobs/ads/ads_daily_content_performance.py`
- DDL：`ddl/ads/ads_daily_content_performance.sql`
- 编排：`env/etl_config.json`（`dws_platform_content_daily`、`ads_daily_content_performance`）
- 当前状态：`Batch 4` 已将平台主题事件事实下沉到 `dws_platform_content_daily`，`ADS` 只保留终层展示指标计算
