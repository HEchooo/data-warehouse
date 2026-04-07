# 平台整体内容表现

## 报表

- 表：`ads_daily_content_performance`
- 粒度：`dt + platform`
- Dashboard：[【AI-Fashion】平台整体内容表现](https://bi.alvinclub.ca/superset/dashboard/91/)（id=91）

## 字段

| 字段 | 口径 |
|------|------|
| platform | 设备类型；`prop_url` 非空记为 `H5`，`LOWER(prop_os) = 'ios'` 记为 `iOS`，`LOWER(prop_os) IN ('android', 'harmony')` 记为 `Android`，其余为 `unknown` |
| platform_exposure_uv | Home 曝光 UV；事件：`v_home_star`, `v_home_magazine`, `v_home_brand`, `v_home_feeds` |
| content_exposure_uv | 帖子内容曝光 UV；事件：`v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail`, `v_kol_post_detail` |
| content_exposure_pv | 帖子内容曝光 PV；事件同 `content_exposure_uv`；当前按 `hash_id` 去重 |
| product_detail_exposure_uv | 商详页曝光 UV；事件：`v_product_detail`；按 `visitor_id` 去重 |
| product_detail_exposure_pv | 商详页曝光 PV；事件：`v_product_detail`；按 `COALESCE(raw_event_id, hash_id)` 去重 |
| avg_browse_content_count_per_user | `content_exposure_pv / content_exposure_uv` |
| like_total_count | 点赞成功次数（PV）；事件：`c_like` |
| like_uv | 点赞 UV；事件：`c_like` |
| like_rate | `like_uv / content_exposure_uv` |
| read_uv | 阅读 UV；阅读事件：`r_star_post_detail`, `r_magazine_post_detail`, `r_brand_post_detail`, `r_kol_post_detail` |
| read_rate（报表中已删除） | 历史口径为 `read_uv / content_exposure_uv` |
| follow_total_count（报表中已取消） | 关注专栏点击次数（PV）；事件：`c_follow`；按 `raw_event_id` 去重且需能识别 `column_id` |
| follow_column_count | 关注专栏个数；当天被关注过的去重专栏数；事件：`c_follow`；按 `raw_event_id` 去重后对 `column_id` 去重 |
| follow_uv | 关注专栏 UV；`c_follow` 去重访客数，且需能识别 `column_id` |
| dau_uv | 当天日活 UV；任意事件活跃的 `COUNT(DISTINCT visitor_id)` |
| follow_rate | `follow_uv / dau_uv` |
| tryon_uv | 试穿 UV；事件：`c_styleme`, `c_tryon`；按 `COALESCE(raw_event_id, hash_id)` 去重事件后统计访客 |
| tryon_penetration_rate | 试穿渗透率；`tryon_uv / dau_uv` |
| follow_conversion_rate | 关注转化率；`follow_uv / like_uv` |
| column_follow_rate（报表中已删除） | 历史口径为 `follow_uv / column_follow_exposure_uv` |
| read_follow_rate（报表中已取消） | 关注次数 / 专栏曝光次数；历史字段名保留 |
| tryon_total_count | 试穿总次数（PV）；事件：`c_styleme`, `c_tryon`；按 `COALESCE(raw_event_id, hash_id)` 去重 |
| column_exposure_pv | 专栏曝光 PV；用于重算曝光试穿率；事件：`v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail`, `v_kol_post_detail`, `v_star_post_feeds`, `v_brand_post_feeds` |
| read_tryon_rate（报表中已删除） | 历史口径为 `tryon_total_count / column_exposure_pv` |

补充说明：
- 访客去重：`visitor_id = COALESCE(prop_user_id, prop_device_id)`
- `follow_rate` 保持 `follow_uv / dau_uv`，不与 `column_follow_rate` 混用
- “试穿”统一口径：`c_styleme` 与 `c_tryon`
- 该表升级为 `dt + platform` 后，跨平台汇总时 UV / 去重类指标与旧版 `dt` 总览不完全一致；率类指标应以分子分母重新聚合，不直接 `AVG`

## 依赖

- 上游：`dwd_event_log`
- 脚本：`jobs/ads/ads_daily_content_performance.py`
- DDL：`ddl/ads/ads_daily_content_performance.sql`
- 编排：`jobs/ads/ads_daily.py`
