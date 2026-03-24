# 平台整体内容表现

## 报表

- 表：`ads_daily_content_performance`
- 粒度：`dt`
- Dashboard：[【AI-Fashion】平台整体内容表现](https://bi.alvinclub.ca/superset/dashboard/91/)（id=91）

## 字段

| 字段 | 口径 |
|------|------|
| platform_exposure_uv | Home 曝光 UV；事件：`v_home_star`, `v_home_magazine`, `v_home_brand`, `v_home_feeds` |
| avg_browse_content_count_per_user | 帖子详情曝光次数 / 帖子内容曝光 UV；不包含 `v_product_detail` |
| like_total_count | 点赞成功次数（PV）；事件：`c_like` |
| like_rate | 点赞 UV / 帖子内容曝光 UV |
| read_rate | 阅读 UV / 帖子内容曝光 UV；阅读事件：`r_star_post_detail`, `r_magazine_post_detail`, `r_brand_post_detail`, `r_kol_post_detail` |
| follow_total_count | 关注专栏点击次数（PV）；事件：`c_follow`；按 `raw_event_id` 去重且需能识别 `column_id` |
| follow_uv | 关注专栏 UV；`c_follow` 去重访客数，且需能识别 `column_id` |
| dau_uv | 当天日活 UV；任意事件活跃的 `COUNT(DISTINCT visitor_id)` |
| follow_rate | `follow_uv / dau_uv` |
| read_follow_rate | 关注次数 / 专栏曝光次数；历史字段名保留 |
| tryon_total_count | 试穿总次数（PV）；事件：`c_tryon` |
| read_tryon_rate | 试穿 PV / 专栏曝光 PV；历史字段名保留 |

补充说明：
- 帖子内容曝光事件：`v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail`, `v_kol_post_detail`
- 专栏曝光事件：上述明细页 + `v_star_post_feeds`, `v_brand_post_feeds`
- 访客去重：`visitor_id = COALESCE(prop_user_id, prop_device_id)`

## 依赖

- 上游：`dwd_event_log`
- 脚本：`jobs/ads/ads_daily_content_performance.py`
- DDL：`ddl/ads/ads_daily_content_performance.sql`
- 编排：`jobs/ads/ads_daily.py`
