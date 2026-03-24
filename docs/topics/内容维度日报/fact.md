# 内容维度日报

## 报表

- 表：`ads_daily_post_performance`
- 粒度：`dt × post_id × module × column_id`
- Dashboard：[【AI-Fashion】内容维度日报（日×帖子）](https://bi.alvinclub.ca/superset/dashboard/94/)（id=94）

## 字段

| 字段 | 口径 |
|------|------|
| post_exposure_uv | 进入帖子详情页的去重用户数；事件：`v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail` |
| like_total_count | 点赞成功次数（PV）；事件：`c_like`；按 `raw_event_id` 去重 |
| like_rate | 点赞 UV / 帖子曝光 UV |
| read_rate | 阅读 UV / 帖子曝光 UV；阅读事件：`r_star_post_detail`, `r_magazine_post_detail`, `r_brand_post_detail`, `r_kol_post_detail` |
| follow_total_count | 关注成功次数（PV）；事件：`c_follow`；按同一天内帖子详情曝光做 last-touch 归因；按 `raw_event_id` 去重 |
| follow_rate | 归因关注 UV / 帖子曝光 UV |
| tryon_total_count | 从该帖子发起的试穿次数；事件：`c_tryon`；按 `raw_event_id` 去重 |

补充说明：
- 访客去重：`visitor_id = COALESCE(NULLIF(prop_user_id,''), NULLIF(prop_device_id,''))`
- 维度映射：`post_code → post_id/post_name/creator` 用 `v3_decom.community_post`；`column_id → column_name` 用 `v3_decom.kol_rel`

## 依赖

- 上游：`dwd_event_log`, `v3_decom.community_post`, `v3_decom.kol_rel`
- 脚本：`jobs/ads/ads_daily_post_performance.py`
- DDL：`ddl/ads/ads_daily_post_performance.sql`
- 编排：`jobs/ads/ads_daily.py`
