# 专栏追踪明细

## 报表

- 表：`ads_daily_column_performance`
- 粒度：`dt × module × column_id`
- Dashboard：[【AI-Fashion】专栏追踪明细（日×专栏）](https://bi.alvinclub.ca/superset/dashboard/93/)（id=93）

## 字段

| 字段 | 口径 |
|------|------|
| column_exposure_uv | 专栏曝光 UV；事件：`v_star_post_feeds`, `v_brand_post_feeds`, `v_star_post_detail`, `v_brand_post_detail`, `v_magazine_post_detail` |
| follow_total_count | 关注点击次数；事件：`c_follow`；按 `raw_event_id` 去重 |
| follow_rate | `follow_uv / column_exposure_uv` |
| read_post_count | 去重帖子数；`COUNT(DISTINCT CONCAT(visitor_id, '-', post_code))` |
| avg_read_post_count_per_user | `read_post_count / column_exposure_uv` |
| post_total_count | 帖子总数；`dws_column_post_daily` 在多伦多时间 0 点后回写昨日的在线帖子存量快照；在线帖子定义为 `v3_decom.community_post.status = 1` |
| tryon_post_count | 试穿帖子数；`v3_decom.common_ext_ai_task.task_type IN ('TRY_ON','OUTFIT')` 且可映射专栏后的当日去重帖子数 |
| tryon_click_uv | 试穿点击人数；`v3_decom.common_ext_ai_task.task_type IN ('TRY_ON','OUTFIT')` 的当日去重用户数 |
| tryon_uv_rate | 试穿率 UV 口径；`tryon_click_uv / column_exposure_uv` |
| ai_tryon_penetration_rate | AI 试穿渗透率（内容维度）；`tryon_post_count / post_total_count` |
| like_post_count | 新增点赞数（帖子口径）；同一专栏中当天获得过点赞的去重帖子数 |
| like_rate | 点赞率；`like_uv / column_exposure_uv` |
| new_follow_count | 新增关注数；`v3_decom.community_star_follow` 当天新增关注该专栏的去重用户数 |
| follow_conversion_rate_7d | 近7日关注转化率；同日同专栏 `(今日关注用户 ∩ 近7日点赞用户)` 去重人数 / 近7日点赞去重人数（窗口为 `[dt-6, dt]`） |

补充说明：
- 人数类统一口径：
  - 若同日同 `device_id` 仅关联一个 `user_id`，则游客态并到该 `user_id`
  - 若同日同 `device_id` 关联多个 `user_id`，人数口径按 `COUNT(DISTINCT user_id)`，不再额外计游客态
- `column_id → column_name` 映射：`v3_decom.kol_rel`（`status = 0`，`kol_type = 2/3/4`）
- `dws_column_post_daily` 只承载帖子在线存量快照，不承载试穿日发生指标
- 试穿相关指标直接在 ADS 当天按 `common_ext_ai_task` 计算

## 依赖

- 上游：`dwd_event_log`, `dws_column_post_daily`, `v3_decom.community_post_like`, `v3_decom.community_star_follow`, `v3_decom.kol_rel`
- 快照脚本：`jobs/dws/dws_column_post_daily.py`
- 快照 DDL：`ddl/dws/dws_column_post_daily.sql`
- 脚本：`jobs/ads/ads_daily_column_performance.py`
- DDL：`ddl/ads/ads_daily_column_performance.sql`
- 编排：`jobs/dws/dws_column_post_daily.py` 独立日调度 -> `jobs/ads/ads_daily.py`
