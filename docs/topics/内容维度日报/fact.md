# 内容维度日报

## 报表

- 表：`ads_daily_post_performance`
- 粒度：`dt × post_id × module × column_id`
- Dashboard：[【AI-Fashion】内容维度日报（日×帖子）](https://bi.alvinclub.ca/superset/dashboard/94/)（id=94）

## 字段

| 字段 | 口径 |
|------|------|
| post_exposure_uv | 进入帖子详情页的去重已登录用户数；事件：`v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail`；仅统计 `prop_user_id` 非空事件，按 `COUNT(DISTINCT prop_user_id)` |
| like_total_count | 点赞成功次数（PV）；服务端表：`v3_decom.community_post_like`；按 `gmt_create` 切天后计数，并通过 `community_post` 映射回 `post_code / column_id` |
| like_rate | 点赞已登录用户数 / 帖子曝光已登录用户数 |
| read_rate | 阅读已登录用户数 / 帖子曝光已登录用户数；阅读事件：`r_star_post_detail`, `r_magazine_post_detail`, `r_brand_post_detail`, `r_kol_post_detail`；仅统计 `prop_user_id` 非空事件，按 `COUNT(DISTINCT prop_user_id)` |
| follow_total_count | 关注成功次数（PV）；服务端表：`v3_decom.community_star_follow`；按 `gmt_create` 切天后计数，并在同一天内按“同用户 + 同专栏 + 关注前最后一次帖子详情曝光”做 last-touch 归因到帖子 |
| follow_rate | 归因关注已登录用户数 / 帖子曝光已登录用户数 |
| tryon_total_count | 从该帖子发起的试穿次数；服务端表：`v3_decom.common_ext_ai_task`；`task_type IN ('TRY_ON', 'STYLE_ME')` 且 `request_param.postId` 非空；按任务行计数 |

补充说明：
- 事件侧 UV 统一只统计 `prop_user_id IS NOT NULL AND prop_user_id != ''` 的已登录用户
- 事件侧 UV 统一按 `COUNT(DISTINCT prop_user_id)` 统计
- `点赞 / 试穿` 的帖子归因直接通过 `v3_decom.community_post` 完成
- `关注` 的服务端事实没有帖子主键，帖子级口径仍需依赖同日详情曝光做 last-touch
- 维度映射：`post_code → post_id/post_name/creator` 用 `v3_decom.community_post`；`column_id → column_name` 用 `v3_decom.kol_rel`

## 依赖

- 上游：`dwd_event_log`, `v3_decom.community_post`, `v3_decom.community_post_like`, `v3_decom.community_star_follow`, `v3_decom.common_ext_ai_task`, `v3_decom.kol_rel`
- 已落地实体层：`dim_post`, `dim_column`
- 实体脚本：`jobs/dim/dim_post.py`, `jobs/dim/dim_column.py`
- 实体 DDL：`ddl/dim/dim_post.sql`, `ddl/dim/dim_column.sql`
- 脚本：`jobs/ads/ads_daily_post_performance.py`
- DDL：`ddl/ads/ads_daily_post_performance.sql`
- 编排：`jobs/ads/ads_daily.py`
- 当前状态：Batch 1 已落地统一帖子 / 专栏实体层；本表现阶段仍直连源表，后续批次切换时默认改为通过 `dim_post / dim_column` 补齐维度
