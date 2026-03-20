# 专栏追踪明细（日×创建者×专栏）(ads_daily_column_performance)

## 粒度

- `dt × creator × module × column_id`
- `dt`：使用 `America/Toronto` 时区切天
- `creator`：帖子创建者，取 `v3_decom.community_post.creator` 原值
- `module`：`star` / `brand` / `magazine`
- `column_id`：专栏ID（对应明星/品牌/杂志的 ID；会去掉首尾双引号 `"`，避免出现 `"260..."`）

## Filters（BI 侧筛选字段）

- 日期范围：`dt`
- 创建者：`creator`
- 模块：`module`
- 专栏ID：`column_id`
- 专栏名称：`column_name`

## 字段清单与口径

### 维度字段

- `dt`：日期
- `creator`：帖子创建者（`v3_decom.community_post.creator` 原值；无映射时为空）
- `module`：模块（star/brand/magazine）
- `column_id`：专栏ID
- `column_name`：专栏名称（`v3_decom.kol_rel.nickname`，`status=0`）

### 指标字段

- `column_exposure_uv`（INT64）：专栏曝光UV
  - 事件范围（feed + detail）：`v_star_post_feeds`、`v_brand_post_feeds`、`v_star_post_detail`、`v_brand_post_detail`、`v_magazine_post_detail`
  - 去重口径：`visitor_id = COALESCE(NULLIF(prop_user_id,''), NULLIF(prop_device_id,''))`
  - 统计口径：`COUNT(DISTINCT visitor_id)`，在 `dt × creator × module × column_id` 粒度下分别统计
- `follow_total_count`（INT64）：关注点击次数
  - 事件范围：`c_follow`
  - 计数口径：按 `raw_event_id` 去重后计数（PV 概念）；即使 `visitor_id` 为空也保留在 `follow_total_count`
  - `creator` 归因：`c_follow` 本身无 `creator`，在其自身已有的 `module + column_id` 范围内，按同一天内同一 `visitor_id` 最近一次匹配到的专栏曝光事件归因，再取该曝光对应 `post_code` 的 `creator`
- `follow_rate`（NUMERIC）：关注转化率
  - 分子：`follow_uv`（同一用户同天同创建者同专栏只要发生过一次关注即计为 1）
  - 分母：`column_exposure_uv`
  - 公式：`follow_uv / column_exposure_uv`
- `read_post_count`（INT64）：去重后的帖子数
  - 定义：同一用户同天同创建者同专栏内，同一帖子（`post_code`）多次曝光只算 1
  - 统计口径：`COUNT(DISTINCT CONCAT(visitor_id,'-',post_code))`
- `avg_read_post_count_per_user`（NUMERIC）：人均阅读帖子数
  - 公式：`read_post_count / column_exposure_uv`

## 任务脚本

- DDL：`ddl/ads/ads_daily_column_performance.sql`
- ETL：`jobs/ads/ads_daily_column_performance/ads_daily_column_performance.py`
- 编排：`jobs/ads/ads_daily/ads_daily.py`

## Superset BI 看板

已在 Superset 落地（2026-03-13）：

- Dashboard：`【AI-Fashion】专栏追踪明细（日×专栏）`（dashboard_id=93，已发布，标题沿用历史命名）
- 访问地址：`https://bi.alvinclub.ca/superset/dashboard/93/`
- Dataset：`decom.ads_daily_column_performance`（dataset_id=222，`main_dttm_col=dt`）
- Chart：`专栏追踪明细（日×专栏）-明细表`（chart_id=546，Table/raw）
- Native filters（默认）：
  - `日期范围`：默认 `Last week`
  - `创建者`：列 `creator`（多选，可搜索）
  - `模块`：列 `module`（多选）
  - `专栏ID`：列 `column_id`（多选，可搜索）
  - `专栏名称`：列 `column_name`（多选，可搜索）
- 生成脚本：`.agents/skills/superset/scripts/create_ads_daily_column_performance_dashboard.py`
