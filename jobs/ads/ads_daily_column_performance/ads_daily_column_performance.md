# 专栏追踪明细（日×专栏）(ads_daily_column_performance)

## 粒度

- `dt × module × column_id`
- `dt`：使用 `America/Toronto` 时区切天
- `module`：`star` / `brand` / `magazine`
- `column_id`：专栏ID（对应明星/品牌/杂志的 ID）

## Filters（BI 侧筛选字段）

- 日期范围：`dt`
- 模块：`module`
- 专栏ID：`column_id`
- 专栏名称：`column_name`

## 字段清单与口径

### 维度字段

- `dt`：日期
- `module`：模块（star/brand/magazine）
- `column_id`：专栏ID
- `column_name`：专栏名称（`v3_decom.kol_rel.nickname`，`status=0`）

### 指标字段

- `column_exposure_uv`（INT64）：专栏曝光UV
  - 事件范围（feed + detail）：`v_star_post_feeds`、`v_brand_post_feeds`、`v_star_post_detail`、`v_brand_post_detail`、`v_magazine_post_detail`
  - 去重口径：`visitor_id = COALESCE(NULLIF(prop_user_id,''), NULLIF(prop_device_id,''))`
  - 统计口径：`COUNT(DISTINCT visitor_id)`
- `follow_total_count`（INT64）：关注点击次数
  - 事件范围：`c_follow`
  - 计数口径：按 `raw_event_id` 去重后计数（PV 概念）
- `follow_rate`（NUMERIC）：关注转化率
  - 分子：`follow_uv`（同一用户同天同专栏只要发生过一次关注即计为 1）
  - 分母：`column_exposure_uv`
  - 公式：`follow_uv / column_exposure_uv`
- `read_post_count`（INT64）：去重后的帖子数
  - 定义：同一用户同天同专栏内，同一帖子（`post_code`）多次曝光只算 1
  - 统计口径：`COUNT(DISTINCT CONCAT(visitor_id,'-',post_code))`
- `avg_read_post_count_per_user`（NUMERIC）：人均阅读帖子数
  - 公式：`read_post_count / column_exposure_uv`

## 任务脚本

- DDL：`ddl/ads/ads_daily_column_performance.sql`
- ETL：`jobs/ads/ads_daily_column_performance/ads_daily_column_performance.py`
- 编排：`jobs/ads/ads_daily/ads_daily.py`

