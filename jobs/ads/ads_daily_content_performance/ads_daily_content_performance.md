# 平台整体内容表现日报 (ads_daily_content_performance)

## 报表定义

统计每日平台整体内容表现（每日一行），用于监控曝光、互动与试穿转化。

## 表结构

| 字段 | 类型 | 说明 |
|------|------|------|
| dt | DATE | 日期（多伦多时间） |
| platform_exposure_uv | INT64 | 平台曝光 UV（Home 曝光 UV） |
| avg_browse_content_count_per_user | NUMERIC | 人均内容曝光数（进入详情，历史字段名保留） |
| like_total_count | INT64 | 点赞总数（点赞成功次数） |
| like_rate | NUMERIC | 点赞率（点赞 UV / 帖子内容曝光 UV） |
| read_rate | NUMERIC | 完读率（阅读 UV / 帖子内容曝光 UV） |
| follow_total_count | INT64 | 关注总数（关注专栏成功次数） |
| read_follow_rate | NUMERIC | 曝光关注率（关注专栏次数 / 专栏曝光次数，历史字段名保留） |
| tryon_total_count | INT64 | 上身试穿总次数（开始试穿 PV） |
| read_tryon_rate | NUMERIC | 曝光试穿率（试穿 PV / 专栏曝光 PV，历史字段名保留） |
| update_time | TIMESTAMP | 更新时间（UTC） |

**分区**: `dt`（按日期分区）

## 指标口径

### 访客去重口径
- **UV 去重键**: `COALESCE(prop_user_id, prop_device_id)`
- **说明**: 兼容登录和未登录用户，优先使用用户 ID。

### 平台曝光 UV (platform_exposure_uv)
- **定义**: Home 曝光 UV 作为平台曝光 UV。
- **事件范围**: `v_home_star`, `v_home_magazine`, `v_home_brand`, `v_home_feeds`

### 人均内容曝光数 (avg_browse_content_count_per_user)
- **定义**: 当天用户平均产生的帖子详情曝光次数。
- **帖子内容曝光事件**: `v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail`, `v_kol_post_detail`
- **不包含**: `v_product_detail`（商详页曝光）
- **计算方式**: `帖子内容曝光次数 / 帖子内容曝光 UV`
- **说明**: 内容曝光次数按内容粒度统计；同一专栏下不同帖子会分别计数。

### 点赞总数 (like_total_count)
- **定义**: 点赞成功行为次数。
- **事件范围**: `c_like`
- **计算方式**: `COUNT(c_like)`（PV）

### 点赞率 (like_rate)
- **定义**: 点赞 UV / 帖子内容曝光 UV。
- **点赞 UV 口径**: 同一用户当天发生多次点赞，只计 1 个 UV。
- **帖子内容曝光 UV 口径**: 详见「人均内容曝光数」的帖子内容曝光事件范围。
- **计算方式**: `点赞UV(visitor_id 去重) / 帖子内容曝光UV(visitor_id 去重)`

### 完读率 (read_rate)
- **定义**: 阅读 UV / 帖子内容曝光 UV。
- **阅读事件范围**: `r_star_post_detail`, `r_magazine_post_detail`, `r_brand_post_detail`, `r_kol_post_detail`
- **阅读 UV 口径**: 同一用户当天发生多次阅读，只计 1 个 UV。
- **帖子内容曝光 UV 口径**: 详见「人均内容曝光数」的帖子内容曝光事件范围。
- **计算方式**: `阅读UV(visitor_id 去重) / 帖子内容曝光UV(visitor_id 去重)`

### 关注总数 (follow_total_count)
- **定义**: 当天点击关注专栏成功的行为次数。
- **事件范围**: `c_follow`
- **计算方式**: `COUNT(c_follow)`（PV，按 `raw_event_id` 去重，且需能识别专栏 `column_id`）

### 曝光关注率 (read_follow_rate)
- **定义**: 点击关注专栏次数 / 专栏曝光次数。
- **说明**: 字段名 `read_follow_rate` 为历史命名，当前语义按“曝光关注率”解释。这里统一使用“次数”表述，不再用 `PV/UV`，避免误解成用户口径或专栏实体去重口径。
- **专栏曝光次数口径**: 专栏曝光事件按 `hash_id` 去重计数，且需能识别专栏 `column_id`。
- **点击关注次数口径**: `c_follow` 按 `raw_event_id` 去重计数，且需能识别专栏 `column_id`。
- **注意**: 这里统计的是“专栏曝光事件次数”，不是“曝光的专栏数”；同理，分子统计的是“点击关注事件次数”，不是关注用户数。
- **专栏曝光事件**:
  - 明细页曝光：`v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail`, `v_kol_post_detail`
  - Feed 曝光（同属专栏曝光）：`v_star_post_feeds`, `v_brand_post_feeds`
  - 说明：`v_home_feeds` 属于 Home 曝光，不计入专栏曝光。
- **计算方式**: `关注专栏次数(follow_total_count) / 专栏曝光次数(column_exposure_pv)`

### 上身试穿总次数 (tryon_total_count)
- **定义**: 当天触发试穿开始的总次数。
- **事件范围**: `c_tryon`
- **计算方式**: `COUNT(c_tryon)`（PV）

### 曝光试穿率 (read_tryon_rate)
- **定义**: 试穿 PV / 专栏曝光 PV。
- **说明**: 字段名 `read_tryon_rate` 为历史命名，当前语义按”曝光试穿率”解释。
- **专栏曝光 PV 口径**: 以专栏曝光事件的明细曝光行为计数（按 `hash_id` 去重）。
- **计算方式**: `试穿PV(COUNT(c_tryon)) / 专栏曝光PV(专栏曝光事件 COUNT(hash_id 去重))`

## 数据来源

```
ads_daily_content_performance
    └── dwd_event_log
```

## 时区说明

| 字段 | 时区 |
|------|------|
| dt | America/Toronto（多伦多时间） |
| update_time | UTC |

## 更新机制

- **调度周期**: T+1（每日更新）
- **更新方式**: DELETE + INSERT（幂等）
- **增量判断**: `dws_device_daily.update_time > ads_daily_content_performance.update_time`

## 相关文件

- **DDL**: `ddl/ads/ads_daily_content_performance.sql`
- **任务脚本**: `jobs/ads/ads_daily_content_performance/ads_daily_content_performance.py`（函数 `run_ads_daily_content_performance`）
- **入口脚本**: `jobs/ads/ads_daily/ads_daily.py`
