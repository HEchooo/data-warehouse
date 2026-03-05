# 平台整体内容表现日报 (ads_daily_content_performance)

## 报表定义

统计每日平台整体内容表现（每日一行），用于监控曝光、阅读、互动与试穿转化。

## 表结构

| 字段 | 类型 | 说明 |
|------|------|------|
| dt | DATE | 日期（多伦多时间） |
| platform_exposure_uv | INT64 | 平台曝光 UV（Home 曝光 UV） |
| avg_browse_content_count_per_user | NUMERIC | 人均浏览内容数（进入详情） |
| like_total_count | INT64 | 点赞总数（点赞成功次数） |
| like_rate | NUMERIC | 点赞率（点赞 UV / 内容曝光 PV） |
| follow_total_count | INT64 | 关注总数（关注成功次数） |
| read_follow_rate | NUMERIC | 阅读关注率（关注 UV / 专栏阅读 UV） |
| tryon_total_count | INT64 | 上身试穿总次数（开始试穿 PV） |
| read_tryon_rate | NUMERIC | 阅读试穿率（试穿 PV / 专栏阅读 PV） |
| update_time | TIMESTAMP | 更新时间（UTC） |

**分区**: `dt`（按日期分区）

## 指标口径

### 访客去重口径
- **UV 去重键**: `COALESCE(prop_user_id, prop_device_id)`
- **说明**: 兼容登录和未登录用户，优先使用用户 ID。

### 平台曝光 UV (platform_exposure_uv)
- **定义**: Home 曝光 UV 作为平台曝光 UV。
- **事件范围**: `v_home_star`, `v_home_magazine`, `v_home_brand`, `v_home_feeds`

### 人均浏览内容数 (avg_browse_content_count_per_user)
- **定义**: 当天用户平均浏览（进入详情）的内容数量。
- **内容详情事件**: `v_product_detail`, `v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail`, `v_kol_post_detail`
- **计算方式**: `内容详情曝光 PV / 内容详情阅读 UV`

### 点赞总数 (like_total_count)
- **定义**: 点赞成功行为次数。
- **事件范围**: `c_like`
- **计算方式**: `COUNT(c_like)`（PV）

### 点赞率 (like_rate)
- **定义**: 点赞 UV / 内容曝光 PV。
- **计算方式**: `c_like UV / 内容详情曝光 PV`

### 关注总数 (follow_total_count)
- **定义**: 当天关注成功的行为次数（关注某个专栏）。
- **事件范围**: `c_follow`
- **计算方式**: `COUNT(c_follow)`（PV）

### 阅读关注率 (read_follow_rate)
- **定义**: 新增关注 UV / 专栏阅读 UV。
- **专栏阅读事件**: `v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail`, `v_kol_post_detail`
- **计算方式**: `c_follow UV / 专栏阅读 UV`

### 上身试穿总次数 (tryon_total_count)
- **定义**: 当天触发试穿开始的总次数。
- **事件范围**: `c_tryon`
- **计算方式**: `COUNT(c_tryon)`（PV）

### 阅读试穿率 (read_tryon_rate)
- **定义**: 试穿行为 PV / 专栏阅读 PV。
- **计算方式**: `c_tryon PV / 专栏阅读 PV`

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
