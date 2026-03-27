# 投资人报表【核心业务报表】

## 报表

- 表：`ads_daily_investor`
- 粒度：`dt`
- Dashboard：[投资人报表【核心业务报表】](https://bi.alvinclub.ca/superset/dashboard/101/)（id=101）

### `dws_appsflyer_download_daily`

- 粒度：`dt`
- 说明：投资人报表 AppsFlyer 新增下载中间层，不是最终报表层

### `dws_content_item_device_daily`

- 粒度：`dt × prop_device_id`
- 说明：投资人报表内容 item 曝光 / 点击中间层，不是最终报表层

## 字段

| 需求字段 | 定义 |
|------|------|
| 日期 | 报表日期 |
| 新增视频 | 当天新发布且有播放的视频数量 |
| 活跃视频数 | 当天有播放的视频数量 |
| 新增视频播放数 | 当天新增播放次数 |
| 视频平均播放数 | 新增视频播放数 / 活跃视频数 |
| 新增下载 | 当日新增的下载数 |
| 播放下载转化率 | 新增下载 / 新增视频播放数 |
| 视频平均下载转化数 | 新增下载 / 活跃视频数量 |
| 新增设备数 | 当日新增的设备数 |
| 活跃设备数 | 当日到访的设备数 |
| 活跃注册用户数 | 当日到访的注册用户 |
| 人均停留时长 | 当日每设备平均停留时长 |
| 人均内容展示量 | 平均每个活跃设备展示了多少商品和内容卡片 |
| 人均内容点击量 | 平均每个活跃设备打开多少商品详情页和内容详情页 |
| 内容点击率 | 人均内容点击量 / 人均内容展示量 |
| 次日留存 | 当日活跃注册用户次日留存率 |
| 7日留存 | 当日活跃注册用户第7日留存率 |

## 已落地字段

| 需求字段 | 落地表 | 落地字段 | 口径 |
|------|------|------|------|
| 新增视频 | `decom.dws_video_daily` | `is_new_video` 聚合 | `发布时间（America/Toronto 日期） = dt` 且 `dt` 当天播放增量 `> 0` 的视频数 |
| 活跃视频数 | `decom.dws_video_daily` | `is_active_video` 聚合 | `dt` 当天播放增量 `> 0` 的视频数 |
| 新增视频播放数 | `decom.dws_video_daily` | `daily_view_increment` 聚合 | `dt` 当天全量视频的播放增量之和，不限定 `发布时间 = dt` |
| 视频平均播放数 | `decom.dws_video_daily` | 派生指标 | `new_video_view_count / active_video_count`，即活跃视频的平均播放增量 |
| 新增下载 | `decom.dws_appsflyer_download_daily` | `new_download_count` | AppsFlyer `install` 口径的 iOS + Android 当日新增下载总数，按 `America/Toronto` 切天 |
| 新增设备数 | `decom.ads_daily_investor` | `new_device_count` | 来自 `decom.dws_device_daily`，按 `COUNT(DISTINCT IF(is_new_device, prop_device_id, NULL))` 计算 |
| 活跃设备数 | `decom.ads_daily_investor` | `active_device_count` | 来自 `decom.dws_device_daily`，按 `COUNT(DISTINCT prop_device_id)` 计算 |
| 活跃注册用户数 | `decom.ads_daily_investor` | `active_registered_user_count` | 来自 `decom.dws_user_daily`，按 `COUNT(DISTINCT prop_user_id)` 计算 |
| 人均停留时长 | `decom.ads_daily_investor` | `avg_duration_sec` | 来自 `decom.dws_device_daily`，`SUM(session_duration_sec) / 活跃设备数` |
| 人均内容展示量 | `decom.ads_daily_investor` | `avg_content_exposure_count` | 来自 `decom.dws_content_item_device_daily`，`SUM(exposure_item_count) / 活跃设备数` |
| 人均内容点击量 | `decom.ads_daily_investor` | `avg_content_click_count` | 来自 `decom.dws_content_item_device_daily`，`SUM(click_item_count) / 活跃设备数` |
| 内容点击率 | `decom.ads_daily_investor` | `content_ctr` | 来自 `decom.dws_content_item_device_daily`，`SUM(click_item_count) / SUM(exposure_item_count)` |
| 次日留存 | `decom.ads_daily_investor` | `next_day_retention_rate` | 来自 `decom.dws_user_daily`，以当日活跃注册用户 cohort 为分母，统计 `dt+1` 仍活跃的去重用户数 / cohort 用户数；若 `dt+1` 未成熟返回 `NULL` |
| 7日留存 | `decom.ads_daily_investor` | `day_7_retention_rate` | 来自 `decom.dws_user_daily`，以当日活跃注册用户 cohort 为分母，统计 `dt+7` 仍活跃的去重用户数 / cohort 用户数；若 `dt+7` 未成熟返回 `NULL` |
| 播放下载转化率 | `decom.ads_daily_investor` | `play_download_conversion_rate` | `new_download_count / new_video_view_count`，其中 `new_video_view_count` 为当日全量视频播放增量 |
| 视频平均下载转化数 | `decom.ads_daily_investor` | `avg_video_download_conversion_count` | `new_download_count / active_video_count` |

补充说明：
- 各渠道 `views / view_count` 为累计值，不是日增值
- 每个视频每天取 `max(collected_at)` 对应的累计播放值，作为该视频该日的日末累计播放数
- 某视频某日播放增量 = `当日日末累计播放数 - 前一日日末累计播放数`
- 若某视频次日没有采集记录，则次日的日末累计播放数沿用前一日，次日播放增量记为 `0`
- AppsFlyer `daily_report/v5` 返回值按 `date + media source + campaign` 分组，同一天需先汇总 `Installs`
- `ods_appsflyer_download` 需保留完整原始行，当前通过 `raw_row_json` 存储 AppsFlyer 每行返回字段
- 投资人主题最终 ADS 粒度仍为 `dt`；复用 `dws_device_daily` / `dws_user_daily` 时，设备数和用户数要按 `id` 去重后再汇总，不能直接相加各 `platform`
- 内容展示量 / 点击量不能复用 `content_consume_count`；它们是 `post / spu` item 级指标，需基于 `dwd_event_log.product_code`、`dwd_event_log.post_code` 重新计数
- `dwd_event_log` 已提前展开 `args_spu` / `args_post`；同一原始事件若带多个 item，需要按 `raw_event_id + item_key` 统计，而不是只按 `raw_event_id`
- 内容展示量的 feeds 事件只包含 item 级曝光：`v_home_feeds`, `v_shop_feeds`, `v_star_post_feeds`, `v_brand_post_feeds`, `v_kol_post_feeds`；不包含 `v_home_star`, `v_home_magazine`, `v_home_brand` 这类 module 曝光
- 留存口径中的“注册用户”指 `prop_user_id` 非空且当日活跃的用户，不是 `is_new_user = TRUE` 的新增注册用户

## 依赖

- 外部来源：AppsFlyer Aggregate Pull API `daily_report/v5`
- 配置：`env/appsflyer.json`
- 上游：`videos.yt_videos`, `videos.yt_video_analytics_log`, `videos.videos`, `videos.video_stats_log`, `videos.ig_media`, `videos.ig_media_stats_log`, `decom.dwd_event_log`, `decom.ods_appsflyer_download`
- 中间表：`decom.dws_video_daily`, `decom.dws_appsflyer_download_daily`, `decom.dws_device_daily`, `decom.dws_user_daily`, `decom.dws_content_item_device_daily`
- 报表表：`decom.ads_daily_investor`
- 脚本：`jobs/ods/ods_appsflyer_download/ods_appsflyer_download.py`, `jobs/dws/dws_video_daily.py`, `jobs/dws/dws_appsflyer_download_daily.py`, `jobs/dws/dws_daily.py`, `jobs/ads/ads_daily_investor.py`
- DDL：`ddl/ods/ods_appsflyer_download.sql`, `ddl/dws/dws_video_daily.sql`, `ddl/dws/dws_appsflyer_download_daily.sql`, `ddl/dws/dws_device_daily.sql`, `ddl/dws/dws_user_daily.sql`, `ddl/dws/dws_content_item_device_daily.sql`, `ddl/dwd/dwd_event_log.sql`, `ddl/ads/ads_daily_investor.sql`
- 编排：`env/etl_config.json`, `etl_run.py`, `jobs/dws/dws_daily.py`, `jobs/ads/ads_daily.py`
