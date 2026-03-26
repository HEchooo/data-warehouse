# 投资人报表【核心业务报表】

## 报表

- 粒度：`dt`
- 当前未落地最终 ADS 表
- 本阶段先沉淀投资人主题所需的 DWS 依赖，待指标集完整后再统一建设最终 ADS

### `dws_appsflyer_download_daily`

- 粒度：`dt`
- 说明：投资人报表 AppsFlyer 新增下载中间层，不是最终报表层

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
| 人均内容展示量 | 平均给每个用户展示了多少商品和内容卡片 |
| 人均内容点击量 | 每个设备平均打开多少商品详情页和内容详情页 |
| 内容点击率 | 人均内容点击量 / 人均内容展示量 |
| 次日留存 | 注册用户次日留存率 |
| 7日留存 | 注册用户第7日留存率 |

## 已落地字段

| 需求字段 | 落地表 | 落地字段 | 口径 |
|------|------|------|------|
| 新增视频 | `decom.dws_video_daily` | `is_new_video` 聚合 | `发布时间（America/Toronto 日期） = dt` 且 `dt` 当天播放增量 `> 0` 的视频数 |
| 活跃视频数 | `decom.dws_video_daily` | `is_active_video` 聚合 | `dt` 当天播放增量 `> 0` 的视频数 |
| 新增视频播放数 | `decom.dws_video_daily` | `daily_view_increment` 聚合 | `发布时间（America/Toronto 日期） = dt` 的视频，在 `dt` 当天产生的播放增量之和 |
| 视频平均播放数 | `decom.dws_video_daily` | 派生指标 | `new_video_view_count / active_video_count` |
| 新增下载 | `decom.dws_appsflyer_download_daily` | `new_download_count` | AppsFlyer `install` 口径的 iOS + Android 当日新增下载总数，按 `America/Toronto` 切天 |

补充说明：
- 各渠道 `views / view_count` 为累计值，不是日增值
- 每个视频每天取 `max(collected_at)` 对应的累计播放值，作为该视频该日的日末累计播放数
- 某视频某日播放增量 = `当日日末累计播放数 - 前一日日末累计播放数`
- 若某视频次日没有采集记录，则次日的日末累计播放数沿用前一日，次日播放增量记为 `0`
- AppsFlyer `daily_report/v5` 返回值按 `date + media source + campaign` 分组，同一天需先汇总 `Installs`
- `ods_appsflyer_download` 需保留完整原始行，当前通过 `raw_row_json` 存储 AppsFlyer 每行返回字段
- `播放下载转化率`、`视频平均下载转化数` 的分母已具备，但当前仍未落最终 ADS 表

## 依赖

- 外部来源：AppsFlyer Aggregate Pull API `daily_report/v5`
- 配置：`env/appsflyer.json`
- 上游：`videos.yt_videos`, `videos.yt_video_analytics_log`, `videos.videos`, `videos.video_stats_log`, `videos.ig_media`, `videos.ig_media_stats_log`, `decom.ods_appsflyer_download`
- 中间表：`decom.dws_video_daily`, `decom.dws_appsflyer_download_daily`
- 报表表：当前未落地最终 ADS 表
- 脚本：`jobs/ods/ods_appsflyer_download/ods_appsflyer_download.py`, `jobs/dws/dws_video_daily.py`, `jobs/dws/dws_appsflyer_download_daily.py`
- DDL：`ddl/ods/ods_appsflyer_download.sql`, `ddl/dws/dws_video_daily.sql`, `ddl/dws/dws_appsflyer_download_daily.sql`
- 编排：`env/etl_config.json`, `etl_run.py`, `jobs/dws/dws_daily.py`
