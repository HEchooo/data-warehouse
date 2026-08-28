# Pipeline 规则

本文件只承载**跨主题**的编排规则与全局链路拓扑。

- 单个主题的字段口径、分母定义、窗口天数、Superset 对象，以 `docs/topics/<主题>/fact.md` 为准，本文件不复制
- 表与脚本的清单以 `env/etl_config.json` 为准，本文件不镜像

## ADS 编排

### 原则

- 全部 ADS 报表都走独立表级入口，共享 `ADS` 旧入口已从仓库移除
- 每张 `ADS` 表自己按本表上游判定待处理范围，不共享整层日期入口
- `ADS` 只消费本主题所需的 `DIM` / `DWS`；发现 `ADS` 直接读 `ODS` / `DWD` / 外部源表，按待收口架构债处理，不当常规模式
- 固定窗口重刷与全量回刷能力留在 `DWS` / `DIM` 层，不重新引入共享 `ADS` 总入口兜底

### 入口清单

入口清单的单一来源是 `env/etl_config.json`，不在文档里另存一份：

```bash
python3 -c "import json;[print(t['table_name'], t['script'], t['depends_on']) for t in json.load(open('env/etl_config.json'))['tasks'] if t['layer']=='ADS']"
```

新增 ADS 表时只需在 `env/etl_config.json` 增加一个 task（`layer` / `table_name` / `script` / `depends_on` / `schedule_group`），不需要回来改本文件。

### 日期判定

1. 默认模式：按上游事实 `update_time` 窗口 + 目标表水位判定待刷 `dt`，两者取并集
2. `ads_daily_new` 按 `dws_device_daily + dws_user_daily + dws_appsflyer_download_daily` 的并集判定；`ads_daily_total` 按 `dws_device_daily + dws_user_daily` 的并集判定
3. 专题型 `ADS` 只按自己的直接依赖判定范围，不复用其他主题的 `dt`
4. 涉及次留的 `ADS`，要把设备事实变更对应的 `DATE_SUB(dt, INTERVAL 1 DAY)` 一并补刷
5. 同时依赖事实表修正和维表修正时，要分别评估当前分区重刷与历史受影响分区补刷
6. 目标表过滤后可能长期为 0 行的主题，不要用「目标表行数为 0 -> 全量」判定，否则每轮调度都会全表扫描且永远写不进行；改用 `update_time` 窗口 + 固定补最近两天，全量重刷由 `--full-refresh` 人工触发
7. 非下载类 `ADS` 不要直接消费下载链路的回刷日期，避免全链路无效回刷

窗口增量、分区重算、固定回刷窗口的模式选择见 [window-incremental.md](./window-incremental.md)。

## 主链路

只列层级拓扑，口径细节见对应主题文档。

### 基础事实

- 事件链路：`ods_event_log -> dwd_event_log -> dws_device_daily / dws_user_daily / dws_content_item_device_daily -> ads_*`
- 用增链路：`dwd_event_log -> dim_device_first_active + dim_user_first_active -> dws_device_daily + dws_user_daily`；`ods_appsflyer_download -> dws_appsflyer_download_daily`；三者 `-> ads_daily_new`
- 全量链路：`dws_device_daily + dws_user_daily -> ads_daily_total`
- 下载链路：`ods_ios_download + ods_android_download -> dwd_download -> dws_download_daily`
- 用户活跃与留存：`dws_user_daily + dwd_event_log + v3 服务端行为 -> dws_user_activity_daily -> dws_user_retention_feature_daily -> ads_daily_retention_user_feature`；同一张 `dws_user_activity_daily` 还直接支撑 5 张留存分桶 ADS（时长 / 累计关注 / 新增关注 / 试穿次数 / 浏览商详次数）
- 用户时长频次：`dwd_event_log -> dws_device_activity_daily -> ads_daily_user_duration_frequency`

### 站内主题

- 平台内容：`dwd_event_log + v3 服务端行为表 -> dws_platform_content_daily -> ads_daily_content_performance`
- 首页模块：`dwd_event_log -> dws_home_module_daily -> ads_daily_home_module_performance`
- 第三方渠道曝光：`dwd_event_log -> dws_thirdapp_channel_device_daily -> ads_daily_thirdapp_channel_uv`
- 专栏与模块：`v3_decom.community_post(status=1) -> dws_column_post_daily -> ads_daily_column_performance`；月级为 `dws_column_post_monthly` / `dws_module_post_monthly` + `dws_column_interaction_monthly` / `dws_module_interaction_monthly` -> `ads_monthly_column_performance` / `ads_monthly_module_performance`
- 试穿：`dwd_event_log -> dws_tryon_confirm_daily -> ads_daily_tryon_confirm`；`+ dim_post -> ads_daily_post_tryon_confirm`；`+ dws_tryon_user_daily -> ads_daily_tryon_add_cart_conversion`
- 商品试穿：`dwd_event_log -> dws_product_behavior_daily -> ads_daily_product_tryon_performance`
- 衣橱概览：`v3_decom.user_fashion_closet -> dim_user_closet_first_use`；`+ dwd_event_log + dws_user_daily -> dws_closet_daily -> ads_daily_closet_overview`
- push：`v3_decom.user_notice -> dws_push_daily -> ads_daily_push`；召回为 `dwd_event_log -> dws_push_recall_daily -> ads_daily_push_recall`

### 站外与投放

- Launch 商详页：`dwd_event_log -> dws_launch_product_detail_device_daily -> ads_daily_launch_product_detail`，同一张 DWS 还支撑多维度与视频来源的日 / 周 / 月 6 张 ADS
- LTA 看板：`dwd_event_log + v3_decom.kol_rel -> dws_lta_thirdparty_event_daily -> ads_daily_lta_overview + ads_daily_lta_kol + ads_daily_lta_spu`
- AI-Fashion 商详页投放：`dwd_event_log -> dws_ai_fashion_product_detail_promotion_device_daily -> ads_daily_ai_fashion_product_detail_promotion`
- TikTok 广告投放：`TikTok Business API -> ods_tiktok_ad_daily -> dws_tiktok_ad_daily -> ads_daily_tiktok_ad`
  - `dt` 是广告主时区 `Etc/GMT+5`（固定 UTC-5，不含夏令时）自然日，与其余主题的美东自然日在夏令时期间相差 1 小时
  - `DWS` 只固化可加的分子分母，CPC / CPM / CTR / CVR 等率值一律由下游 `SAFE_DIVIDE(SUM(分子), SUM(分母))` 现算，不得对派生列求和或求平均
  - 店铺来自 `/ad/get/` 而不是报表接口；非带货广告（`objective_type = VIDEO_VIEWS` 的 Smart+）没有店铺，看板按店铺过滤时会整体排除
- TikTok GMV Max 投放：`TikTok Business API -> ods_tiktok_gmv_max_daily -> dws_tiktok_gmv_max_daily -> ads_daily_tiktok_gmv_max`
  - 与上面的普通竞价投放是两条独立链路，接口是 `/gmv_max/report/get/`，最细层级只有广告计划，没有广告组 / 广告
  - `gross_revenue` 是商品全渠道归因（付费 + 自然 + 达人），与普通投放的广告归因收入不是同一指标，两套收入禁止相加
  - 接口带 `stat_time_day` 时单次窗口最多 30 天，`ODS` 内部按 30 天切片；报表按 campaign × 天返回笛卡尔积，`DWS` 过滤全零填充行
- AI-Fashion Style Me：`dwd_event_log + v3_decom.common_ext_ai_task + dws_user_daily -> dws_ai_fashion_styleme_daily -> ads_daily_ai_fashion_styleme`
- AI-Fashion 试穿 OKR：`dwd_event_log + dim_user_first_active + v3_decom.common_ext_ai_task -> dws_ai_fashion_tryon_okr_daily`（+ `dws_ai_fashion_tryon_okr_user_daily`）`-> ads_daily_ai_fashion_tryon_okr_v2 + ads_weekly_ai_fashion_tryon_okr(_v2)`
- AI-Fashion 素材内容：`v3_decom.community_fashion_asset + v3_decom.community_post + dim_column -> dim_asset`；`dwd_event_log + v3_decom.common_ext_ai_task -> dws_asset_user_daily`；两者 `-> ads_weekly_ai_fashion_asset_overview + ads_daily_ai_fashion_asset_rankings + ads_daily_ai_fashion_asset_detail`

### Makepod 独立站

- 事件：`ods_event_log + oss_key_process_log.makepod_analyze_state -> dwd_makepod_event_log`
- 日报：`dwd_makepod_event_log -> dim_makepod_device_first_visit -> dws_makepod_traffic_daily`；`v3_decom.user_info -> dim_makepod_user_channel`（同时供流量与订单）；`dim_makepod_user_channel + v3_decom.user_order / pay_order / user_order_item / sku + decom.currency_conversion_dict -> dws_makepod_order_daily`；两张 DWS `-> ads_daily_makepod`
- 周报：`dim_makepod_user_channel + v3_decom.pay_order + decom.currency_conversion_dict -> dws_makepod_order_weekly -> ads_weekly_makepod_paid_user_order`
- 周维度去重人数不能由日表日值相加（跨天多次支付会重复计数），必须回明细在周窗口内整体去重

### 社媒与视频

- 视频维表：`videos.*（含 platform_video_tags）-> dim_video`
- 播放快照：`videos.yt_video_stats_log + videos.video_stats_log + videos.ig_media_stats_log + dim_video -> dws_social_video_snapshot_daily_v2 -> dws_social_video_state_daily_v2`
  - `snapshot` 只有当天真实采集到的视频；`state` 按视频前向填充出日末全量状态，下游算增量统一读 `state`
  - 日末字段含 `day_end_views / likes / comments / shares`；YouTube 源表没有分享数字段，其 `day_end_shares` 保持空值，TikTok 与 Instagram 按 0 起算
- 社媒内容：`dws_social_video_state_daily_v2 + dim_auto_publish_video_task -> dws_social_channel_daily_v2 -> ads_daily_social_content_v2`
- 社媒属性维度：`videos.video_upload_tasks + videos.video_upload_task_channels + dim_video -> dim_social_video_attribute`；`+ dws_social_video_state_daily_v2 -> dws_social_attribute_daily_v2 -> ads_daily_social_content_attribute_v2`
- 视频监控日榜：`dim_video + dws_social_video_state_daily_v2 -> ads_daily_social_video_monitor_rankings_v2`
- 疑似限流账号：`dim_video + dws_social_video_snapshot_daily_v2 -> dws_social_creator_published_t1_daily -> dws_social_creator_view_fluctuation_daily -> ads_daily_social_creator_suspected_throttle_rankings + ads_daily_social_creator_zero_view_rankings + ads_daily_social_creator_view_boost_rankings`
- 橱窗号数据：`videos.users -> dws_social_creator_snapshot_daily`；`+ dim_video + dws_social_video_state_daily_v2 -> ads_daily_social_affiliate_creator + ads_daily_social_affiliate_video`
  - `videos.users` 是状态表没有历史，粉丝这类「新增」指标只能逐日快照后相减，快照表只重写当天分区，历史不可回溯
- 正式号视频发布增量：`dim_video + dws_social_video_state_daily_v2 -> ads_daily_formal_video_increment -> ads_weekly_formal_video_increment_growth`
- 视频编码转化：`dwd_event_log + dim_user_first_active -> dws_video_code_search_user_daily`；`videos.video_upload_tasks + dim_auto_publish_video_task + dws_video_daily + dws_video_code_search_user_daily -> dws_video_code_conversion_daily -> ads_daily_video_code_conversion`
- 投资人主题：`videos.* -> dim_auto_publish_video_task + dws_video_daily`；`dwd_event_log -> dws_registered_app_device_daily + dws_registered_app_user_daily`；`+ dws_appsflyer_download_daily -> ads_daily_investor`
  - 遗留债：视频团队边界过滤仍在 `ADS` 聚合层做过渡处理，后续下沉到稳定中层

## 跨主题约束

这些约束会同时影响多个主题，改动前必须确认全部下游：

- `dwd_event_log` 落库时统一把 `event_name` 小写化（`jobs/dwd/dwd_event_log/dwd_event_log.py`），所有 SQL 必须用小写事件名匹配；按前端抓包的大写形态取数会永远查不到数据
- `c_third_party_goods` 被 LTA 与 AI-Fashion 商详页投放两个主题共用，域名过滤（LTA 走 `mycch.me`、投放走 `https://alvinclub.com`）是唯一的隔离手段，不得放宽
- 周口径统一为 `WEEK(MONDAY)`（周一起），跨主题一致；2026-07-31 前 Makepod 曾误用 `WEEK(SUNDAY)`，已订正
- 总量与新增异常时，先确认自己走的是事件链路还是下载链路
- 首次 / 首日类口径不在增量脚本里现算，规则见 [code.md](./code.md)
