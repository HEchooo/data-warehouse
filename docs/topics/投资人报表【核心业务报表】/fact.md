# 投资人报表【核心业务报表】

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | 投资人报表 | 投资人看板、核心业务报表 |
| 指标 | 活跃设备数 | DAU、日活设备数 |
| 指标 | 活跃注册用户数 | 注册用户 DAU、注册日活用户 |
| 指标 | 新增下载 | 新增下载数、新增安装数 |
| 指标 | 次日留存 | D1 留存、1 日留存 |
| 指标 | 7 日留存 | D7 留存、第 7 日留存 |
| 指标 | 内容点击率 | 内容 CTR |

## 适用范围

- 业务线：`Alvin's Club App`, `ECN 视频`, `TikTok 橱窗号`
- 适合：查询按天汇总的核心经营指标，包括视频、下载、App 活跃、内容互动、商详页和活跃注册用户留存。
- 不适合：App 分平台日常分析应使用[全量报表](../全量报表/fact.md)或[用增报表](../用增报表/fact.md)；社媒账号、视频和属性明细应使用[Social Content 社媒表现](../【Social Content】社媒表现数据看板/fact.md)。
- 需要澄清：问题未说明业务线、统计主体、日期范围，或“留存”指活跃用户还是新增用户 cohort 时，需要先确认。

## 报表

### ads_daily_investor

- 表：`my-project-8584-jetonai.decom.ads_daily_investor`
- 状态：`active`
- 粒度：一行代表一个 `dt`
- 时区：`America/Toronto`
- 数据范围：jhonson 团队视频指标，以及 App 下载、活跃、内容互动、商详页和活跃注册用户留存的日汇总。
- 必要过滤：无。
- 不包含：平台、渠道、账号或单视频明细；`dim_video` 的 pre-binding 视频排除规则；新增注册用户 cohort 留存。
- Dashboard：[投资人报表【核心业务报表】](https://bi.alvinclub.ca/superset/dashboard/101/)（id=101）
- Lark 推送：独立脚本 `jobs/ads/ads_daily_investor_lark_push.py`，每日推送 `America/Toronto` 昨日一行明细数据；卡片附 dashboard 链接和数据口径 wiki 链接

## 字段

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 日期 | `dt` | 自然日 | 报表日期 | 无 | 不聚合 |
| 新增视频 | `new_video_count` | 统一视频 | 首次正播放日计新增；自动发布按跨平台 `task_id` 统一键去重，手动发布按原始视频键 | jhonson 团队 | 可跨天求和 |
| 活跃视频数 | `active_video_count` | 统一视频 | 当天有播放增量的视频数；自动发布按跨平台 `task_id` 统一键去重 | jhonson 团队 | 不可跨天求和 |
| 新增视频播放数 | `new_video_view_count` | 播放 | 当天全量视频播放增量之和，不限定发布时间 | jhonson 团队 | 可跨天求和；源累计值回落时可能为负 |
| 新增视频次日平均播放数 | `new_video_next_day_avg_view_count` | 新增视频 cohort | `dt-1` 首次正播放视频在 `dt` 的累计播放总和 / cohort 视频数 | jhonson 团队 | 不可跨天求和或平均 |
| 新增视频 7 日播放数 | `new_video_7d_avg_view_count` | 新增视频 cohort | `dt-6` 首次正播放视频在 `dt` 的累计播放总和 / cohort 视频数 | jhonson 团队 | 不可跨天求和或平均 |
| 视频制作成本（美元） | `video_production_cost_usd` | 视频 | 新增 YouTube 视频时长秒数 × `1.05` 人民币/秒 × CNY→USD 汇率 | jhonson 团队 YouTube | 可跨天求和 |
| 每千次播放成本（美元） | `cost_per_thousand_views_usd` | 播放 | `video_production_cost_usd / new_video_view_count × 1000` | jhonson 团队 | 不可求和或平均；跨天须用汇总分子分母重算 |
| 新增下载 | `new_download_count` | 安装 | AppsFlyer iOS + Android 当日 `install` 总数 | 无 | 可跨天求和 |
| 播放下载转化率 | `play_download_conversion_rate` | 安装 / 播放 | `new_download_count / new_video_view_count` | 无 | 不可求和或平均；跨天须用汇总分子分母重算 |
| 新增设备数 | `new_device_count` | 设备 | `first_app_active_dt = dt` 的去重 App 设备数 | App | 可跨天求和 |
| 活跃设备数 | `active_device_count` | 设备 | 当日去重 App 设备数 | App | 不可跨天求和 |
| 活跃注册用户数 | `active_registered_user_count` | 用户 | 当日去重 App 活跃注册用户数 | App；排除被动触达事件 | 不可跨天求和 |
| 人均停留时长 | `avg_duration_sec` | 已注册活跃设备 | 已注册设备停留时长总和 / App 已注册活跃设备数 | App | 不可跨天求和或平均 |
| 人均内容展示量 | `avg_content_exposure_count` | 已注册活跃设备 | item 级内容展示总量 / App 已注册活跃设备数 | App | 不可跨天求和或平均 |
| 人均内容点击量 | `avg_content_click_count` | 已注册活跃设备 | item 级内容点击总量 / App 已注册活跃设备数 | App | 不可跨天求和或平均 |
| 内容点击率 | `content_ctr` | 已注册活跃设备 | item 级内容点击总量 / item 级内容展示总量 | App | 不可跨天求和或平均；跨天须用汇总分子分母重算 |
| 商详页曝光 UV | `product_detail_exposure_uv` | 用户 | 商详页曝光 PV 大于 0 的去重 App 注册用户数 | App | 不可跨天求和 |
| 商详页曝光 PV | `product_detail_exposure_pv` | 曝光事件 | App 注册用户商详页曝光次数总和 | App | 可跨天求和 |
| 人均商详页点击量 | `avg_product_detail_exposure_count` | 用户 | `product_detail_exposure_pv / product_detail_exposure_uv` | App | 不可跨天求和或平均 |
| 次日留存 | `next_day_retention_rate` | 活跃注册用户 cohort | `dt+1` 仍在 App 活跃的去重用户数 / 当日活跃注册用户数；未成熟返回 `NULL` | App；排除被动触达事件 | 不可跨天求和或平均 |
| 7 日留存 | `day_7_retention_rate` | 活跃注册用户 cohort | `dt+7` 仍在 App 活跃的去重用户数 / 当日活跃注册用户数；未成熟返回 `NULL` | App；排除被动触达事件 | 不可跨天求和或平均 |

## 需求字段

| 需求字段 | 定义 |
|------|------|
| 日期 | 报表日期 |
| 新增视频 | 某视频第一次被采集到且播放量大于 0 的那一天计为新增，后续日期不重复计数；自动发布跨平台复用同一 `task_id` 时，只要成功发布的平台数 `>= 1` 就按一个视频计算，手动发布不去重 |
| 活跃视频数 | 当天有播放的视频数量；自动发布跨平台复用同一 `task_id` 时，只要成功发布的平台数 `>= 1` 就按一个视频计算，手动发布不去重 |
| 新增视频播放数 | 当天新增播放次数 |
| 新增视频次日平均播放数 | 统计日 `dt` 回看 `dt-1` 新增视频 cohort，计算这些视频在 `dt` 当天累计播放数均值 |
| 新增视频 7 日播放数 | 统计日 `dt` 回看 `dt-6` 新增视频 cohort，计算这些视频在 `dt` 当天累计播放数均值 |
| 视频制作成本（美元） | 当天新增视频累计时长（秒）× `1.05`（人民币/秒）× `CNY->USD` 汇率 |
| 每千次播放成本（美元） | `视频制作成本（美元） / 新增视频播放数 * 1000` |
| 新增下载 | 当日新增的下载数 |
| 播放下载转化率 | 新增下载 / 新增视频播放数 |
| 新增设备数 | 当日新增的设备数 |
| 活跃设备数 | 当日到访的设备数 |
| 活跃注册用户数 | 当日到访的注册用户 |
| 人均停留时长 | 当日每设备平均停留时长 |
| 人均内容展示量 | 平均每个活跃设备展示了多少商品和内容卡片 |
| 人均内容点击量 | 平均每个活跃设备打开多少商品详情页和内容详情页 |
| 内容点击率 | 人均内容点击量 / 人均内容展示量 |
| 商详页曝光 UV | 当日访问过商详页的去重访客数 |
| 商详页曝光 PV | 当日商详页曝光次数 |
| 人均商详页点击量 | 商详页曝光 PV / 商详页曝光 UV |
| 次日留存 | 当日活跃注册用户次日留存率 |
| 7日留存 | 当日活跃注册用户第7日留存率 |

## 已落地字段

| 需求字段 | 落地表 | 落地字段 | 口径 |
|------|------|------|------|
| 新增视频 | `decom.ads_daily_investor` | `new_video_count` | 先按 `decom.dws_video_daily.is_new_video` 识别各平台视频首次正播放日期，再通过 `decom.dim_auto_publish_video_task.unified_video_key` 把自动发布且跨平台复用同一 `task_id` 的成功发布平台视频收敛成一个统一视频键；同一 `task_id` 只要成功平台数 `>= 1`，就只在该统一键最早的首次正播放日计 `1`；手动发布视频仍按原始 `video_key` 计数；当前仅保留 jhonson 团队视频 |
| 活跃视频数 | `decom.ads_daily_investor` | `active_video_count` | 先按 `decom.dws_video_daily.is_active_video` 识别当天有播放增量的视频，再通过 `decom.dim_auto_publish_video_task.unified_video_key` 把自动发布且跨平台复用同一 `task_id` 的成功发布平台视频收敛成一个统一视频键做去重；同一 `task_id` 只要当天任一成功平台活跃，就记 `1`；手动发布视频仍按原始 `video_key` 计数；当前仅保留 jhonson 团队视频 |
| 新增视频播放数 | `decom.dws_video_daily` | `daily_view_increment` 聚合 | `dt` 当天全量视频的播放增量之和，不限定 `发布时间 = dt`；当前仅保留 jhonson 团队视频 |
| 新增视频次日平均播放数 | `decom.ads_daily_investor` | `new_video_next_day_avg_view_count` | 按统一视频键回看 `dt-1` 新增 cohort，统计这些视频在 `dt` 当天累计播放总和 / cohort 视频去重数；旧字段 `avg_new_video_view_count` 仅保留兼容，不再作为展示字段 |
| 新增视频 7 日播放数 | `decom.ads_daily_investor` | `new_video_7d_avg_view_count` | `published_7d_cohort_view_count / published_7d_cohort_video_count`；其中 `published_7d_cohort_video_count` / `published_7d_cohort_view_count` 为内部聚合分子分母：按统一视频键回看 `dt-6` 新增 cohort，统计 `dt` 当天累计播放样本数与总和 |
| 视频制作成本（美元） | `decom.ads_daily_investor` | `video_production_cost_usd` | `new_youtube_video_duration_sec * 1.05 * cny_to_usd_rate`；`new_youtube_video_duration_sec` 仅统计 jhonson 团队 YouTube 渠道 `is_new_video = TRUE` 的 `videos.yt_videos.duration` 累计秒数，汇率来自 `decom.currency_conversion_dict` |
| 每千次播放成本（美元） | `decom.ads_daily_investor` | `cost_per_thousand_views_usd` | `video_production_cost_usd / new_video_view_count * 1000`，分母直接复用表内“新增视频播放数” |
| 新增下载 | `decom.dws_appsflyer_download_daily` | `new_download_count` | `dws_appsflyer_download_daily` 先落 `dt × platform` 的 AppsFlyer `install` 日新增下载；`ads_daily_investor` 再按 `dt` 汇总 iOS + Android 总数，按 `America/Toronto` 切天 |
| 新增设备数 | `decom.ads_daily_investor` | `new_device_count` | 来自 `decom.dws_registered_app_device_daily` 的 app 设备日事实，按 `COUNT(DISTINCT IF(first_app_active_dt = dt, prop_device_id, NULL))` 计算 |
| 活跃设备数 | `decom.ads_daily_investor` | `active_device_count` | 来自 `decom.dws_registered_app_device_daily` 的 app 设备日事实，按 `COUNT(DISTINCT prop_device_id)` 计算 |
| 活跃注册用户数 | `decom.ads_daily_investor` | `active_registered_user_count` | 来自 `decom.dws_registered_app_user_daily` 的 app 活跃注册用户事实，按 `COUNT(DISTINCT prop_user_id)` 计算 |
| 人均停留时长 | `decom.ads_daily_investor` | `avg_duration_sec` | 来自 `decom.dws_registered_app_device_daily.registered_duration_sec` 汇总 / app 已注册活跃设备数 |
| 人均内容展示量 | `decom.ads_daily_investor` | `avg_content_exposure_count` | 来自 `decom.dws_registered_app_device_daily.registered_content_exposure_count` 汇总 / app 已注册活跃设备数 |
| 人均内容点击量 | `decom.ads_daily_investor` | `avg_content_click_count` | 来自 `decom.dws_registered_app_device_daily.registered_content_click_count` 汇总 / app 已注册活跃设备数 |
| 内容点击率 | `decom.ads_daily_investor` | `content_ctr` | 来自 `decom.dws_registered_app_device_daily` 的已注册设备级点击 / 曝光汇总比值 |
| 商详页曝光 UV | `decom.ads_daily_investor` | `product_detail_exposure_uv` | 来自 `decom.dws_registered_app_user_daily`，按 `COUNT(DISTINCT IF(product_detail_exposure_pv > 0, prop_user_id, NULL))` 计算 |
| 商详页曝光 PV | `decom.ads_daily_investor` | `product_detail_exposure_pv` | 来自 `decom.dws_registered_app_user_daily.product_detail_exposure_pv` 汇总 |
| 人均商详页点击量 | `decom.ads_daily_investor` | `avg_product_detail_exposure_count` | 逻辑不变，仍为 `product_detail_exposure_pv / product_detail_exposure_uv`；仅改展示名称为“人均商详页点击量”，并仅保留 app 已注册用户数据 |
| 次日留存 | `decom.ads_daily_investor` | `next_day_retention_rate` | 来自 `decom.dws_registered_app_user_daily` 的 app 活跃注册用户 cohort，以当日活跃注册用户为分母，统计 `dt+1` 仍在 app 活跃的去重用户数 / cohort 用户数；若 `dt+1` 未成熟返回 `NULL` |
| 7日留存 | `decom.ads_daily_investor` | `day_7_retention_rate` | 来自 `decom.dws_registered_app_user_daily` 的 app 活跃注册用户 cohort，以当日活跃注册用户为分母，统计 `dt+7` 仍在 app 活跃的去重用户数 / cohort 用户数；若 `dt+7` 未成熟返回 `NULL` |
| 播放下载转化率 | `decom.ads_daily_investor` | `play_download_conversion_rate` | `new_download_count / new_video_view_count`，其中 `new_video_view_count` 为当日全量视频播放增量 |

补充说明：
- 投资人报表里的视频播放类指标仍复用 `decom.dws_video_daily`；YouTube 累计播放快照已切到 v2 源 `videos.yt_video_stats_log.view_count`，TikTok / Ins 仍分别使用 `videos.video_stats_log.view_count` / `videos.ig_media_stats_log.view_count`
- `decom.dws_video_daily` 不经过 `dim_video`，因此投资人报表不会自动套用 `dim_video` 的 pre-binding 视频排除规则，当前仍可能包含账号绑定前的存量视频
- 根据 `docs/rules/video-mapping.md`，当前 jhonson 团队口径按 YouTube 排除 bob 团队测试渠道和内部渠道实现；TikTok / Ins 在该文档中未定义团队边界，因此暂维持原有全量口径
- 投资人报表与 Social Content v2 使用同一组 10 个 YouTube 排除频道；这两个报表的视频范围不按 `decom.dim_video.is_formal_video` 做例外保留
- `新增视频`、`活跃视频数` 现在统一通过 `decom.dim_auto_publish_video_task` 做自动发布 `task_id` 跨平台去重；只有能稳定映射到唯一 `task_id` 的自动发布视频会去重，同一 `task_id` 只要存在至少一个成功平台就作为一个统一视频参与计数；手动发布视频不去重
- 自动发布跨平台去重只作用于视频数量类指标，不改 `new_video_view_count`、`new_video_contributed_view_count` 这类播放量汇总辅助字段
- 各渠道 `view_count` 为累计值，不是日增值
- 每个视频每天取 `max(collected_at)` 对应的累计播放值，作为该视频该日的日末累计播放数
- 某视频某日播放增量 = `当日日末累计播放数 - 前一日日末累计播放数`
- 若源头累计播放值较前一日回落，则播放增量出现负数属于正常情况，当前口径保留该负值，不做截断为 `0`
- 若某视频次日没有采集记录，则次日的日末累计播放数沿用前一日，次日播放增量记为 `0`
- `new_video_contributed_view_count` 是内部辅助字段：取 `is_new_video = TRUE` 当天的 `raw_day_end_views`，表示该视频首次被采到正播放量时已经累计拿到的播放数；该辅助字段不在展示层单独输出，也不再作为“新增视频次日平均播放数”的分子
- 新增视频次日平均播放数按 `first_positive_view_dt = dt-1` 的统一视频 cohort 计算，分子取 cohort 视频在 `dt` 当天的累计播放总和，分母取 cohort 视频去重数
- `avg_new_video_view_count` 为历史兼容字段，当前由 ADS 写入与 `new_video_next_day_avg_view_count` 相同的值；新看板和推送应使用 `new_video_next_day_avg_view_count`
- 新增视频 7 日播放数按 `first_positive_view_dt = dt-6` 的统一视频 cohort 计算，分子取 cohort 视频在 `dt` 当天的累计播放总和，分母取 cohort 视频去重数
- 视频制作成本（美元）只使用 jhonson 团队 YouTube 新增视频的 `videos.yt_videos.duration`（秒）作为时长分子，先乘 `1.05` 人民币单价，再乘 `decom.currency_conversion_dict` 的 `CNY->USD` 汇率
- AppsFlyer `daily_report/v5` 返回值按 `date + media source + campaign` 分组，同一天需先汇总 `Installs`
- `ods_appsflyer_download` 需保留完整原始行，当前通过 `raw_row_json` 存储 AppsFlyer 每行返回字段
- `dws_appsflyer_download_daily` 当前已升级为 `dt × platform` 复用事实；投资人主题最终粒度仍为 `dt`，新增下载需要先按 `dt` 汇总 iOS + Android，不能继续假设中层一日一行
- 投资人主题最终 ADS 粒度仍为 `dt`；复用 `dws_device_daily` / `dws_user_daily` 时，设备数和用户数要按 `id` 去重后再汇总，不能直接相加各 `platform`
- `新增设备数`、`活跃设备数`、`活跃注册用户数` 已下沉到 `decom.dws_registered_app_device_daily` / `decom.dws_registered_app_user_daily`，当前仅保留 app 数据
- 人均停留时长的分母要收敛到 app 已注册活跃设备；当前通过 `decom.dws_registered_app_device_daily.has_registered_user_activity` 控制分母
- 当前 `投资人报表（日表）-明细表` 直接读取物理 dataset `ads_daily_investor`（Superset dataset id=228）；若要让历史日期切到 Batch 7 新链路，仍需单独确认回填与切换方案
- `ads_daily_investor` 默认只处理 `America/Toronto` 昨日及以前的成熟 `dt`；不再把多伦多当天的跨上游半成品写进日报
- 内容展示量 / 点击量不能复用 `content_consume_count`；它们是 `post / spu` item 级指标，当前已下沉到 `decom.dws_registered_app_device_daily`
- 商详页曝光 UV / PV 当前已下沉到 `decom.dws_registered_app_user_daily`
- app 活跃注册用户与留存口径排除 `send_push`、`send_email` 这类被动触达事件；它们不能单独把用户算成 app 回访
- `dwd_event_log` 已提前展开 `args_spu` / `args_post`；同一原始事件若带多个 item，需要按 `raw_event_id + item_key` 统计，而不是只按 `raw_event_id`
- 内容展示量的 feeds 事件只包含 item 级曝光：`v_home_feeds`, `v_shop_feeds`, `v_star_post_feeds`, `v_brand_post_feeds`, `v_kol_post_feeds`；不包含 `v_home_star`, `v_home_magazine`, `v_home_brand` 这类 module 曝光
- 留存口径中的“注册用户”指 `prop_user_id` 非空且当日在 app 活跃的用户，不是 `is_new_user = TRUE` 的新增注册用户
- Lark 推送不挂在高频共享 ETL 成功回调上；当前远程服务器高频运行主链路，若直接复用会重复发送
- Lark 推送脚本默认查询 `DATE_SUB(CURRENT_DATE('America/Toronto'), INTERVAL 1 DAY)`，仅发送单个 `dt` 对应的一行明细数据
- Lark 卡片展示核心明细指标，当前包含 `人均商详页点击量`，且不再展示 `视频平均下载转化数`；仍不展示 `次日留存`、`7日留存`；同时提供 dashboard 与数据口径 wiki 跳转按钮
- Lark 卡片中的“新增视频次日平均播放数”“新增视频 7 日播放数”按展示层单独做四舍五入后以整数推送，不修改 `ads_daily_investor` 表内原始小数值
- Lark 卡片中的“每千次播放成本（美元）”“人均内容展示量”“人均内容点击量”“人均商详页点击量”按展示层统一保留两位小数，不修改 `ads_daily_investor` 表内原始小数值
- 本次仅修改逻辑，不包含生产历史分区回填；若要让历史日期统一 v2 YouTube 口径，需要单独确认回填方案
- 推送时间若按上海固定时刻配置，需要考虑 `America/Toronto` 夏令时；要稳定拿到多伦多昨日完整数据，建议独立 cron 配到上海 `13:00` 之后

## 依赖

- 外部来源：AppsFlyer Aggregate Pull API `daily_report/v5`
- 配置：`env/appsflyer.json`
- 配置：`env/lark_webhook.json`
- 上游：`videos.yt_videos`, `videos.yt_video_stats_log`, `videos.videos`, `videos.video_stats_log`, `videos.ig_media`, `videos.ig_media_stats_log`, `decom.dwd_event_log`, `decom.ods_appsflyer_download`, `decom.currency_conversion_dict`
- 中间表：`decom.dim_auto_publish_video_task`, `decom.dws_video_daily`, `decom.dws_appsflyer_download_daily`, `decom.dws_registered_app_device_daily`, `decom.dws_registered_app_user_daily`
- 中间表粒度：`dws_appsflyer_download_daily` 为 `dt × platform`；`dws_registered_app_device_daily` 为 `dt × prop_device_id`；`dws_registered_app_user_daily` 为 `dt × prop_user_id`
- 报表表：`decom.ads_daily_investor`
- Superset：物理 dataset `ads_daily_investor`（id=228），chart `投资人报表（日表）-明细表`（id=574）
- 脚本：`jobs/ods/ods_appsflyer_download/ods_appsflyer_download.py`, `jobs/dim/dim_auto_publish_video_task.py`, `jobs/dws/dws_video_daily.py`, `jobs/dws/dws_appsflyer_download_daily.py`, `jobs/dws/dws_registered_app_device_daily.py`, `jobs/dws/dws_registered_app_user_daily.py`, `jobs/ads/ads_daily_investor.py`, `jobs/ads/ads_daily_investor_lark_push.py`
- DDL：`ddl/ods/ods_appsflyer_download.sql`, `ddl/dim/dim_auto_publish_video_task.sql`, `ddl/dws/dws_video_daily.sql`, `ddl/dws/dws_appsflyer_download_daily.sql`, `ddl/dws/dws_registered_app_device_daily.sql`, `ddl/dws/dws_registered_app_user_daily.sql`, `ddl/dwd/dwd_event_log.sql`, `ddl/ads/ads_daily_investor.sql`
- 规则：`docs/rules/data.md`, `docs/rules/video-mapping.md`
- 编排：`env/etl_config.json`, `etl_run.py`, `jobs/ads/ads_daily_investor.py`
