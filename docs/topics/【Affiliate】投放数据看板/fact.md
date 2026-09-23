# 【Affiliate】投放数据看板

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | 【Affiliate】投放数据看板 | 联盟项目数据看板、Social Content 联盟项目、橱窗号投流对照 |
| 看板 | 【Affiliate】运营核心数据看板 | Affiliate 运营核心、广告 ROI / GMV / 件数趋势 |
| 看板 | 【Affiliate】爆款漏斗数据看板 | Affiliate 爆款漏斗、活跃联盟号漏斗 |
| 指标 | 销售金额 | GMV、Gross revenue (Shop) |
| 指标 | 广告投放 ROI | ROAS、投产比 |
| 指标 | 广告投放后带来播放点击率 | 播放点击率 |
| 指标 | 广告投放后带来详情页购买转化率 | 详情页购买转化率 |

## 适用范围

- 业务线：`TikTok 橱窗号`
- 适合：对照橱窗号视频的自然表现与 TikTok 广告投放，看 region、成本、ROI、销售金额 / 件数，以及投流前后件数；也包含按 `dt` 看广告 ROI / 广告 GMV / 广告件数日趋势，以及活跃联盟号 → 当日发布视频 → 第一次投放 ROI>2.5 的漏斗。
- 不适合：橱窗号自然内容总览应使用[Social Content 橱窗号数据](../【Social Content】橱窗号数据/fact.md)；Makepod 店铺广告应使用[Makepod 店铺 TK 广告投放数据](../【Social Content】Makepod 店铺 TK 广告投放数据/fact.md)。不要用 GMV Max 的 `gross_revenue` 填本看板。
- 需要澄清：需要确认是视频日明细还是汇总、看的是广告归因 GMV 还是 RPA 自然 GMV、ROI 是单行还是 SUM/SUM。

## 报表

### ads_daily_social_affiliate_project

- 表：`my-project-8584-jetonai.decom.ads_daily_social_affiliate_project`
- 状态：`active`
- 粒度：一行代表一个 `dt × video_key`
- 时区：`America/Toronto`
- 数据范围：`usage_type = 'tiktok-affiliate'` 的橱窗号视频日表现，左连接同一广告主下能用 `tiktok_item_id` 对上的 AUCTION 广告。账号名单以最新账号快照为准，不写死个数。
- 必要过滤：已关联到广告、且 `spend = 0` 且广告 GMV = 0 的行去掉。未投流视频保留，广告字段保持空值。
- 不包含：Makepod 店铺过滤；按广告名是否含「联盟」圈数；用 `username` / `identity_id` 做 join。

- Dashboard：[【Affiliate】投放数据看板](https://bi.alvinclub.ca/superset/dashboard/affiliate-ad-delivery/)（id=134，slug `affiliate-ad-delivery`）
- Superset 物理 dataset：`ads_daily_social_affiliate_project`（id=299），主时间列 `dt`
- Superset chart：`727`（联盟项目视频日报，raw table）；`728`（联盟项目视频汇总，aggregate）
- 两张图表固定过滤 `first_ad_dt IS NOT NULL`，排除从未投放广告的视频；后来投放的视频，其投放前日记录仍会保留，因为 `first_ad_dt` 是视频级属性
- 新增维度 `ai_kol_name`（ECN AI 博主名称），明细 chart `727` 显示，汇总 chart `728` 按其分组
- 明细按 `dt × video_key` 出一行。汇总不按 `dt` 分组：`dt` 只做时间筛选，维度是 `region + account_name + ai_kol_name + published_dt + first_ad_dt + channel + video_id + video_link + ecn_detail_url`，指标按所选时间段汇总
- 视频链接列单元格文字是 `video_url`，href 指向 `video_link`；ECN 后台详情链接列单元格文字是 `ecn_detail_url`，href 指向 `ecn_detail_url`；`video_id` 展示在视频链接左边，`channel` 展示在视频 ID 左边

- Dashboard：[【Affiliate】运营核心数据看板](https://bi.alvinclub.ca/superset/dashboard/affiliate-core-operation/)（id=135，slug `affiliate-core-operation`）
- 同一个 dataset（id=299），三张 `echarts_timeseries_line`：`729` 广告 ROI 趋势、`730` 广告带来 GMV 趋势、`731` 广告带来售出件数趋势
- 横轴统一是 `dt`（`time_grain_sqla = P1D`），按 `region` 拆线，即每个联盟地区一条线，没有跨地区合计线
- 拆线维度用 `IFNULL(region, '(空)')`：只是图例显示用，不改 `region` 列本身的空值语义。从未成功绑定过地区的行归到 `(空)` 这条线，不当成一个地区
- 指标口径：ROI `SAFE_DIVIDE(SUM(ad_gmv_usd), SUM(spend))`；GMV `SUM(ad_gmv_usd)`；件数 `SUM(ad_items)`。三张图都只看广告侧，不混 RPA、不取 `sales_*`
- 筛选器：`dt` 时间筛选（默认最近 30 个完整美东日）、`region` 多选。没投流的日期三条线都是空值，不补 0

### ads_daily_social_affiliate_funnel

- 表：`my-project-8584-jetonai.decom.ads_daily_social_affiliate_funnel`
- 状态：`active`
- 粒度：一行代表一个 `dt × region`
- 时区：`America/Toronto`
- 数据范围：当天快照里 `usage_type = 'tiktok-affiliate'` 的橱窗号，按 ECN `tiktok_showcase_status` 还原当天是否活跃，再数这些账号当天发布的视频、后来有过消耗的视频、发布当天就开投的视频、以及第一次投放 ROI>2.5 的视频。按 `dim_ecn_account_affiliation.affiliation_label` 拆行。
- 必要过滤：只出已结束的美东日。发布数、在投数、当天新开投数和爆款数都限制在当天仍活跃的联盟号。
- 不包含：跨地区合计行；未投流视频不算在投、不算当天新开投、不算爆款；lifetime ROI 或第二次及以后投放达标不算。

- Dashboard：[【Affiliate】爆款漏斗数据看板](https://bi.alvinclub.ca/superset/dashboard/affiliate-hit-funnel/)（id=136，slug `affiliate-hit-funnel`）
- Superset 物理 dataset：`ads_daily_social_affiliate_funnel`（id=300），主时间列 `dt`
- 一张 `mixed_timeseries`：`732` 柱 = 活跃联盟号，折线 = 当天发布视频数、第一次投放 ROI>2.5 的视频数。图本身不按 region 拆系列
- 一张 `echarts_timeseries_line`：`734` 在投广告视频数、新增投放广告视频数。横轴同样是 `dt`（视频发布日），图本身不按 region 拆系列
- 筛选器：`dt` 时间筛选（默认最近 30 个完整美东日）；`region` **单选**，默认选中第一项，一次只看一个联盟，不能多选对比
- 快照从 2026-08-21 才有橱窗号分区，更早发布日不进这张表
- 省略 DWS：五个计数只给这一张看板用，口径直接读已有 DIM / 账号快照 / 视频日事实

## 字段

### ads_daily_social_affiliate_project

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 统计日 | `dt` | 自然日 | 骨架按 `America/Toronto` 切日，只出已结束的美东日。广告指标源表按广告主时区 `Etc/GMT+5` 切日，夏令时期间两边可能差 1 小时，按日历日字符串对齐，不对齐到小时 | 无 | 不聚合 |
| region | `region` | 账号 | `dim_ecn_account_affiliation.affiliation_label`（联盟美国 / 联盟墨西哥）。ECN 首次成功绑定后不再改；封号后上游删除不影响。从未绑上过才保持空值，不补「未知」 | 按目标地区过滤 | 不聚合 |
| 账号名称 | `account_name` | 账号 | `dim_video` / 账号快照的 `display_name`，不是广告 `identity_id` | 按目标账号过滤 | 不聚合 |
| ECN AI 博主名称 | `ai_kol_name` | 账号 | ECN `account-affiliation` 接口的 `ai_kol_name`，按 `platform + account_handle` 关联；每次成功响应更新。2026-09-22 抽查的 5 个账号和 Katerina Vale 对应账号均无此字段；2026-09-23 该账号返回 `Vivian Lockhart`，字段按账号/时间可能为空 | 按目标账号过滤 | 不聚合 |
| 账号 handle | `account_handle` | 账号 | 内部 `username`，只作展示和 ECN 入参，不当 join key | 按目标账号过滤 | 不聚合 |
| 视频发布日期 | `published_dt` | 视频 | `dim_video.published_dt` | 无 | 不聚合 |
| 渠道 | `channel` | 视频 | `dim_video.channel` 枚举 `youtube / tiktok / ins`。当前左表只收 `tiktok-affiliate` 橱窗号，看板值实际都是 `tiktok` | 按目标渠道过滤 | 不聚合 |
| 视频 ID | `video_id` | 视频 | 内库 `videos.videos.id`，经 `dim_video.video_id` 透出，等于广告 `tiktok_item_id`。看板列名是「视频 ID」 | 按目标视频过滤 | 不聚合 |
| 首次广告投放日期 | `first_ad_dt` | 视频 | 该视频第一次出现 `spend > 0` 的日历日。当天还没投过保持空 | 无 | 视频级属性，同一视频跨天重复出现；汇总用 `MIN` |
| 累计投放次数 | `ad_run_cnt` | 视频 | 截止当天，有消耗的连续日历日算一次投放，中间断一天就算新的一次。例如 9/19 投一天、9/21 再投，则 9/19=1、9/21=2。还没投过保持空，不补 0 | 无 | 截止当天的累计值，不可跨天求和；汇总用 `MAX` |
| 视频链接文案 | `video_url` | 视频 | 固定字符串 `video_url` | 无 | 不聚合 |
| 视频链接 | `video_link` | 视频 | `dim_video.video_link`，看板超链接指向这里 | 按目标视频过滤 | 不聚合 |
| ECN 后台详情链接 | `ecn_detail_url` | 视频 | `ods_ecn_video_publication_owner.task_id` 生成的 ECN 任务详情页 URL；未成功映射 `task_id` 的视频为空，不补默认链接 | 按目标视频过滤 | 不聚合 |
| 广告投放 ROI | `roi` | 视频 | `广告 GMV / spend`，倍数。分子只用广告侧 Shop GMV，不用 RPA、不用接口 `onsite_shopping_roas` | 无 | 不可求和或平均；汇总须用 `SAFE_DIVIDE(SUM(ad_gmv_usd), SUM(spend))` |
| 广告投放成本 | `spend` | 视频 | 当日该视频下全部广告 `spend` 之和，广告主账户货币（当前 USD） | 无 | 可加 |
| 广告 CPM | `cpm` | 视频 | `spend / impressions × 1000`。没有展示时保持空 | 无 | 不可求和或平均；汇总须用 `SAFE_DIVIDE(SUM(spend), SUM(impressions)) * 1000` |
| 广告 CPC | `cpc` | 视频 | `spend / clicks`，点击是目标页点击。没有点击时保持空 | 无 | 不可求和或平均；汇总须用 `SAFE_DIVIDE(SUM(spend), SUM(clicks))` |
| 广告 CPS | `cps` | 视频 | `spend / ad_purchases`，分母是成交笔数 `shop_total_purchase_by_order_submission`，不是件数。没有成交时保持空 | 无 | 不可求和或平均；汇总须用 `SAFE_DIVIDE(SUM(spend), SUM(ad_purchases))` |
| 广告投放后带来播放点击率 | `play_click_rate` | 视频 | `clicks / video_play_actions`，广告接口 Clicks (destination) / Video views，0–1。不是自然播放 `view_increment`，也不是 `clicks / impressions`。没有广告播放时保持空 | 无 | 不可求和或平均；汇总须用 `SAFE_DIVIDE(SUM(clicks), SUM(video_play_actions))` |
| 广告投放后带来详情页购买转化率 | `detail_purchase_cvr` | 视频 | `ad_purchases / clicks`，广告接口购买数 / 点击数，0–1。分子是成交笔数 `shop_total_purchase_by_order_submission`，分母是目标页点击。没有点击时保持空。不要用站内详情页浏览 `onsite_on_web_detail`，也不要用站外像素 `product_details_page_browse` | 无 | 不可求和或平均；汇总须用 `SAFE_DIVIDE(SUM(ad_purchases), SUM(clicks))` |
| 广告 GMV | `ad_gmv` / `ad_gmv_usd` | 视频 | API `shop_gross_revenue_by_order_submission`（后台 Gross revenue (Shop)），按视频汇总。`ad_gmv_usd` 经汇率字典转美元，缺币种保持空值，不按 1 折算 | 无 | `ad_gmv_usd` 可加；原币跨币种不可加 |
| RPA GMV | `rpa_gmv` / `rpa_gmv_usd` | 视频 | TikTok Shop 后台 RPA 视频成交，按 `stats_date × tiktok_video_id` 跨商品求和（含无商品名的行）。`rpa_gmv_usd` 转美元规则同上 | 无 | `rpa_gmv_usd` 可加；原币跨币种不可加 |
| 销售金额 | `sales_amount_usd` | 视频 | 行级 `GREATEST(广告 GMV 美元, RPA GMV 美元)`；一侧为空时用另一侧，两侧都空则空 | 无 | 可按已取 max 的行求和；不能先分别汇总两套 GMV 再取 max 来对账 |
| 播放量 | `view_increment` | 视频 | `dws_social_video_state_daily_v2` 当日增量，对齐橱窗号视频维度 | 无 | 可跨天求和（同一粒度内） |
| 点赞量 | `like_increment` | 视频 | 同上 | 无 | 可跨天求和（同一粒度内） |
| 广告件数 | `ad_items` | 视频 | API `shop_total_items_purchased`（Items purchased (Shop)），按视频汇总。看板「广告投放后带来销售件数」用这一列 | 无 | 可加 |
| RPA 件数 | `rpa_items` | 视频 | RPA `items_sold` 按视频跨商品求和 | 无 | 可加 |
| 销售件数 | `sales_items` | 视频 | 行级 `GREATEST(广告件数, RPA 件数)`；一侧为空时用另一侧，两侧都空则空 | 无 | 可按已取 max 的行求和 |
| 广告投放前自然销售件数 | `items_before_ads` | 视频 | 有 `first_ad_dt` 时：RPA 源表里 `DATE(fetched_at UTC) < first_ad_dt` 的最新快照，按商品取最新再把件数加总；`fetched_at` 为空时退到 `stats_date < first_ad_dt`。广告只有日粒度，首次消耗当天的 RPA 不算投流前。没有首次消耗日、或投流前从未抓到 RPA：空，不补 0，也不用当天 RPA 顶 | 无 | 视频级快照，同一视频跨天重复出现，不可跨天求和 |
| 广告投放后自然销售件数 | `organic_items_after_ads` | 视频 | `销售件数 - 投流前件数 - 广告件数`。投流前件数为空时本列也为空。可能为负：投流前件数是累计快照，销售件数是当日值，相减没有业务保证非负。保留负数，不 `GREATEST(..., 0)` | 无 | 不可当作可靠增量跨天求和 |

### ads_daily_social_affiliate_funnel

| 指标 | 字段 | 统计主体 | 口径 | 必要过滤 | 聚合限制 |
|---|---|---|---|---|---|
| 统计日 | `dt` | 自然日 | 骨架按 `America/Toronto` 切日，只出已结束的美东日。从橱窗号快照最早日铺到昨天；缺快照的日子沿用最近一次快照，不记 0 | 无 | 不聚合 |
| region | `region` | 账号 | `dim_ecn_account_affiliation.affiliation_label`（联盟美国 / 联盟墨西哥）。从未绑上过保持空值，不补「未知」。看板用单选 filter 一次只看一个地区，不把各地区叠在同一张图里 | 按目标地区过滤 | 不聚合 |
| 活跃的联盟号 | `active_account_cnt` | 账号 | 当天快照里的 `tiktok-affiliate` 账号，去掉当时已被封橱窗的号，按 region 计数。看 `tiktok_showcase_status`，不看 `channel_status`：`unknown` / `not_applicable` / 空（接口 404 从未拉到状态）算活跃；`permanently_banned` 且 `DATE(tiktok_showcase_status_updated_at, America/Toronto) <= dt` 算不活跃，封禁日当天不算活跃 | 按目标地区过滤 | 当天存量，不可跨天求和 |
| 联盟号当天发布的视频数 | `published_video_cnt` | 视频 | `dim_video.published_dt = dt` 且发布账号当天仍活跃。`pre_binding` 已在 `dim_video` 排除 | 只统计活跃联盟号 | 可跨天求和 |
| 在投广告的视频数 | `advertised_video_cnt` | 视频 | 横轴 `dt` 是视频发布日。当天发布、后来有过 `spend > 0` 的视频数，用 DWS `first_ad_dt IS NOT NULL`。发布日当天账号必须仍活跃 | 只统计活跃联盟号里已投流的视频 | 可跨天求和 |
| 新增投放广告的视频数 | `new_ad_video_cnt` | 视频 | 横轴 `dt` 是视频发布日。当天发布且当天就开始投：`first_ad_dt = published_dt`。发布日当天账号必须仍活跃 | 只统计活跃联盟号里当天开投的视频 | 可跨天求和 |
| 第一次投放 ROI>2.5 的视频数 | `first_run_hit_video_cnt` | 视频 | 横轴 `dt` 是视频发布日。第一次投放 = `ad_run_cnt = 1` 的那些天，ROI = `SUM(广告 GMV 美元) / SUM(spend)`。`spend > 0` 且 ROI > 2.5 才计数。发布日当天账号必须仍活跃 | 只统计活跃联盟号里已投流的视频 | 可跨天求和；不得对 ROI 求平均后再数 |

### 数据边界

| 边界 | 说明 |
|------|------|
| 左表 | 橱窗号视频。广告名有没有「联盟」无关 |
| Join | 只用 `ods_tiktok_ad_daily.tiktok_item_id = dim_video.video_id`（`channel = tiktok`）。禁止 `username` / `display_name` / `identity_id` |
| 广告 `video_id` | 接口该字段实测全空，作废。看板上的视频 ID 用 `dim_video.video_id` |
| 投放次数 | 没有计划开关时间，只能用日消耗切段：相邻有消耗日算同一次，断一天算新的一次。同一天里停了再开看不出来 |
| 漏斗新列 | `video_play_actions` 是 2026-09-20 后加的，2026-08-07 ~ 2026-09-16 已回填；这一列为空时广告投放后带来播放点击率也空。广告投放后带来详情页购买转化率只用已有的购买数和点击数，不依赖详情页浏览列 |
| 零填充行 | 已连上广告且 spend、广告 GMV 都为 0 的行，接口会吐全零填充，看板去掉。未投流行的空广告字段不要补 0 再滤掉 |
| 时区 | 本表 `dt` 用 `America/Toronto`。广告源是 `Etc/GMT+5`（不含夏令时），RPA `stats_date` 是 Shop 后台日（历史店铺曾是 MX），`first_ad_dt` 跟广告源同一套日历日。跨源按日历日对齐，可能错位 1 小时到 1 天 |
| `fetched_at` | RPA 源表是无时区 `DATETIME`，和 `first_ad_dt` 比较时按 UTC 取日期；首次消耗当天的快照不算投流前 |
| 汇率 | `decom.currency_conversion_dict` 只有当前快照。缺币种时美元金额保持空值 |
| ECN 404 / 409 | 是业务结果，不落行、任务不失败。已有联盟地区不再覆盖。橱窗状态只在 200 时更新；404 拿不到 `tiktok_showcase_status`，按空值处理（漏斗里算活跃）。从未绑上过的账号 region 保持空 |
| 封号删号 | 成功绑定写入后地区不再覆盖。`dim_ecn_account_affiliation` 只插入未见过的成功绑定，已有地区不覆盖、不删除。橱窗状态随最近一次 200 更新。接口已经 404 的账号，按确认名单补维表地区，不写回 ODS |
| 省略 DWD | 广告日报已是报表粒度；affiliation 是实体映射，没有去重和多源打平需求 |
| affiliation 建 DIM | 绑定后地区不变；人工确认的 404 账号也写在维表。视频 ID 仍直接 join 已有 `dim_video`。橱窗状态放同一张 DIM，因为封禁日要给漏斗还原历史活跃 |
| 漏斗省略 DWS | `ads_daily_social_affiliate_funnel` 五个计数只给这一张看板用，直接读 DIM 状态 + 账号快照 + 已有视频日事实 |

## 已落地字段

需求 1–14 以及首次投放日、累计投放次数、广告 CPM / CPC / CPS、广告投放后带来播放点击率、广告投放后带来详情页购买转化率全部落到 `ads_daily_social_affiliate_project`。两套 GMV / 两套件数先入库，看板上的销售金额、销售件数取 max 后的展示列。活跃联盟号 / 当天发布视频数 / 在投广告视频数 / 新增投放视频数 / 第一次投放 ROI>2.5 落到 `ads_daily_social_affiliate_funnel`。

`ai_kol_name` 已接入 ODS → DIM → DWS → ADS；ODS 对缺失值只告警、不阻断。2026-09-23 生产链路已有非空值，Superset dataset 299 已刷新；明细 chart 727 与汇总 chart 728 均在「账号名称」右侧显示「ECN AI 博主名称」，真实查询已验证。

## 关联

- 一个视频当天可能对应多条广告，DWS 按 `dt × video_id` 把 spend / 广告 GMV / 广告件数 / 展示 / 点击 / 播放加总后再左连。
- 视频维度 RPA 按商品出多行，本表在 DWS 已按视频收口，不能再拿橱窗号视频商品 DWS 直接当件数（那张表丢掉了无商品名的行）。
- ROI、销售金额的 max、投流后自然件数、CPM / CPC / CPS / 广告投放后带来播放点击率 / 广告投放后带来详情页购买转化率都是行级派生值，看板汇总不得对这些率求平均。
- 汇总表：ROI 用 `SAFE_DIVIDE(SUM(ad_gmv_usd), SUM(spend))`；CPM / CPC / CPS / 广告投放后带来播放点击率 / 广告投放后带来详情页购买转化率同样先 `SUM` 分子分母再除；可加列 `SUM`；投流前件数是视频级快照，用 `MAX` 不 `SUM`；累计投放次数用 `MAX`；首次投放日用 `MIN`；投流后自然件数现算 `SUM(sales_items) - MAX(items_before_ads) - IFNULL(SUM(ad_items), 0)`，投流前件数为空时保持空。不得 `SUM(organic_items_after_ads)`。

## 依赖

- 原始层：`decom.ods_tiktok_ad_daily`（扩了 `tiktok_item_id`、identity、Shop GMV / 件数）；`decom.ods_ecn_account_affiliation`（地区、`ai_kol_name` + 最近一次橱窗状态）；`decom.ods_ecn_video_publication_owner`（`platform × video_id → task_id`，只补缺失映射）
- 维表层：`decom.dim_ecn_account_affiliation`（账号首次联盟地区只增不改；AI 博主名称和橱窗状态随 ODS 200 更新）
- 事实层：`decom.dws_social_affiliate_project_video_daily`（只固化可加分子分母、投流前件数、`ai_kol_name` 和 `ecn_task_id`；max / ROI / 美元 / 看板过滤在 ADS）
- 报表层：`decom.ads_daily_social_affiliate_project`；`decom.ads_daily_social_affiliate_funnel`
- 其他上游：`decom.dim_video`、`decom.dws_social_creator_snapshot_daily`、`decom.dws_social_video_state_daily_v2`、`videos.tiktok_shop_rpa_video_stats_snapshots`、`decom.currency_conversion_dict`
- 脚本：`jobs/ods/ods_tiktok_ad_daily/ods_tiktok_ad_daily.py`、`jobs/ods/ods_ecn_account_affiliation/ods_ecn_account_affiliation.py`、`jobs/ods/ods_ecn_video_publication_owner/ods_ecn_video_publication_owner.py`、`jobs/dim/dim_ecn_account_affiliation.py`、`jobs/dws/dws_social_affiliate_project_video_daily.py`、`jobs/ads/ads_daily_social_affiliate_project.py`、`jobs/ads/ads_daily_social_affiliate_funnel.py`
- DDL：`ddl/ods/ods_tiktok_ad_daily.sql`、`ddl/ods/ods_ecn_account_affiliation.sql`、`ddl/ods/ods_ecn_video_publication_owner.sql`、`ddl/dim/dim_ecn_account_affiliation.sql`、`ddl/dws/dws_social_affiliate_project_video_daily.sql`、`ddl/ads/ads_daily_social_affiliate_project.sql`、`ddl/ads/ads_daily_social_affiliate_funnel.sql`
- 编排：`env/etl_config.json` 中的独立表级入口
- 凭据：TikTok token 用 Secret Manager `prod-ads-runner-tiktok-access-token`。ECN 读未跟踪文件 `env/ecn.json`（模板 `env/ecn.json.example`），不入库
- 规则：`docs/rules/video-mapping.md` 橱窗号一节、`docs/rules/layering.md`

## 运行

```bash
python3 jobs/ods/ods_tiktok_ad_daily/ods_tiktok_ad_daily.py
python3 jobs/ods/ods_ecn_account_affiliation/ods_ecn_account_affiliation.py
python3 jobs/ods/ods_ecn_video_publication_owner/ods_ecn_video_publication_owner.py
python3 jobs/dim/dim_ecn_account_affiliation.py
python3 jobs/dws/dws_social_affiliate_project_video_daily.py
python3 jobs/ads/ads_daily_social_affiliate_project.py
python3 jobs/ads/ads_daily_social_affiliate_funnel.py
```

TikTok 广告 ODS 默认只重刷最近 4 天。2026-08-07 ~ 2026-09-16 已回填 `video_play_actions` / `onsite_on_web_detail`。更早分区如果再缺列，需要显式 `--start-date` / `--end-date`，回填前必须先问。接口带 `stat_time_day` 时单次最多 30 天，更长窗口要拆开、不要重叠。

ECN 凭据读 `env/ecn.json`（未跟踪）。账号归属每天拉全部橱窗号刷新状态，视频归属只拉没有非空 `task_id` 的 video_id；404 / 409 不落行，下次继续补缺。每 50 条写入一次；网关偶发 502 时脚本会退避重试。已停用 `promotion-entry`，投流前件数改切 `first_ad_dt`。
