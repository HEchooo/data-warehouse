# 【Social Content】橱窗号数据

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | Social Content 橱窗号数据 | 橱窗号看板、TikTok 橱窗号、橱窗号账号数据、橱窗号视频数据 |

## 报表

- Dashboard：[【Social Content】橱窗号数据（账号维度）](https://bi.alvinclub.ca/superset/dashboard/130/)（id=130）
- Dashboard：[【Social Content】橱窗号数据（视频维度）](https://bi.alvinclub.ca/superset/dashboard/131/)（id=131）
- 账号维度表：`decom.ads_daily_social_affiliate_creator`，粒度 `dt × creator_id`
- 视频维度表：`decom.ads_daily_social_affiliate_video`，粒度 `dt × video_key`
- Superset 物理 dataset：`ads_daily_social_affiliate_creator`（id=295）、`ads_daily_social_affiliate_video`（id=296），主时间列均为 `dt`
- Superset chart：`721`（账号维度日报 table）、`722`（视频维度日报 table）
- 橱窗号范围：`videos.users.usage_type = 'tiktok-affiliate'`，当前 10 个账号，只有 TikTok 有这类账号
- 视频范围：绑定前存量视频（`platform_video_tags` 的 `tag_type='origin' AND tag_value='pre_binding'`）已在 `dim_video` 落库时排除，只统计绑定后新发布的视频；1161 个橱窗号视频中排除 1114 个，剩 47 个，10 个账号全部保留
- 统计口径：所有互动指标同时给出 `当日增量` 与 `日末累计`；增量 = `D 日末累计 - D-1 日末累计`
- 成交口径：GMV / 成交商品件数 / 订单笔数只出**当日值**，不做累计列，因为源数据本身就是 TikTok Shop 后台的当日报表
- `dt` 为已完整结束的美东自然日；Superset 默认用 Advanced Time Range 将实例 `now` 向后平移 5 小时后截日，严格筛选一个完整美东日

## 字段

### 账号维度需求字段

| 需求字段 | 落地字段 | 口径 |
|------|------|------|
| 统计日 | `dt` | 美东自然日 |
| 渠道 | `channel` | 固定 `tiktok` |
| 账号 | `creator_id` / `account_name` / `account_handle` / `account_url` | `creator_id = videos.users.id`；展示字段取账号日快照，`account_handle = users.username` |
| 发布的视频数 | `new_video_count` / `total_video_count` | 当日新发布（`dim_video.published_dt = dt`）与截至当日累计已发布（`published_dt <= dt`） |
| 播放量 | `view_increment` / `total_view_count` | 账号下全部历史视频的当日增量之和、日末累计之和 |
| 点赞数 | `like_increment` / `total_like_count` | 同上 |
| 评论数 | `comment_increment` / `total_comment_count` | 同上 |
| 分享数 | `share_increment` / `total_share_count` | 同上 |
| 新增粉丝数 | `follower_increment` / `total_follower_count` | 账号日快照 `day_end_follower_count` 相减；前一日无快照时为空值，不按 0 起算 |
| GMV | `gmv` / `currency` / `gmv_usd` | TikTok Shop 后台归因 GMV（`attr_gmv`），按 `dt × account_handle` 关联；`gmv` 为后台原币金额，`gmv_usd` 用 `decom.currency_conversion_dict` 转美元，跨账号聚合金额只能用 `gmv_usd` |
| 成交商品件数 | `items_sold` | 后台 `attr_items_sold` |
| 订单笔数 | `sku_order_count` | 后台 `sku_orders_count` |

### 视频维度需求字段

| 需求字段 | 落地字段 | 口径 |
|------|------|------|
| 统计日 | `dt` | 美东自然日 |
| 视频 | `video_id` / `video_key` / `video_link` / `published_ts_utc` / `published_dt` | `video_key = 'tiktok:' + video_id`；链接与发布时间取 `dim_video` |
| 视频标题 | `video_title` | `dim_video.video_title`，TikTok 取 `videos.videos.title`；映射统一引用 `docs/rules/video-mapping.md` |
| 播放量 | `view_increment` / `total_view_count` | 单视频当日增量与日末累计 |
| 点赞数 | `like_increment` / `total_like_count` | 同上 |
| 评论数 | `comment_increment` / `total_comment_count` | 同上 |
| 分享数 | `share_increment` / `total_share_count` | 同上 |
| 账号 | `creator_id` / `account_name` / `account_handle` / `account_url` | 与账号维度同源 |
| 产品名称 | `product_title` / `product_url` | TikTok Shop 后台该视频当日挂的商品，按 `dt × video_id` 关联；一个视频当日挂多个商品就出多行，互动指标在这几行里重复出现 |
| GMV | `gmv` / `currency` / `gmv_usd` | 商品级 GMV，口径与账号维度一致，`gmv_usd` 由汇率字典换算 |
| 成交商品件数 | `items_sold` | 后台 `items_sold` |

补充说明：

- 视频当天首发时前一日没有日末状态，当日增量等于日末累计值
- 账号维度按 `dt × 橱窗号` 补齐骨架，账号当日没有任何视频也出零值行；粉丝字段保持空值语义，不补 0
- 橱窗号名单取账号日快照的最新分区；名单变化会同步影响历史分区的账号集合
- 视频维度主键是 `dt × video_key × product_title`；视频当日没有匹配到商品时 `product_title` 为空，仍保留该视频行，互动指标照常产出
- 账号维度成交口径来自 TikTok Shop 后台自己的归因结果，**不是**视频商品 GMV 的求和，两者不能互相验算

### 数据边界

| 边界 | 说明 |
|------|------|
| 报表起点 | `2026-08-12`（美东），即橱窗号视频在 `dws_social_video_snapshot_daily_v2` 的首个真实快照日；更早分区没有采集数据，不产出 |
| 存量视频排除 | 橱窗号视频绝大多数是账号绑定前的存量视频（每个账号 84%~98%），已在 `dim_video` 落库时排除。重算后每个账号只剩 3~9 个视频，受影响分区的播放、点赞、评论、分享、视频数都会大幅下降；默认只重刷最近 30 天，更早分区需要 `--full-refresh` 才会清除旧数据；粉丝数取账号快照，不受影响 |
| 采集范围扩大 | 橱窗号视频采集覆盖是逐步放开的：`08-12/08-13` 约 162 个视频、`08-14 ~ 08-16` 约 420 个、`08-17` 起才覆盖全部 1146 个视频。`08-14` 与 `08-17` 的播放、评论、分享增量包含新纳入采集视频的历史存量，不代表当日真实增量，`08-17` 之前的增量口径不可用于趋势判断 |
| 粉丝数历史 | `videos.users` 只有当前状态，没有历史，粉丝快照从 `dws_social_creator_snapshot_daily` 首次运行日（`2026-08-21`）起逐日累积；`2026-08-22` 起才有第一个可用的 `follower_increment`，更早分区的粉丝字段恒为空 |
| 分享数平台差异 | `day_end_shares` 只有 YouTube 保持空值（源表没有该字段）；TikTok 与 Instagram 与播放、点赞、评论一致按 0 起算。橱窗号只涉及 TikTok，两个维度的分享数字段都不会出现空值 |
| 历史评论/分享 | 两个日末字段是本次新增列，已于 `2026-08-21` 用 `--full-refresh` 从 `2026-03-13` 全量回刷完成，三端历史分区均已有值 |
| 成交数据起点 | RPA 抓取的成交数据从 `2026-08-26` 才有第一个统计日，更早分区的 `gmv` / `items_sold` / `sku_order_count` / `product_title` 恒为空 |
| 成交数据账号覆盖 | 截至 `2026-08-28`，RPA 只覆盖 10 个橱窗号中的 `kristdmyxkv`、`rheaezuowg4` 两个。**未采集到的账号成交字段是 `NULL`，不是 0** —— 补成 0 会让「橱窗号总 GMV = 0」看起来像真结论 |
| 成交数据视频覆盖 | 即使被覆盖的账号，也只采到部分视频（`kristdmyxkv` 9 个非存量视频采到 5 个、`rheaezuowg4` 7 个采到 4 个），且这些视频里只有少数匹配到商品；`product_title` 为空是「后台没匹配到商品」，不是数据错误 |
| 商品归因可靠性 | 源表 `product_matched_by` 当前全是 `publish_date`，即视频与商品的关联是**按发布日期推断**的，不是 TikTok 官方归因。视频维度的 GMV 只能当参考，不能当结算依据 |
| 成交数据时区 | `stats_date` 是 TikTok Shop 后台的统计日（当前店铺 `region=MX`），直接对齐到美东 `dt`，未做时区换算，可能存在跨日错位 |
| 汇率历史 | `decom.currency_conversion_dict` 只有当前快照，没有历史。历史分区回刷时 `gmv_usd` 会按当天汇率重算，与 Makepod 同口径；字典缺该币种时 `gmv_usd` 保持空值并打 warning，不会静默按 1 折算 |
| 成交数据延迟 | RPA 是 T+1 抓取（`2026-08-26` 的数据在 `2026-08-27 11:32` 才落地），且当天内会重复抓取多次；两个 ADS 都固定回看最近 3 天的成交统计日，超过 3 天的历史补数需要 `--full-refresh` |

## 依赖

- 账号快照：`decom.dws_social_creator_snapshot_daily`（`videos.users` 逐日快照，只重写当天分区，历史不可回溯）
- 视频播放事实：`decom.dws_social_video_snapshot_daily_v2` → `decom.dws_social_video_state_daily_v2`
- 视频维表：`decom.dim_video`
- 视频商品成交事实：`decom.dws_social_affiliate_video_product_daily`（`dt × video_key × product_title`），通过 `dim_video` 限定视频范围
- 账号成交事实：`decom.dws_social_affiliate_creator_shop_daily`（`dt × account_handle`）
- 成交源表：`videos.tiktok_shop_rpa_video_stats_snapshots`、`videos.tiktok_shop_rpa_account_stats_snapshots`（RPA 抓取 TikTok Shop 后台），汇率来自 `decom.currency_conversion_dict`
- 脚本：`jobs/dws/dws_social_creator_snapshot_daily.py`, `jobs/dws/dws_social_affiliate_video_product_daily.py`, `jobs/dws/dws_social_affiliate_creator_shop_daily.py`, `jobs/ads/ads_daily_social_affiliate_creator.py`, `jobs/ads/ads_daily_social_affiliate_video.py`
- DDL：`ddl/dws/dws_social_creator_snapshot_daily.sql`, `ddl/dws/dws_social_affiliate_video_product_daily.sql`, `ddl/dws/dws_social_affiliate_creator_shop_daily.sql`, `ddl/ads/ads_daily_social_affiliate_creator.sql`, `ddl/ads/ads_daily_social_affiliate_video.sql`
- 编排：`env/etl_config.json` 中的独立表级入口 `dws_social_creator_snapshot_daily`、`dws_social_affiliate_video_product_daily`、`dws_social_affiliate_creator_shop_daily`、`ads_daily_social_affiliate_creator`、`ads_daily_social_affiliate_video`
- 展示字段映射：`docs/rules/video-mapping.md`
- 省略 `ODS` / `DWD` 的原因：`videos.*` 是服务端库在 BigQuery 的 Datastream 镜像，本身已是保真副本，沿用 `dws_social_creator_snapshot_daily` 的既有先例由 `DWS` 直读；RPA 源表只需要「一天多次抓取取最后一次」这一步去重，放在 `DWS` 一并完成，单独建 `DWD` 等价于 `SELECT * FROM 上一层`
- 两张成交 `DWS` 走全表重建（源表只有百行量级），没有水位判定，也就没有漏刷分区的路径
- 性能：两张 ADS 每个 `dt` 只读取 D / D-1 两个日末状态分区，并先按橱窗号名单收窄到 10 个账号；账号快照每天只写 1 个分区、10 行
