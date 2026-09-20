# 【Affiliate】投放数据看板 排查经验

## 对账号不要用 username，对视频不要用广告 video_id

- 广告 `/identity/get/` 的 `identity_id`（BC_AUTH_TT 是 UUID）对不上内部 `users.id` / `open_id` / `union_id`。
- `/ad/get/` 的 `video_id` 实测全空。素材 ID 只在 `tiktok_item_id`，等于 `dim_video.video_id`。
- `username` 只给 ECN affiliation 查询和看板展示用。排查对不上视频时，先看 ODS 的 `tiktok_item_id` 是不是空（历史分区在扩列前回填前会是空）。

## Gross revenue (Shop) 不是 GMV Max 的 gross_revenue

- 后台列 Gross revenue (Shop) 对应 AUCTION 报表 `shop_gross_revenue_by_order_submission`。
- AUCTION 里请求 `gross_revenue` 会被拒。那是 GMV Max 全渠道收入，不能填进本看板。
- ROI 用 `广告 GMV / spend` 现算。接口 `onsite_shopping_roas` 是四舍五入到两位的同一比值，不能拿去 SUM / AVG。

## ECN 的 404 / 409 不是任务失败

- affiliation 查不到或不是美墨联盟：ODS 落一行空 `affiliation_label`。看板 region 读 `dim_ecn_account_affiliation`：曾经绑成功过的账号继续用首次地区，从未绑上过才空。
- promotion-entry 尚未投流：落一行空 `ads_first_sent_at`，广告字段保持空，投流前件数也保持空。
- ECN 网关会偶发 `502 Bad Gateway`。ODS 对 502 / 503 / 504 重试 5 次（2/4/8/16/32s）；HTML 错误页不要当 JSON 解析。

## 账号被封后 ECN 会删掉，当天接口再也给不出地区

- 现象：橱窗号封禁后，`account-affiliation` 变成 404，ODS 整表覆盖，`affiliation_label` 变空，两个 Affiliate 看板的 region 跟着丢。
- 原因：绑定一次之后地区不会变，但 ODS 只保存当天接口结果，源删了等于历史也没了。
- 修法：成功绑定写入 `dim_ecn_account_affiliation`，只插新账号、不改已有地区。DWS / 看板只读这张维表。
- 接口已经 404 的账号，按业务确认名单补进维表，不改 ODS。ODS 继续留当天 404，用来判断「上游现在已经没有这个账号了」。

## 没投流不要用当天 RPA 顶「投流前件数」

- 列名是投流前自然销售，没有 `ads_first_sent_at` 时这列不适用。
- 曾经用当天 `rpa_items` 兜底，同一视频跨天数字会跟着当天成交变，看起来像口径错了。
- 现口径：只有投流前快照才落数；没投流或没有投流前快照保持空，不补 0。空投流前件数时「投流后自然件数」也保持空。

## 看板 `Unknown column used in orderby: dt`

- table 图为了中文列名，`all_columns` 用了 `sqlExpression: "dt "`（末尾空格）。
- 看板时间筛选会按 `order_by_cols` 重写排序。写成 `["dt ", false]` 时，后端按字符串列名 `dt ` 去找，找不到就报这个错。
- 排序必须用不带空格的物理列：`order_by_cols = ['["dt", false]']`，`query_context.queries[0].orderby = [["dt", false]]`。两处都要改。

## 可空日期不要标成时间列

- `first_ad_dt` 很多行是空的。标成 temporal / `is_dttm` 后，table 会先转成毫秒时间戳再格式化，空值会把图表打成 `Unexpected error`。
- 看板上用 `CAST(first_ad_dt AS STRING)` 展示，并且不要放进 `temporal_columns_lookup`。`dt` / `published_dt` 不会空，可以继续当时间列。

## 广告扩展列只对回填后的分区有效，历史分区 join 不上视频

- `tiktok_item_id`、`shop_gross_revenue_by_order_submission`、`shop_total_items_purchased` 是后加进 `ods_tiktok_ad_daily` 的列。
- 2026-09-14 之前的分区这三列全空（实测 629 行全空），但 `spend` 有真实值，钱是花了的。
- 后果：看板靠 `tiktok_item_id = video_id` 连视频，历史分区连不上，ROI / 广告 GMV / 广告件数整段为空。看起来像「那段时间没投流」，实际是缺 join key。
- 判断方法：先查 ODS 同期 `SUM(spend)`。有 spend 但 `COUNTIF(tiktok_item_id IS NOT NULL) = 0`，就是缺回填，不是没投流。
- 修法：`ods_tiktok_ad_daily.py --start-date/--end-date` 显式回填，再重跑 DWS / ADS。回填前必须先问用户。
- 2026-09-18 已回填 2026-08-07 ~ 2026-09-13，629 行全部补上 `tiktok_item_id`，551 行补上 Shop 指标。

## 回填广告 ODS 的两个硬限制

- `/report/integrated/get/` 带 `stat_time_day` 时**单次窗口最多 30 天**，超了报 `code=40002 max time span is 30 days when use stat_time_day`。38 天要拆成两段跑，窗口不要重叠。
- Secret Manager 的 Python 客户端默认走 gRPC，部分网络出口会拦（`503 tcp handshaker shutdown`，`bq` / `gcloud` 却正常）。`SecretManagerServiceClient(transport="rest")` 走普通 HTTPS，本地和 GCP 都通。
- 回填后 `spend > 0` 但 `ad_gmv = 0` 的日期是真实的「投了没成交」，不是缺数。`spend` 全为 0 的日期会被零填充过滤规则去掉（例如 2026-08-23），看板上不出现。

## 投放次数只能用日消耗切段，切不到几点开停

- 没有计划 `schedule_start_time`，ECN `ads_first_sent_at` 又几乎是空的。
- 现口径：有消耗的连续日历日算一次。9/19 投一天、9/21 再投，就是第 1 次和第 2 次。
- 同一天里停了再开、当天投了几小时，都看不出来。

## 广告投放后带来详情页购买转化率是购买数 / 点击数

- 分母是目标页点击 `clicks`，分子是成交笔数 `ad_purchases`。不是进详情页之后的转化。
- 不要用站外像素 `product_details_page_browse`（橱窗号实测全 0），也不要用站内详情页浏览 `onsite_on_web_detail` 当这一列的分母。

## `/identity/get/` 的 page_size 上限是 100

- 报表 `/report/integrated/get/` 可以 `page_size=1000`。identity 接口传 1000 会报 `code=40002 page_size: number must be most 100`。
- 返回列表在 `data.identity_list` 而不是报表那种 `data.list`；`page_info` 可能全是 0，不能靠 `total_page` 翻页。
