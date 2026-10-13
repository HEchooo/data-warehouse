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

- affiliation 查不到或不是美墨联盟：不落新行，下次继续补。看板 region 读 `dim_ecn_account_affiliation`：曾经绑成功过的账号继续用首次地区，从未绑上过才空。
- ECN 网关会偶发 `502 Bad Gateway`。ODS 对 502 / 503 / 504 重试 5 次（2/4/8/16/32s）；HTML 错误页不要当 JSON 解析。2026-09-23 对 32 个不同账号做了 3 轮 32 并发只读测试，均无超时、限流或 5xx；ODS 请求并发上限设为 32，每 50 个账号完成后再处理下一批，整轮结果合并写入。

## ECN 采集每轮只换表一次

- 2026-09-23 起，每 50 个账号都执行字段 `ALTER TABLE` 和整表换名，触发 BigQuery `too many table update operations`；`etl_run.py` 遇错退出，使排在后面的素材维表和周报一起停更。
- 保留每 50 个账号一批的接口请求节奏，但本轮成功结果合并后只执行一次 staging 校验与换表；已有字段不再重复修改 schema。调度中断超过 6 小时后，仍需单独确认并补刷遗漏的素材维表和事实分区。

## ECN 视频归属接口不能照搬账号接口的并发量

- 2026-09-24 生产 ETL 调用 `publication-owner` 时，32 并发下首批大量连接超时、随后返回 502，100 个视频没有查到成功映射，熔断后阻断了排在后面的素材链路。同一生产机对单个视频连续 3 次请求，以及对 4 个视频同时请求，均在 0.1 秒内得到正常业务状态 404；这只能证明探测时 4 并发可达，不能证明并发是唯一原因。
- 视频归属请求先降至 4 并发，连接超时设为 5 秒，临时连接错误及 502/503/504 各重试一次；连续 3 次不可用仍失败报警。生产重新运行后要检查成功数与失败数；若低并发仍失败，继续排查 ECN 网关及后端日志，不把失败视为成功。

## account-affiliation 的 `ai_kol_name` 按账号返回

- 2026-09-22 抽查 5 个账号和 Katerina Vale 对应账号时，接口均为 HTTP 200，但没有 `ai_kol_name`。
- 2026-09-23 重试 Katerina Vale 对应账号，接口仍为 HTTP 200，返回 `ai_kol_name = "Vivian Lockhart"`。字段可能按账号或时间逐步出现，不能假设每个成功响应都有值。
- ODS 已按 `payload.get("ai_kol_name")` 透传；缺字段时记录 warning，不阻断账号归属更新。直接接口响应有值后，还需日常采集写入 ODS，再沿 ODS → DIM → DWS → ADS 传递。

## 账号被封后不要靠当天接口重刷地区

- 现象：橱窗号封禁后，`account-affiliation` 变成 404。旧逻辑整表覆盖会把 `affiliation_label` 变空，两个 Affiliate 看板的 region 跟着丢。
- 现口径：每天仍会请求全部橱窗号以刷新 `tiktok_showcase_status`。404 / 409 不落新行、不改已有 ODS 行。`dim_ecn_account_affiliation` 地区只插新账号，橱窗状态只在 200 时更新。
- 接口已经 404 的账号，按业务确认名单补进维表地区，不改 ODS。漏斗里没有橱窗状态的账号按活跃计。
- 橱窗活不活看 `tiktok_showcase_status`，不看 `channel_status`：`permanently_banned` 为封禁，封禁时间用 `tiktok_showcase_status_updated_at`；`unknown` / `not_applicable` 为活跃。

## 漏斗缺快照日要沿用最近一次快照，不能记 0

- `dws_social_creator_snapshot_daily` 橱窗号分区从 2026-08-21 才有，中间 2026-08-29 缺一天。
- 若按当天精确匹配，活跃联盟号会掉成 0，当天发布的视频（包括唯一一条第一次投放 ROI>2.5 的 `tiktok:7679375949170789652`）也会从漏斗消失。
- 现口径：每个 `dt` 取 `<= dt` 的最近一次橱窗号快照。更早的 `published_dt` 仍然不进这张表。

## 没投流不要用当天 RPA 顶「投流前件数」

- 列名是投流前自然销售，没有 `first_ad_dt` 时这列不适用。
- 曾经用当天 `rpa_items` 兜底，同一视频跨天数字会跟着当天成交变，看起来像口径错了。
- 现口径：只有首次消耗日之前的 RPA 快照才落数；没投流或没有投流前快照保持空，不补 0。空投流前件数时「投流后自然件数」也保持空。
- 不要用 ECN `ads_first_sent_at`：534 条视频里只有 9 条有值，真正有消耗的 84 条里 76 条 ECN 是空的，而且同一批发送时间精确到同一毫秒。

## 投放次数只能用日消耗切段，切不到几点开停

- 没有计划 `schedule_start_time`。
- 现口径：有消耗的连续日历日算一次。9/19 投一天、9/21 再投，就是第 1 次和第 2 次。
- 同一天里停了再开、当天投了几小时，都看不出来。

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

## 广告投放后带来详情页购买转化率是购买数 / 点击数

- 分母是目标页点击 `clicks`，分子是成交笔数 `ad_purchases`。不是进详情页之后的转化。
- 不要用站外像素 `product_details_page_browse`（橱窗号实测全 0），也不要用站内详情页浏览 `onsite_on_web_detail` 当这一列的分母。

## Chart 的 `datasource_id` 空了看板会一直转圈

- 现象：【Affiliate】投放数据看板一直转圈。点进 chart 显示 `Missing dataset / The dataset linked to this chart may have been deleted.`
- 根因：dataset `299` 还在，chart `params` / `query_context` 里也还是 `299__table`，但 `slices.datasource_id` 是空的。Dashboard bootstrap 按独立关系字段重装 `form_data.datasource`，变成 `None__table`。
- 修法：`PUT /api/v1/chart/{id}` 写回 `datasource_id=299`、`datasource_type=table`。727 / 728 都要补。
- 验收不能只看单个 chart：`GET /api/v1/dashboard/134/charts` 的 `form_data.datasource` 必须是 `299__table`。

## 看板播放量是 0，ECN 详情页却有 views

- 现象：汇总或最近几天的日报里某条视频「播放量」是 0，点 `ecn_detail_url` 却能看到几十上百 views。例：`7674588645512285448`，ECN 任务 `898481ac-5a60-4f53-8b39-4cd018431293` 显示 81 views。
- 看板「播放量」是 `SUM(view_increment)`，只加所选日期里的新增播放。ECN 页面是这条视频的累计播放。
- 先看日末累计，不要只看增量。这条视频 `video_stats_log` 最后一次采集是 2026-09-15，`view_count = 83`；日末状态之后沿用 83。全历史增量加总也是 83。2026-09-16 及之后每天增量都是 0，这段日期筛出来就是 0。
- 81 和 83 是同一件累计播放，差在采集时点。播放量显示 0 不是没采到。

## `/identity/get/` 的 page_size 上限是 100

- 报表 `/report/integrated/get/` 可以 `page_size=1000`。identity 接口传 1000 会报 `code=40002 page_size: number must be most 100`。
- 返回列表在 `data.identity_list` 而不是报表那种 `data.list`；`page_info` 可能全是 0，不能靠 `total_page` 翻页。

## ECN 批量查询应在线程内复用连接

- 2026-10-13 只读排查发现：连续三轮账号归属请求均只有 64 个到达 ECN，后续出现连接超时，紧接着的视频归属任务也熔断；稍后用原凭据单独查询，两接口均返回 200。出口 NAT 端口耗尽是待确认假设，云网络配置和监控查询因权限不足未完成，不能作为已确认根因。
- `ods_ecn_account_affiliation` 原来每个账号创建并关闭一个 `requests.Session`。改为每个工作线程保留一个 Session，跨批次复用连接池，不跨线程共享 Session；并发数、业务状态码处理、失败上报和写入逻辑保持原样。两个任务都在工作线程退出后显式关闭 Session；回调注册加锁，避免关闭仍在使用的连接。
- `ods_ecn_video_publication_owner` 已使用线程本地 Session，没有每个视频新建连接的问题；本次补上与账号任务相同的生命周期清理。
- 账号任务连接超时设为 5 秒、读取超时保留 60 秒，首次连接/读取异常等待 2 秒后重试一次；第二次尝试及之后的连接异常直接上报。502 / 503 / 504 保留最多 5 次总尝试，最后一次不再等待；被丢弃的重试响应及时关闭。视频任务保留原来的重试和熔断阈值，并关闭触发熔断的响应。
- 本地回归使用 HTTP/1.1 服务对两个任务分别发送 80 次请求，包含 200 / 404 / 409、两个线程和两个批次，验证每个任务只建立 2 条 TCP 连接。入口：`.venv/bin/python scripts/ecn_connection_test.py`；测试不读取生产凭据、不访问 BigQuery。
- 连接复用依赖服务端支持持久连接；本地验证不代表线上网络故障已消除，上线后仍需核对实际连接数和完整任务结果。
