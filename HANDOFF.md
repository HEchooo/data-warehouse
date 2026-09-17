# Handoff

## 1. Project Overview

- **Project**: `/Users/sinn/Documents/guyin/data-warehouse`
- **Tech Stack**: Python + BigQuery + Superset。TikTok 投放走 Business API `v1.3`，橱窗号自然数据走 `videos.*` Datastream 镜像。
- **Brief Description**: 新建看板【Social Content】联盟项目数据看板。调研已完成、口径已定，本 session **没有写任何生产代码**。下一 session 按本文执行建表、ETL、主题文档和 Superset 看板。

## 2. Current Session Goals

- 原始目标：做联盟项目数据看板，字段 1–14 见下文需求清单。
- 用户当务之急曾是：先搞清 TikTok Ads Manager 怎么调、怎么和内部账号/视频对应。
- 已确认变更：
  - 不要用 `username` 当 join key。
  - 不要按广告名是否含「联盟」圈数。
  - 骨架是内部橱窗号，**左连接**广告数据。
  - 广告侧销售金额对准后台列 **Gross revenue (Shop)**，不是 GMV Max 的 `gross_revenue`。
  - ROI 用分子分母现算，不依赖接口四舍五入后的 ROAS 去做汇总。
- **下一 session 目标**：落地链路 + 看板。不改调研结论，除非执行中发现接口/数据打脸。

## 3. What Was Done

只做了只读调研。仓库无功能 diff。临时探针写在 `/tmp`，已删除。

- 复用现有 Makepod 投放同一广告主、同一 token，打通 `/ad/get/`、`/adgroup/get/`、`/identity/get/`、`/report/integrated/get/`。
- 用截图广告组 `AR-AG-48-联盟美国_2` / Showcase `Isla Rayne` 做了端到端对齐。
- 确认了 Gross revenue (Shop)、Items purchased (Shop)、ROI 的 API 名，并用 7 条有成交广告对过 ROI 现算。
- 确认内部 `videos.users` 没有能对上广告 `identity_id` 的字段。

Git：当前在 `main`。未跟踪文件只有 `AI博主外部查询接口文档.md`（需求依赖，保留）。不要提交凭据文件。

## 4. Key Decisions & Rationale

1. **圈数**：`videos.users.usage_type = 'tiktok-affiliate'` 的账号及其 `dim_video` 视频为左表。广告名有没有「联盟」无关。橱窗号快照现在远不止文档里写的 10 个，以表里实际账号为准，不要写死 10。
2. **Join key**：只用视频 ID。`/ad/get/` 的 `tiktok_item_id` = `videos.videos.id` = `dim_video.video_id`（channel=`tiktok`）。再经 `dim_video.creator_id` = `videos.users.id` 回到账号。禁止 `username` / `display_name` / `identity_id` 当主键。
3. **广告 `video_id` 字段作废**：实测 248 条全空。素材 ID 只在 `tiktok_item_id`。
4. **账号 identity**：`/identity/get/` 有 `identity_id`（BC_AUTH_TT 为 UUID），对不上内部 `users.id` / `open_id` / `union_id`。`username` 只做展示和外部 affiliation 查询入参。
5. **不要复用 Makepod 看板过滤**：现有 dashboard 133 固定 `store_id = '7494736378860701060'`。联盟橱窗 Showcase 是别的 store_id。现有 `ods_tiktok_ad_daily` 里其实已经有这些广告行，但缺 identity、`tiktok_item_id`、Shop GMV/件数。
6. **广告侧金额**：后台列 Gross revenue (Shop) = API `shop_gross_revenue_by_order_submission`。这是**广告归因**的 Shop GMV，不是 GMV Max 全渠道 `gross_revenue`。AUCTION 报表里请求 `gross_revenue` 会被拒。
7. **ROI**：`SAFE_DIVIDE(广告 GMV, spend)`。接口 `onsite_shopping_roas` 就是这个比、两位小数。入库只落可加分子分母；汇总禁止对 ROI 求和/平均。
8. **过滤**：排除「已经连上广告」且 `spend=0` 且广告 GMV=0 的行（接口会吐全零填充）。未投流视频左连接后广告字段为空，应保留，不要当成 0 再滤掉。
9. **两套 GMV / 两套件数都要入库**，看板上的「销售金额」「销售件数」取更大值。金额转 USD 走 `decom.currency_conversion_dict`，缺币种保持空值，不按 1 折算。
10. **新主题**，不要塞进【Social Content】橱窗号数据或 Makepod TK 广告投放。业务线：`TikTok 橱窗号`。表名不要带 `makepod`。

### 建议分层（执行时按 `docs/rules/layering.md` 再判断，不要空转层）

| 层 | 建议 | 原因 |
|---|---|---|
| ODS | 必须 | 三个外部源：TikTok Ads、ECN affiliation、ECN promotion-entry |
| DWD | 默认省略 | 广告日报已是报表粒度；affiliation / promotion-entry 是实体快照 |
| DIM | 仅当映射被多主题复用才建 | 当前视频 ID 映射只服务本看板时，DWS 里 join `dim_video` 即可 |
| DWS | 必须 | 橱窗号骨架 + 广告 + RPA + 投流时间的口径只应写一遍 |
| ADS | 必须 | 看板粒度、取 max、ROI 派生、中文展示 |

TikTok 广告 ODS：**优先扩现有** `ods_tiktok_ad_daily`（同一广告主、同一报表接口，避免两套任务打同一 API）。用 `ALTER TABLE` 追加列，不要改原始 `CREATE TABLE`。Makepod 下游脚本只 select 旧列，不受影响。若扩表会把 Shop 指标和 identity 污染 Makepod 口径，再拆新表。

ECN 两个 GET 必须单独 ODS，禁止 ADS 直打 HTTP。

## 5. Current State

- **Working / Stable**：调研结论已用生产 token 实测。参考看板 133、橱窗号 130/131 可运行。
- **In Progress**：本需求零代码。
- **Broken / Failing**：无。

## 6. Unfinished Work & Next Steps

按这个顺序做，做完一层再往下：

1. 读 `docs/rules/layering.md`、`code.md`、`topics.md`、`pipeline.md`，以及两个参考主题 fact：
   - `docs/topics/【Social Content】Makepod 店铺 TK 广告投放数据/fact.md`
   - `docs/topics/【Social Content】橱窗号数据/fact.md`
2. 建主题目录 `docs/topics/【Social Content】联盟项目数据看板/`，`fact.md` 先写完整需求字段，再写 `已落地字段`。同步改 `docs/topics/index.md`。
3. 扩/建 TikTok 广告 ODS：在现有 `/report/integrated/get/` 上增加 Shop 指标；`/ad/get/` 增加 `tiktok_item_id`、`identity_id`、`identity_type`、`display_name`；必要时 `/identity/get/` 补 `username`（只入库展示，不当 join key）。DDL 用 `ALTER`。表变更走 **bigquery-safe-table-change skill**。
4. 建 ECN ODS：`account-affiliation`、`videos/promotion-entry`。Key 从生产配置 `ACCOUNT_CHANNEL_API_KEY` 读，禁止写入仓库或对话。Host 用现有 ECN API 域名，仓库文档是占位符。
5. DWS：橱窗号视频日骨架左连广告（`tiktok_item_id`）。固化可加分子分母：spend、广告 GMV、RPA GMV、广告件数、RPA 件数、投流前件数、投流后广告件数。派生值不要落 DWS。
6. ADS：取 max、转 USD、现算 ROI、拼 region / 账号名 / 视频链接。`ADS` 禁止读 ODS / `videos.*`。
7. `env/etl_config.json` 加独立表级入口和 `depends_on`。
8. Superset：用 **superset skill** 建 dataset / table chart / dashboard。视频列单元格展示字符串 `video_url`，超链接指向 `dim_video.video_link`。
9. 提交前跑 `python3 scripts/check_knowledge.py`。直接在 `main` 提交（用户未要求分支）。
10. **回填生产数据前必须再问用户**，不能自行刷历史分区。

## 7. Blockers & Open Questions

执行中只需默认，不必卡住：

- 播放量/点赞量需求没写增量还是累计。默认对齐橱窗号视频维度：出当日增量；若看板只要一列，用当日增量。
- 「投流前自然件数」的 RPA 时间字段：源表有 `stats_date` 和 `fetched_at`。默认用 `fetched_at < ads_first_sent_at` 取最大一条（按 `tiktok_video_id`，件数跨商品求和）。若 `fetched_at` 缺失再退到 `stats_date`。
- `ads_first_sent_at` 为 UTC；广告 `dt` 是 `Etc/GMT+5`；RPA `stats_date` 是 Shop 后台日（历史店铺曾是 MX）。跨源对齐时不要假装同一时区。
- affiliation 404 / 非联盟标签：`region` 空值，不要补「未知」。
- promotion-entry 404 / 尚未投流：广告字段空，投流前件数 = 该视频当前 RPA 件数（或空，若从未抓到）。
- ECN host 与 API key 不在仓库。下一 session 从生产配置取，不要把 key 写进文档。

不需要再问用户、已经拍板的：join 用视频 ID；左表是橱窗号；Gross revenue (Shop) 用 `shop_gross_revenue_by_order_submission`；ROI 现算。

## 8. Important Context & Gotchas

### 需求字段（看板一行 ≈ `dt × 视频`）

| # | 看板字段 | 口径 |
|---|---|---|
| 1 | dt | 报表日。广告指标按广告主时区 `Etc/GMT+5` 切日；自然播放/RPA 现有链路是 `America/Toronto`。fact.md 必须写清本表用哪一个，以及跨源可能差 1 小时。 |
| 2 | region | ECN `GET /api/v1/open-api/account-affiliation?platform=tiktok&account_identifier=`，取 `affiliation_label`（联盟美国 / 联盟墨西哥）。入参用内部 `username`（可带 `@`）。 |
| 3 | 账号名称 | `videos.users.display_name`（经 `dim_video.creator_id`），不是广告 `identity_id`。 |
| 4 | 视频发布日期 | `dim_video.published_dt` |
| 5 | 视频链接 | 单元格文字固定 `video_url`，href = `dim_video.video_link` |
| 6 | 广告投放 ROI | `广告GMV / spend`，倍数。明细可单行现算；汇总必须 `SUM/SUM`。 |
| 7 | 广告投放成本 | `spend` |
| 8 | 销售金额 | 入库两列：广告 `shop_gross_revenue_by_order_submission`、RPA `gmv`（转 USD）。展示 `GREATEST` 后的美元。 |
| 9 | 播放量 | `dws_social_video_state_daily_v2`（见橱窗号视频主题） |
| 10 | 点赞量 | 同上 |
| 11 | 销售件数 | 入库两列：广告 `shop_total_items_purchased`、RPA `items_sold`。展示 `GREATEST`。 |
| 12 | 广告投放前自然销售件数 | ECN `GET /api/v1/open-api/videos/promotion-entry?platform=tiktok&video_id=` 得 `ads_first_sent_at`；在 `videos.tiktok_shop_rpa_video_stats_snapshots` 中取该时间之前最新快照的件数。 |
| 13 | 广告投放后带来销售件数 | Ads Manager `shop_total_items_purchased`（Items purchased (Shop)） |
| 14 | 广告投放后自然销售件数 | `销售件数 - #12 - #13`。可能为负，保留负数并在 fact.md 写风险，不要 `GREATEST(...,0)` 除非用户要求。 |

看板过滤：已关联广告且 spend=0 且广告 GMV=0 的去掉。

### TikTok API（与 Makepod 投放同源）

```
base = https://business-api.tiktok.com/open_api/v1.3
advertiser_id = 7667456906528735239
token = Secret Manager prod-ads-runner-tiktok-access-token
时区 = Etc/GMT+5（/advertiser/info/ 的 timezone，传参改不了）
```

调用模板见 `jobs/ods/ods_tiktok_ad_daily/ods_tiktok_ad_daily.py`。

| 接口 | 用途 | 注意 |
|---|---|---|
| `GET /report/integrated/get/` `AUCTION`+`BASIC`+`AUCTION_AD` | spend、Shop 成交 | dimensions 至少 `ad_id`+`stat_time_day` |
| `GET /ad/get/` | `tiktok_item_id`、identity、`showcase_products[].store_id` | `fields` 必须是白名单。`shopping_ads_type` 不在 ad 层 |
| `GET /adgroup/get/` | `product_source=SHOWCASE`、`shopping_ads_type=VIDEO` | `objective_type` 不在 adgroup fields 白名单 |
| `GET /identity/get/` | `username`、`display_name` | 只展示 |

报表 metric（AUCTION 已实测）：

| 后台 | API | 状态 |
|---|---|---|
| 消耗 | `spend` | OK |
| Gross revenue (Shop) | `shop_gross_revenue_by_order_submission` | OK，要对准截图这列 |
| Total complete payment value (onsite) | `total_onsite_shopping_value` | OK，当前样本与上列相等，仍建议两列都落 ODS 以便对账 |
| Items purchased (Shop) | `shop_total_items_purchased` | OK |
| Purchases by order submission | `shop_total_purchase_by_order_submission` | OK，是笔数不是件数；`mx-08` 上 17 vs 件数 24 |
| ROI | `onsite_shopping_roas` | OK，约等于 GMV/spend 四舍五入到 2 位 |
| GMV Max 总收入 | `gross_revenue` | AUCTION **拒绝** |

ROI 对账样本（窗口 2026-08-07 ~ 当天，ad 级合计）：

- spend 45.55 / GMV 44.33 / API ROI 0.97 / 现算 0.9732
- spend 98.28 / GMV 248.87 / API ROI 2.53 / 现算 2.5323

Isla 对齐样本：

- adgroup `AR-AG-48-联盟美国_2`
- `tiktok_item_id` `7685227707055050015`
- 内部 `creator_id` `-000kAzWv_ouhdEDzgo2LFzk50TZBN6MIdu8`，handle `isla_rayne`
- `dim_video.published_dt` `2026-09-13`
- 近窗 spend `17.76`（该条当时广告 GMV 为 0）

`/ad/get/` 合法 fields 含：`tiktok_item_id`、`identity_id`、`identity_type`、`identity_authorized_bc_id`、`display_name`、`showcase_products`、`ad_id`、`adgroup_id`、`campaign_id`。非法：`shopping_ads_type`、`product_source`（这两在 adgroup）。

接口 `"-"` 表示不适用，落 NULL，不能补 0。

### 内部表

- 橱窗号账号：`decom.dws_social_creator_snapshot_daily`（`usage_type='tiktok-affiliate'`）
- 视频维：`decom.dim_video`（绑定前 `pre_binding` 已排除）
- 播放点赞：`decom.dws_social_video_state_daily_v2`
- RPA 视频成交：`videos.tiktok_shop_rpa_video_stats_snapshots`（`tiktok_video_id`、`gmv`、`items_sold`、`currency`、`stats_date`、`fetched_at`、`product_title`）。已有 DWS：`dws_social_affiliate_video_product_daily`（无商品名的行被丢掉；投流前件数若需要全量件数，确认是否应读源表而不是这张 DWS）
- 汇率：`decom.currency_conversion_dict`

### ECN 外部接口

文档：仓库根目录 `AI博主外部查询接口文档.md`（未跟踪）。

- `GET /api/v1/open-api/account-affiliation` → `affiliation_label`
- `GET /api/v1/open-api/videos/promotion-entry` → `ads_first_sent_at`
- Header：`X-API-Key: <ACCOUNT_CHANNEL_API_KEY>`
- 404/409 是业务结果，ODS 要能存「查过但没有」，不要当任务失败

### 明确不要做的

- 不要用 GMV Max `/gmv_max/report/get/` 填这个看板。
- 不要用广告 `video_id`、不要用 `identity_id` join `users.id`。
- 不要把 Makepod 店铺过滤拷到这个看板。
- 不要在 ADS 里直连 TikTok / ECN / `videos.*`。
- 不要把真实 token、API key 写进仓库或 fact.md。
- 不要未确认就回填历史分区。

## 9. Environment & Configuration Notes

- Python：本机用 `/Users/sinn/Documents/work_py_venv`。
- TikTok token：`prod-ads-runner-tiktok-access-token`（已验证可用）。
- ECN key：生产 `ACCOUNT_CHANNEL_API_KEY`，本仓库没有。
- Superset：`env/superset.json` + `.agents/skills/superset/SKILL.md`。凭据不要拼进 shell。
- 表变更：`.agents/skills/bigquery-safe-table-change/SKILL.md`。禁止对可能有 streaming buffer 的表 DELETE/UPDATE。
- 提交：`main` 直推；先 `python3 scripts/check_knowledge.py`。
- `etl_run.py` 没有 `--help`，传参会开跑全链路。入口看 `env/etl_config.json`。

## 10. Key Files & Locations

| 用途 | 路径 |
|---|---|
| 分层 | `docs/rules/layering.md` |
| 脚本/DDL | `docs/rules/code.md` |
| 主题写法 | `docs/rules/topics.md` |
| 橱窗号映射 | `docs/rules/video-mapping.md` 橱窗号一节 |
| 编排 | `docs/rules/pipeline.md`、`env/etl_config.json` |
| TikTok 广告 ODS 模板 | `jobs/ods/ods_tiktok_ad_daily/ods_tiktok_ad_daily.py` |
| 广告 ODS DDL | `ddl/ods/ods_tiktok_ad_daily.sql` |
| 橱窗号视频 ADS | `jobs/ads/ads_daily_social_affiliate_video.py` |
| RPA 视频 DWS | `jobs/dws/dws_social_affiliate_video_product_daily.py` |
| 参考 fact | `docs/topics/【Social Content】Makepod 店铺 TK 广告投放数据/fact.md` |
| 参考 fact | `docs/topics/【Social Content】橱窗号数据/fact.md` |
| 广告踩坑 | `docs/topics/【Social Content】Makepod 店铺 TK 广告投放数据/patterns.md` |
| ECN 接口 | `AI博主外部查询接口文档.md` |
| 知识校验 | `scripts/check_knowledge.py` |
| 索引 | `docs/topics/index.md` |

## 11. Quick-Start Summary

新建【Social Content】联盟项目数据看板。调研已完成，无代码。左表是 `tiktok-affiliate` 橱窗号视频，用 `tiktok_item_id = dim_video.video_id` 左连 TikTok AUCTION 广告，不要用 username，也不要按广告名「联盟」过滤。广告主仍是 `7667456906528735239`，token 仍是 `prod-ads-runner-tiktok-access-token`，调用抄 `ods_tiktok_ad_daily.py`。必须扩 ODS：补 `tiktok_item_id`、identity、以及 `shop_gross_revenue_by_order_submission`（Gross revenue (Shop)）、`shop_total_items_purchased`、`spend`。AUCTION 没有 GMV Max 的 `gross_revenue`。ROI 用 GMV/spend 现算。region 和投流首次时间走 ECN 两个 GET，单独建 ODS，key 从生产 `ACCOUNT_CHANNEL_API_KEY` 读。销售金额/件数：广告侧与 RPA 都入库，展示取 max，金额转 USD。投流前件数用 `ads_first_sent_at` 之前最新 RPA 快照。看板去掉已投流但 spend 与广告 GMV 都为 0 的行；未投流视频保留空广告字段。新主题，业务线 `TikTok 橱窗号`，表名不要 makepod。ADS 禁止读 ODS。回填先问用户。提交前 `check_knowledge.py`，走 `main`。
