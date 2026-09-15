# 品牌帖子点击率与新用户首次点击内容占比临时取数（2026-08）

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | 品牌帖子点击率与新用户首次点击内容占比临时取数 | 品牌帖子 CTR、新用户首次点击内容 |

## 适用范围

- 业务线：`Alvin's Club App`
- 适合：复用 2026-08 临时查询，滚动查看最近 30 天品牌帖子点击率、新用户首活首次点击内容类型和品牌商品点击率。
- 不适合：稳定帖子日报应使用[内容维度日报](../内容维度日报/fact.md)；本主题不代表持续维护的 ADS 报表。
- 需要澄清：需要确认三个查询中的哪一个、运行截止日，以及 UV 或率值口径。

## 状态

- 状态：`completed`（一次性查询，可按原滚动窗口重新运行）

## 品牌帖子曝光点击率

### 报表

- 类型：临时取数，不新增 ADS 表，不修改生产数据
- 状态：`completed`
- 时间：最近 30 天（截至昨天），按 `America/Toronto` 切天，运行时自动滚动
- 粒度：`dt × post_code × 品牌`
- 产物：SQL：`query_brand_post_ctr.sql`

### 字段

| 字段 | 口径 |
|------|------|
| dt | 事件发生日期，按 `America/Toronto` 切天 |
| post_code | 帖子 ID，来自 `v3_decom.community_post.post_code` |
| brand_name | 品牌名称，`community_post.poster_user_id -> v3_decom.kol_rel.nickname` |
| exposure_uv | 帖子曝光 UV：`event_name` 以 `v_` 开头、`args_post`（`dwd_event_log.post_code`）关联到该品牌帖子的已登录用户数，`COUNT(DISTINCT prop_user_id)` |
| click_uv | 帖子点击 UV：`event_name` 以 `c_` 开头（排除 `c_email`、`c_push`）、`args_post` 关联到该品牌帖子的已登录用户数，`COUNT(DISTINCT prop_user_id)` |
| ctr | `click_uv / exposure_uv` |
| like_uv | 点赞 UV：服务端表 `v3_decom.community_post_like`，按 `gmt_create` 切天后 `COUNT(DISTINCT user_id)`，映射回品牌帖子 |

补充说明：

- 品牌帖子判定：`community_post.post_type IN (6, 7)`（`FASHION_LOOK` / `FASHION_PRODUCT`，见 `docs/rules/post-mapping.md`），不依赖 `v_brand_post_detail` 等事件名称，因为品牌帖子也可能通过 `v_home_feeds`、`v_search_result_feeds` 等通用场景曝光/点击。
- “点击”严格按用户给定规则执行：`event_name` 只要以 `c_` 开头且该事件携带的 `args_post` 关联到品牌帖子，即计入点击 UV；因此 `c_like`、`c_follow`、`c_tryon_confirm` 等互动类事件只要带该帖子也会计入点击。已按需排除 `c_email`、`c_push` 这类被动通知事件。
- 输出只保留有曝光、点击或点赞任一活动的 `dt × post_code` 行；完全无活动的品牌帖子不出现在结果中。

### 依赖

- 上游：`decom.dwd_event_log`、`v3_decom.community_post`、`v3_decom.kol_rel`、`v3_decom.community_post_like`
- 规则：`docs/rules/post-mapping.md`、`docs/rules/data.md`、`docs/rules/code.md`
- 查询文件：`query_brand_post_ctr.sql`

## 新用户首活当天首次点击内容类型占比

### 报表

- 类型：临时取数，不新增 ADS 表，不修改生产数据
- 状态：`completed`
- 时间：最近 30 天（截至昨天），按 `America/Toronto` 切天，运行时自动滚动
- 粒度：`dt（新用户首活日） × content_type`
- 产物：SQL：`query_new_user_first_click_content.sql`

### 字段

| 字段 | 口径 |
|------|------|
| dt | 新用户首活日期，来自 `decom.dim_user_first_active.first_active_date` |
| content_type | 首次点击帖子的内容类型：`post_type` 2/4=明星、5=杂志、6/7=品牌，其余（含帖子已删除等无法关联的情况）归为「其他」 |
| content_type_users | 当天首次点击落在该内容类型的新用户数 |
| content_type_share | `content_type_users / 当天首活当天有 c_home_feeds 点击的新用户数` |

补充说明：

- “新用户”＝`dim_user_first_active.first_active_date` 落在取数窗口内的用户。
- “第一次点击”＝该用户首活当天（`America/Toronto` 切天）产生的第一条 `c_home_feeds` 事件，按事件时间最早排序；不看首活日之后才发生的点击。
- 占比分母是“首活当天有点击”的新用户数，不含当天完全没有点击 `c_home_feeds` 的新用户。

### 依赖

- 上游：`decom.dwd_event_log`、`decom.dim_user_first_active`、`v3_decom.community_post`
- 规则：`docs/rules/post-mapping.md`、`docs/rules/data.md`、`docs/rules/code.md`
- 查询文件：`query_new_user_first_click_content.sql`

## 品牌商品点击率

### 报表

- 类型：临时取数，不新增 ADS 表，不修改生产数据
- 时间：`2026-03-01` 至今（按 `America/Toronto` 切天，含当天不完整数据）
- 粒度：`dt × 品牌`（`dim_column.module = 'brand'`），按天输出，不对整段时间汇总
- 产物：SQL：`query_brand_product_click_rate.sql`

### 字段

| 字段 | 口径 |
|------|------|
| dt | 事件发生日期，按 `America/Toronto` 切天 |
| brand_name | 品牌名称，来自 `decom.dim_column.column_name`（`module = 'brand'`） |
| product_click_rate | 商品点击率 = 当天该品牌商品点击 UV / 当天该品牌帖子曝光 UV |

补充说明：

- 商品点击 UV：事件 `c_product`，`args.asset_code` 关联 `decom.dim_asset`（`module = 'brand'`）得到品牌，直接复用已下沉的 `decom.dws_asset_user_daily`（`product_click_pv > 0` 的用户按 `dt × 品牌` 去重），不重新解析 `dwd_event_log` 的 `args.asset_code`。
- 帖子曝光 UV：口径与 `query_brand_post_ctr.sql` 一致——`event_name` 以 `v_` 开头、`args_post` 关联到该品牌帖子的已登录用户数，按 `dt × 品牌` 去重。
- 只保留当天商品点击 UV > 0 的 `dt × 品牌` 行；当天商品点击 UV = 0 的组合不出现在结果中。
- `c_product.args.asset_code` 从 `2026-07-25` 起才上报，`2026-03-01 ~ 2026-07-24` 期间该字段缺失，因此实测结果全部集中在 `2026-07-26` 之后，属预期。

### 依赖

- 上游：`decom.dwd_event_log`、`v3_decom.community_post`、`decom.dim_column`、`decom.dim_asset`、`decom.dws_asset_user_daily`
- 规则：`docs/rules/asset-mapping.md`、`docs/rules/post-mapping.md`、`docs/rules/data.md`、`docs/rules/code.md`
- 查询文件：`query_brand_product_click_rate.sql`
