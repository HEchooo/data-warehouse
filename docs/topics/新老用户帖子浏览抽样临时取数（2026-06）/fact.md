# 新老用户帖子浏览抽样临时取数（2026-06）

## 帖子详情浏览抽样

### 报表

- 类型：临时取数，不新增 ADS 表，不修改生产数据
- 时间：`2026-06-01 ~ 2026-06-07`，首尾包含，按 `America/Toronto` 切天
- 抽样：每天分别稳定抽取 30 个新用户和 30 个老用户
- 粒度：`浏览日期 × 用户分群 × 浏览用户 ID × 帖子 ID`
- 产物：
  - SQL：`query.sql`
  - CSV：`outputs/新老用户帖子浏览抽样临时取数（2026-06）/result.csv`

### 字段

| 字段 | 口径 |
|---|---|
| `browse_dt` | 美东浏览日期 |
| `user_group` | 新用户：`first_active_date = browse_dt`；老用户：`first_active_date < browse_dt` |
| `browse_user_id` | 浏览用户 ID，即事件 `prop_user_id` |
| `post_code` | 帖子 ID |
| `poster_user_id` | 发帖人 ID，来自 `community_post.poster_user_id` |
| `asset_codes` | 按 `mainAssets` 原始顺序输出 `post_ext.mainAssets[*].assetCode` 数组 |
| `content_type` | `post_type` 映射：`2/4=明星`、`5=杂志`、`6/7=品牌` |
| `creator` | 发布运营，来自 `community_post.creator` |
| `content_create_time_cn` | 内容创建时间；将服务端 UTC `DATETIME` 转为东八区时间 |
| `content_publish_time_cn` | 内容发布时间；将服务端 UTC `DATETIME` 转为东八区时间 |
| `first_browse_time_et` | 用户当天首次浏览该帖的美东当地时间 |
| `browse_count` | 用户当天浏览该帖次数，按 `raw_event_id` 去重，缺失时回退 `hash_id` |
| `post_image_urls` | 按 `mainAssets` 原始顺序输出 `post_ext.mainAssets[*].image.url` 数组；与 `asset_codes` 按数组下标一一对应 |

### 依赖

- 事件：`decom.dwd_event_log`
- 首活维表：`decom.dim_user_first_active`
- 帖子主数据：`v3_decom.community_post`
- 浏览事件：`v_star_post_detail`、`v_magazine_post_detail`、`v_brand_post_detail`
- 规则：`docs/rules/data.md`、`docs/rules/code.md`、`docs/rules/post-mapping.md`

补充说明：

- 抽样使用固定盐值的 `FARM_FINGERPRINT` 排序，因此同一份源数据重复执行结果一致。
- 先抽用户，再输出样本用户当天浏览的全部符合条件帖子；不是每天每组只取 30 条浏览记录。
- `asset_codes[n]` 与 `post_image_urls[n]` 来自同一个 `mainAssets[n]`；没有 `assetCode` 的元素不进入两个数组。
- 临时查询直接读取 DWD 与服务端源表；若转为固定报表，应将帖子素材和封面等稳定属性下沉到 `DIM`，再由 DWS/ADS 消费。

## 首页 Feeds 帖子曝光抽样

### 报表

- 类型：临时取数，不新增 ADS 表，不修改生产数据
- 时间：`2026-06-01 ~ 2026-06-07`，首尾包含，按 `America/Toronto` 切天
- 抽样：每天分别稳定抽取 10 个新用户和 10 个老用户
- 抽样用户池：当天存在 `v_home_feeds` 帖子曝光，且 `args_post` 对应帖子能映射为明星、杂志或品牌的用户
- 粒度：`曝光日期 × 用户分群 × 浏览用户 ID × 帖子 ID`
- 产物：
  - SQL：`query_home_feeds.sql`
  - CSV：`outputs/新老用户帖子浏览抽样临时取数（2026-06）/result_home_feeds.csv`

### 字段

| 字段 | 口径 |
|---|---|
| `browse_dt` | 美东曝光日期 |
| `user_group` | 新用户：`first_active_date = browse_dt`；老用户：`first_active_date < browse_dt` |
| `browse_user_id` | 曝光用户 ID，即事件 `prop_user_id` |
| `post_code` | `v_home_feeds.args_post` 拆出的帖子 ID，对应 DWD 字段 `post_code` |
| `poster_user_id` | 发帖人 ID，来自 `community_post.poster_user_id` |
| `asset_codes` | 按 `mainAssets` 原始顺序输出 `post_ext.mainAssets[*].assetCode` 数组 |
| `content_type` | `post_type` 映射：`2/4=明星`、`5=杂志`、`6/7=品牌` |
| `creator` | 发布运营，来自 `community_post.creator` |
| `content_create_time_cn` | 内容创建时间；将服务端 UTC `DATETIME` 转为东八区时间 |
| `content_publish_time_cn` | 内容发布时间；将服务端 UTC `DATETIME` 转为东八区时间 |
| `first_browse_time_et` | 用户当天首次收到该帖 Feeds 曝光的美东当地时间；字段名为兼容原结果而保留 |
| `browse_count` | 用户当天该帖 Feeds 曝光次数，按 `raw_event_id` 去重，缺失时回退 `hash_id`；字段名为兼容原结果而保留 |
| `post_image_urls` | 按 `mainAssets` 原始顺序输出 `post_ext.mainAssets[*].image.url` 数组；与 `asset_codes` 按数组下标一一对应 |

### 依赖

- 事件：`decom.dwd_event_log`
- 首活维表：`decom.dim_user_first_active`
- 帖子主数据：`v3_decom.community_post`
- 曝光事件：`v_home_feeds`
- 帖子来源：`v_home_feeds.args_post` 已由 DWD 拆行为 `post_code`
- 规则：`docs/rules/data.md`、`docs/rules/code.md`、`docs/rules/post-mapping.md`

补充说明：

- 抽样使用固定盐值的 `FARM_FINGERPRINT` 排序，因此同一份源数据重复执行结果一致。
- 先将曝光帖子过滤到明星、杂志或品牌，再抽用户；随后输出样本用户当天全部符合条件的 Feeds 曝光帖子。
- 同一个 `v_home_feeds` 原始事件可上报多个帖子；帖子粒度按 `raw_event_id` 去重，避免 DWD 拆行造成重复计数。
- CSV 中 `asset_codes` 和 `post_image_urls` 使用 JSON 数组字符串保存，数组顺序和下标对应关系不变。
- 临时查询直接读取 DWD 与服务端源表；若转为固定报表，应将帖子素材和封面等稳定属性下沉到 `DIM`，再由 DWS/ADS 消费。
