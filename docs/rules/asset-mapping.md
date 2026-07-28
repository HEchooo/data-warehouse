# asset 基础映射与互动口径

## 适用范围

- 本文档是素材实体、帖子素材关联、素材曝光与素材点赞口径的 SSOT。
- `asset` 指社区素材，业务主键为 `asset_code`。
- 本文档不适用于视频数据；视频相关口径见 `docs/rules/video-mapping.md`。

## 素材主数据

- 表：`v3_decom.community_fashion_asset`
- 素材 ID：`asset_code`
- 素材生产时间：`gmt_create`

- 同一 `asset_code` 存在多条记录时，优先按 `COALESCE(gmt_update, gmt_create)` 倒序、`id` 倒序保留最近一条。

## 帖子与素材映射

- 帖子表：`v3_decom.community_post`
- 帖子素材来源：`community_post.post_ext.mainAssets[*].assetCode`
- `post_ext.mainAssets[*].assetCode` 与 `community_fashion_asset.asset_code` 关联。
- `mainAssets` 是数组；一个帖子可以包含多个素材，同一素材也可能被多个帖子复用。
- 素材生产时间取 `community_fashion_asset.gmt_create`，不能使用帖子 `publish_time` 代替。
- 素材发布时间取 `community_post.publish_time`，因为素材的发布时间就是帖子的发布时间。

## 素材曝光

- 事件表：`decom.dwd_event_log`
- 曝光事件：`v_home_feeds`、`v_closet_feeds`
- 素材 ID 来源：JSON 字段 `args.asset_code`。
- `args.asset_code` 是数组，统计前需要使用 `JSON_QUERY_ARRAY` 和 `UNNEST` 拆到素材粒度。
- 有效用户条件：`prop_user_id IS NOT NULL AND prop_user_id != ''`。
- 曝光 PV：按 `asset_code` 分组，`COUNT(DISTINCT raw_event_id)`。
- 曝光 UV：按 `asset_code` 分组，`COUNT(DISTINCT prop_user_id)`。
- DWD 拆行后，同一原始事件可能形成多条明细；曝光 PV 必须使用 `raw_event_id`，不能使用 DWD 行数或 `hash_id` 代替。

## 素材点赞

- 表：`v3_decom.community_asset_like`
- 素材关联键：`asset_code`
- 点赞用户：`user_id`
- 点赞 UV：按 `asset_code` 分组，排除空用户后 `COUNT(DISTINCT user_id)`。

## 帖子详情页素材曝光

- 事件：`v_brand_post_detail`、`v_magazine_post_detail`、`v_star_post_detail`
- 素材 ID 来源：JSON 字段 `args.asset_code`
- `args.asset_code` 目前是**标量字符串**，不是数组（与「素材曝光」小节的 `v_home_feeds` / `v_closet_feeds` 不同）；解析时应同时兼容标量和数组两种形态（`JSON_TYPE(args.asset_code) = 'array'` 时按数组展开，否则按单元素处理），避免未来埋点改结构时静默丢数
- 该口径 `args.asset_code` **从 2026-07-16 起才开始上报**，之前的事件不带该字段
- 有效用户条件：`prop_user_id IS NOT NULL AND prop_user_id != ''`
- 曝光 PV：按 `asset_code` 分组，`COUNT(DISTINCT raw_event_id)`（同一 `raw_event_id` 可能因 `args.spu` 拆多行，必须按 `raw_event_id` 去重，不能用行数）
- 曝光 UV：按 `asset_code` 分组，`COUNT(DISTINCT prop_user_id)`
- `args.brand` / `args.magazine` / `args.star` 与素材所属帖子的 `community_post.poster_user_id` 一致，可直接由 `dim_asset.column_id` 复用，不必反查事件

## 素材试穿

- 表：`v3_decom.common_ext_ai_task`
- 素材 ID 来源：`json_extract(request_param, '$.assetCode')`（标量字符串）
- 同时要求 `task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')`（见 `docs/rules/tryon-mapping.md`）
- 试穿次数：按 `asset_code` 分组，`COUNT(DISTINCT task_id)`
- 试穿 UV：按 `asset_code` 分组，`COUNT(DISTINCT user_id)`

## 素材负反馈

- 事件：`c_feedback_entry`
- 素材 ID 来源：JSON 字段 `args.asset_code`
- 有效用户条件：`prop_user_id IS NOT NULL AND prop_user_id != ''`
- 负反馈 PV：按 `asset_code` 分组，`COUNT(DISTINCT raw_event_id)`
- 负反馈 UV：按 `asset_code` 分组，`COUNT(DISTINCT prop_user_id)`
- 负反馈率：负反馈 UV / 曝光 UV
- 截至 2026-07，`c_feedback_entry` 全历史事件都只带 `args.post` / `args.spu`，尚未见到 `args.asset_code` 上报；口径按 `args.asset_code` 正常实现，当前结果为 0 属预期，待埋点补齐后自动产出数据

## 素材下方商品点击

- 事件：`c_product`
- 素材 ID 来源：JSON 字段 `args.asset_code`
- `args.asset_code` 是**标量字符串**，与「帖子详情页素材曝光」小节一致；解析时同样需兼容标量/数组两种形态
- 该字段 **从 2026-07-25 起才开始上报**，之前的事件不带该字段
- `args.page_key` 目前只落在 `star_detail` / `magazine_detail` / `brand_detail` 三个帖子详情页，与素材曝光的页面范围一致，故不额外按 `page_key` 过滤
- 有效用户条件：`prop_user_id IS NOT NULL AND prop_user_id != ''`
- 商品点击 PV：按 `asset_code` 分组，`COUNT(DISTINCT raw_event_id)`（同一 `raw_event_id` 会因 `args.spu` 拆多行，必须按 `raw_event_id` 去重）
- 商品点击 UV：按 `asset_code` 分组，`COUNT(DISTINCT prop_user_id)`
- 当前只落地 UV，不产出对外可见的 PV 报表字段

## 素材模块 / 专栏归属

- 素材模块由其首发帖（`publish_time` 最早的关联帖）的 `post_type` 派生：`2,4 -> star`、`5 -> magazine`、`6,7 -> brand`、`8 -> video`、`9 -> kol`，其余归为 `other`
- 素材专栏 ID = 首发帖 `community_post.poster_user_id`；专栏名称取 `dim_column`（即 `v3_decom.kol_rel.nickname`）
- 实测 89,150 / 89,154 个素材单模块；极少数（4 个）跨模块，按首发帖归属即可满足绝大多数场景

## 分层建议

- 一次性临时取数可以直接读取素材主表、点赞表和 `dwd_event_log`。
- 素材维度（稳定属性 + 模块/专栏归属）已下沉到 `DIM`：`decom.dim_asset`
- 素材曝光 / 试穿 / 负反馈 / 商品点击用户级事实已下沉到 `DWS`：`decom.dws_asset_user_daily`（粒度 `dt × asset_code × user_id`）
- 后续素材相关报表应优先复用 `dim_asset` / `dws_asset_user_daily`，不要重新读取 `community_fashion_asset` / `community_post` / `dwd_event_log` 现算，避免报表层重复全历史扫描事件表
