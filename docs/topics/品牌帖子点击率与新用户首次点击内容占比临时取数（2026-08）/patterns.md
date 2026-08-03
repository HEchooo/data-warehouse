# 品牌帖子点击率与新用户首次点击内容占比临时取数（2026-08）踩坑记录

## 带 `args_post` 的 `c_` 事件集合远大于“点进帖子”

- 实际探查（近 30 天）发现，携带 `args_post`（`dwd_event_log.post_code` 非空）且以 `c_` 开头的事件不止 `c_home_feeds`，还包括 `c_like`、`c_follow`、`c_tryon_confirm`、`c_third_party_goods`、`c_search_result_feeds`、`c_go_product_detail`、`c_product` 等互动/转化类事件。
- 按用户给定口径（`c_` 开头 + `args_post` 命中品牌帖子即计入点击 UV），这些事件都会计入 `click_uv`；已按需排除 `c_email`、`c_push`。
- 如果后续要收敛成“仅统计导航型点击”，需要单独确认事件白名单，否则点击 UV 会包含点赞、关注、试穿确认等非导航行为。

## `community_post.post_type` 存在未在规则文档定义的取值

- `docs/rules/post-mapping.md` 只定义了 `2/4/5/6/7/9`，实际数据中还出现 `post_type = 1` 和 `10`。
- 需求 2 的内容类型口径把未定义取值统一归入「其他」，避免臆造映射；如果后续要精确拆分，需要先找业务确认 `1`、`10`、`8`（视频）、`9`（kol）在这个场景下算什么类型。

## 品牌判定用 `post_type` 而不是事件名

- 品牌帖子可能通过通用场景事件（`v_home_feeds`、`v_search_result_feeds` 等）产生曝光/点击，不会都落在 `v_brand_post_detail` / `v_brand_post_feeds` 这类专属事件名下。
- 因此判断“这个帖子是不是品牌”改为直接查服务端 `community_post.post_type IN (6, 7)`，再用 `args_post` 关联事件，而不是靠事件名前缀猜内容类型。

## `c_product.args.asset_code` 起报时间导致品牌商品点击率覆盖率很低

- `docs/rules/asset-mapping.md` 已注明 `c_product.args.asset_code` 从 `2026-07-25` 起才上报；`2026-03-01` 至今的取数窗口里，实测只有 5 个日期、8 个品牌命中商品点击数据，且全部落在 `2026-07-26` 之后（`2026-03-01 ~ 2026-07-24` 期间该事件不带 `asset_code`，无法归因到素材/品牌）。
- 这不是查询逻辑问题，是埋点上报时间的硬限制；如果业务方觉得覆盖品牌数偏少，需要先确认是否要把统计窗口收窄到 `2026-07-25` 之后，而不是继续排查 SQL。
- 商品点击率的计算直接复用了已下沉的 `decom.dws_asset_user_daily`（`product_click_pv`）和 `decom.dim_asset`，没有重新解析 `dwd_event_log.args.asset_code`，避免和已有 DWS 口径产生第二份实现。
