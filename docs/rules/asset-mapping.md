# asset 基础映射与互动口径

## 适用范围

- 本文档是素材实体、帖子素材关联、素材曝光与素材点赞口径的 SSOT。
- `asset` 指社区素材，业务主键为 `asset_code`。
- 本文档不适用于视频数据；视频相关口径见 `docs/rules/video-mapping.md`。

## 素材主数据

- 表：`v3_decom.community_fashion_asset`
- 素材 ID：`asset_code`
- 素材生产时间：`gmt_create`
- `gmt_create` 为服务端 UTC `DATETIME`；转换为东八区时间时使用：

```sql
DATETIME(TIMESTAMP(gmt_create, 'UTC'), 'Asia/Shanghai')
```

- 同一 `asset_code` 存在多条记录时，优先按 `COALESCE(gmt_update, gmt_create)` 倒序、`id` 倒序保留最近一条。

## 帖子与素材映射

- 帖子表：`v3_decom.community_post`
- 帖子素材来源：`community_post.post_ext.mainAssets[*].assetCode`
- `post_ext.mainAssets[*].assetCode` 与 `community_fashion_asset.asset_code` 关联。
- `mainAssets` 是数组；一个帖子可以包含多个素材，同一素材也可能被多个帖子复用。
- 素材生产时间取 `community_fashion_asset.gmt_create`，不能使用帖子 `publish_time` 代替。

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

## 分层建议

- 一次性临时取数可以直接读取素材主表、点赞表和 `dwd_event_log`。
- 如果素材维度或互动指标转为固定报表，应将素材稳定属性下沉到 `DIM`、素材曝光与点赞事实下沉到 `DWS`，再由 `ADS` 聚合，避免报表层重复全历史扫描事件表。
