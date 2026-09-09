# 素材生产与互动全量临时取数（2026-07）

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 字段 | 素材 ID | asset_code、素材编码 |
| 字段 | 素材生产时间 | production_time_cn、素材创建时间 |
| 指标 | exposure_uv | 曝光 UV、曝光人数 |
| 指标 | 曝光 PV | 曝光次数、展示量 |
| 指标 | like_uv | 点赞 UV、点赞人数 |

## 报表

- 类型：临时取数，不新增 ADS 表，不修改生产数据
- 统计范围：全历史累计
- 粒度：每个 `asset_code` 一行
- 产物：`query.sql`

## 字段

| 字段 | 口径 |
|---|---|
| `asset_code` | 素材 ID，来自 `v3_decom.community_fashion_asset.asset_code` |
| `production_time_cn` | 素材生产时间；将 `community_fashion_asset.gmt_create` 按服务端 UTC `DATETIME` 转为东八区时间 |
| `exposure_pv` | `v_home_feeds`、`v_closet_feeds` 中有效登录用户产生的素材曝光；拆解 `args.asset_code` 后按 `asset_code` 分组，`COUNT(DISTINCT raw_event_id)` |
| `exposure_uv` | 与曝光 PV 使用相同事件和有效登录用户范围，按 `asset_code` 分组，`COUNT(DISTINCT prop_user_id)` |
| `like_uv` | `v3_decom.community_asset_like` 按 `asset_code` 分组，`COUNT(DISTINCT user_id)`；空用户不参与统计 |

## 依赖

- 素材主数据：`v3_decom.community_fashion_asset`
- 素材点赞：`v3_decom.community_asset_like`
- 曝光事件：`decom.dwd_event_log`
- SQL：`query.sql`
- 规则：`docs/rules/asset-mapping.md`、`docs/rules/code.md`、`docs/rules/data.md`

补充说明：

- 结果以素材主表为基表，因此没有曝光或点赞的素材仍会输出，对应指标填 `0`。
- `dwd_event_log.args.asset_code` 是 JSON 数组；拆到素材粒度后，PV 仍按 `raw_event_id` 去重，避免同一原始事件重复计数。
- 这是一次性全历史查询。若转为固定报表，应将素材维度下沉到 `DIM`、素材互动事实下沉到 `DWS`，避免报表层重复全量扫描事件表。
