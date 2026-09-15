# 【AI-Fashion】素材内容数据

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 主题 | AI-Fashion 素材内容数据 | 素材内容数据看板 |
| 指标 | 负反馈率 | 负向反馈率 |

## 适用范围

- 业务线：`Alvin's Club App`
- 适合：查询素材周度上线表现、日榜单和素材明细中的曝光、试穿、商品点击及负反馈。
- 不适合：帖子维度表现应使用[内容维度日报](../内容维度日报/fact.md)；平台整体内容趋势应使用[平台整体内容表现](../平台整体内容表现/fact.md)。
- 需要澄清：需要确认周总览、日榜或素材明细，素材模块、排名类型和日期范围。

## 报表

### ads_weekly_ai_fashion_asset_overview

- 表：`my-project-8584-jetonai.decom.ads_weekly_ai_fashion_asset_overview`
- 状态：`active`
- 粒度：一行代表一个 `week_start_dt × module`
- 时区：`America/Toronto`
- 数据范围：本周新上线素材按内容模块的周表现。
- 必要过滤：模块仅 `brand / magazine / star`。
- 不包含：非本周上线素材和日榜单。

### ads_daily_ai_fashion_asset_rankings

- 表：`my-project-8584-jetonai.decom.ads_daily_ai_fashion_asset_rankings`
- 状态：`active`
- 粒度：一行代表一个 `dt × ranking_type × rank_no`
- 时区：`America/Toronto`
- 数据范围：素材试穿、商品点击和负反馈的日榜单。
- 必要过滤：按目标 `ranking_type` 过滤。
- 不包含：榜单候选外素材和周总览。

### ads_daily_ai_fashion_asset_detail

- 表：`my-project-8584-jetonai.decom.ads_daily_ai_fashion_asset_detail`
- 状态：`active`
- 粒度：一行代表一个 `dt × asset_code`
- 时区：`America/Toronto`
- 数据范围：当日存在曝光、试穿或负反馈行为的素材明细。
- 必要过滤：按目标 `asset_code` 过滤。
- 不包含：当日所有行为均为 0 的素材。

### 主题总览

- 主题：素材内容数据看板，围绕「素材（asset）」维度统计新上线素材分发效果、日榜单和素材明细
- 三张 ADS 表：
  - `ads_weekly_ai_fashion_asset_overview`：粒度 `week_start_dt × module`，页面顶部「每周新上线素材数据总览」
  - `ads_daily_ai_fashion_asset_rankings`：粒度 `dt × ranking_type × rank_no`，页面中部 Top 榜单、下部 Bottom 榜单
  - `ads_daily_ai_fashion_asset_detail`：粒度 `dt × asset_code`，页面底部「素材明细」
- Dashboard：[128](https://bi.alvinclub.ca/superset/dashboard/ai-fashion-asset-content-data/)

### 页面顶部：每周新上线素材数据总览（`ads_weekly_ai_fashion_asset_overview`）

- 硬性条件：统计对象固定为「本周（美东时区）新上线的素材」，即 `dim_asset.publish_dt` 落在目标周内的素材
- 素材发布时间取 `community_post.publish_time`（素材的发布时间就是帖子的发布时间）
- 模块只保留 `brand` / `magazine` / `star` 三个内容模块（`kol` / `video` / `other` 不进本报表）
- 自然周：周一起、周日止（`DATE_TRUNC(dt, WEEK(MONDAY))`），美东时区
- 有效曝光阈值：周曝光 UV > 500（脚本常量 `VALID_EXPOSURE_UV_THRESHOLD`，当前实际周曝光 UV 量级远低于该阈值，`valid_exposure_asset_count` 恒为 0 属预期）
- 帖子状态：不过滤 `community_post.status`（含隐藏/审核中的帖子所含素材）
- 报表只统计到当前周为止；服务端已存在排期到未来的素材（`publish_time` 在未来），不提前物化尚未到来的周

### 页面中部：Top 榜单 / 页面下部：Bottom 榜单（`ads_daily_ai_fashion_asset_rankings`）

- 不限「新发布」，不限模块，全站一份榜；`module` / `column_name` 仅作展示字段
- 粒度：每日（美东时区）
- `ranking_type` 四种：
  - `tryon_uv_top5`：当日试穿 UV > 0 的素材，按试穿 UV 降序取前 5（并列时按曝光 UV 降序、`asset_code` 兜底排序）
  - `product_click_uv_top5`：当日素材下方商品点击 UV > 0 的素材，按商品点击 UV 降序取前 5（并列排序规则同上）
  - `tryon_uv_bottom50`：候选池为当日曝光 UV > 0 的素材，按试穿 UV 升序取前 50
  - `negative_feedback_rate_top50`：候选池为当日曝光 UV > 0 的素材，按负反馈率降序取前 50
- 榜单条数不足时按实际条数输出，不补空行

### 页面底部：素材明细（`ads_daily_ai_fashion_asset_detail`）

- 不限「新发布」；粒度：每日（美东时区）× 素材
- 只保留当日曝光 / 试穿 / 负反馈任一行为不为 0 的素材（全量素材约 9 万个，仅输出当日有行为的子集）

## 字段

### 每周新上线素材数据总览

| ADS 字段 | 含义 | 计算 |
|---|---|---|
| `week_start_dt` | 自然周周一（内部字段，用于分区与周报日期筛选，不在 Superset 展示） | `DATE_TRUNC(dt, WEEK(MONDAY))` |
| `week_range` | 自然周起止展示字段，格式 `YYYY-MM-DD ~ YYYY-MM-DD` | `week_start_dt ~ week_start_dt+6天` |
| `module` | 内容模块 | `brand` / `magazine` / `star` |
| `new_asset_count` | 本周新上线素材总数 | `dim_asset.publish_dt` 落在本周的素材数 |
| `exposed_asset_count` | 获得曝光素材数 | 新上线素材中，本周曝光 PV > 0 的素材数 |
| `valid_exposure_asset_count` | 有效曝光素材数 | 获得曝光素材中，周曝光 UV > 500 |
| `insufficient_sample_asset_count` | 样本不足素材数 | 获得曝光素材中，`0 < 周曝光 UV <= 500` |
| `zero_exposure_asset_count` | 0 曝光素材数 | `new_asset_count - exposed_asset_count`（服务端有、日志无曝光） |
| `exposure_uv` | 曝光 UV | 获得曝光素材中，`prop_user_id` 按周去重 |
| `product_click_uv` | 素材下方商品点击 UV | 获得曝光素材范围内，商品点击次数 > 0 的用户按周去重（`c_product.args.asset_code`，2026-07-25 起上报） |
| `tryon_uv` | 试穿 UV | 获得曝光素材范围内，试穿次数 > 0 的用户按周去重 |
| `negative_feedback_uv` | 负反馈 UV | 获得曝光素材范围内，负反馈 PV > 0 的用户按周去重 |
| `negative_feedback_rate` | 负反馈率 | `negative_feedback_uv / exposure_uv` |

### Top / Bottom 榜单

`ads_daily_ai_fashion_asset_rankings` 物理表保留完整宽表字段（与素材明细同源同口径，便于核对），供三个榜单 chart 共用；Superset chart 只展示与该榜单直接相关的精简字段，不逐一铺开全部宽表列：

| ADS 字段 | 含义 |
|---|---|
| `dt` | 日报日期 |
| `ranking_type` | `tryon_uv_top5` / `tryon_uv_bottom50` / `negative_feedback_rate_top50` |
| `rank_no` | 榜单排名（每个 `dt × ranking_type` 内连续不重复） |
| `asset_code` / `asset_name` | 素材 ID / 名称 |
| `module` / `column_id` / `column_name` | 内容模块 / 专栏 ID / 专栏名称 |
| `creator` | 运营人员 |
| `publish_time` / `online_days` | 素材上线时间 / 上线天数（`dt - publish_dt`） |
| `exposure_pv` / `exposure_uv` | 当日曝光 PV / UV |
| `product_click_uv` | 当日素材下方商品点击 UV（只落 UV，不落 PV） |
| `tryon_count` / `tryon_uv` | 当日试穿次数 / UV |
| `negative_feedback_pv` / `negative_feedback_uv` | 当日负反馈 PV / UV |
| `negative_feedback_rate` | 负反馈率 |

Chart 实际展示列（精简版，见下方 Superset 小节）：
- `每日试穿UV Top5` / `每日试穿UV Bottom50`：日报日期、排名、素材ID、素材名称、内容模块、专栏名称、曝光UV、试穿UV
- `每日素材下方商品点击UV Top5`：日报日期、排名、素材ID、素材名称、内容模块、专栏名称、曝光UV、素材下方商品点击UV
- `每日负反馈率Top50`：日报日期、排名、素材ID、素材名称、内容模块、专栏名称、曝光UV、负反馈UV、负反馈率

### 素材明细

| 需求字段 | ADS 字段 | 口径 |
|---|---|---|
| 日报日期 | `dt` | 美东时区切天 |
| 内容模块 | `module` | 素材首发帖 `post_type` 派生 |
| 专栏 ID | `column_id` | 素材首发帖 `poster_user_id`（= 曝光事件 `args.brand/magazine/star`） |
| 专栏名称 | `column_name` | `v3_decom.kol_rel.nickname` |
| 素材 ID | `asset_code` | - |
| 运营人员 | `creator` | `community_fashion_asset.creator` |
| 素材上线时间 | `publish_time` | `community_post.publish_time`（首发帖） |
| 素材上线天数 | `online_days` | `DATE_DIFF(dt, publish_dt, DAY)` |
| 曝光 PV | `exposure_pv` | `COUNT(DISTINCT raw_event_id)` |
| 曝光 UV | `exposure_uv` | `COUNT(DISTINCT prop_user_id)` |
| 素材下方商品点击 UV | `product_click_uv` | `c_product` 的 `COUNT(DISTINCT prop_user_id)`（`args.asset_code`，2026-07-25 起上报；只落 UV，不落 PV） |
| 试穿次数 | `tryon_count` | `COUNT(DISTINCT task_id)`，`task_type IN ('QUICK_TRY_ON','TRY_ON','OUTFIT')` |
| 试穿 UV | `tryon_uv` | `COUNT(DISTINCT user_id)`，同上条件 |
| 负反馈 PV | `negative_feedback_pv` | `c_feedback_entry` 的 `COUNT(DISTINCT raw_event_id)` |
| 负反馈 UV | `negative_feedback_uv` | `c_feedback_entry` 的 `COUNT(DISTINCT prop_user_id)` |
| 负反馈率 | `negative_feedback_rate` | `negative_feedback_uv / exposure_uv` |

补充说明：

- 周总览 UV 按 `prop_user_id` 在自然周内去重；日榜与明细 UV 按业务日、素材去重，不能由日 UV 相加得到周 UV
- `negative_feedback_rate` 的分母为 0 时返回 `NULL`；跨素材或跨周期必须分别汇总负反馈 UV 与曝光 UV 后重算

## 已落地字段

需求字段已全部落地，无缺口。

## 依赖

### 链路

```
v3_decom.community_fashion_asset + v3_decom.community_post + decom.dim_column
  -> decom.dim_asset

decom.dwd_event_log（曝光/负反馈/商品点击） + v3_decom.common_ext_ai_task（试穿）
  -> decom.dws_asset_user_daily

dim_asset + dws_asset_user_daily
  -> ads_weekly_ai_fashion_asset_overview
  -> ads_daily_ai_fashion_asset_rankings
  -> ads_daily_ai_fashion_asset_detail
```

### 表与脚本

- `decom.dim_asset`：素材维度层，粒度 `asset_code`，脚本 `jobs/dim/dim_asset.py`，DDL `ddl/dim/dim_asset.sql`
- `decom.dws_asset_user_daily`：素材用户日事实层，粒度 `dt × asset_code × user_id`，脚本 `jobs/dws/dws_asset_user_daily.py`，DDL `ddl/dws/dws_asset_user_daily.sql`
- `decom.ads_weekly_ai_fashion_asset_overview`：脚本 `jobs/ads/ads_weekly_ai_fashion_asset_overview.py`，DDL `ddl/ads/ads_weekly_ai_fashion_asset_overview.sql`
- `decom.ads_daily_ai_fashion_asset_rankings`：脚本 `jobs/ads/ads_daily_ai_fashion_asset_rankings.py`，DDL `ddl/ads/ads_daily_ai_fashion_asset_rankings.sql`
- `decom.ads_daily_ai_fashion_asset_detail`：脚本 `jobs/ads/ads_daily_ai_fashion_asset_detail.py`，DDL `ddl/ads/ads_daily_ai_fashion_asset_detail.sql`

### 规则

- `docs/rules/asset-mapping.md`：素材主数据、帖子素材映射、帖子详情页素材曝光、素材试穿、素材负反馈、素材下方商品点击、素材模块/专栏归属
- `docs/rules/tryon-mapping.md`：试穿任务类型与素材试穿映射
- `docs/rules/data.md`：时间切天、去重、聚合规则

### Superset

- 物理 dataset：`ads_weekly_ai_fashion_asset_overview`（id=289）、`ads_daily_ai_fashion_asset_rankings`（id=290）、`ads_daily_ai_fashion_asset_detail`（id=291）
- chart（自上而下即 dashboard 展示顺序）：
  1. `每周新上线素材数据总览`（id=711）
  2. `每日试穿UV Top5`（id=712，`ranking_type='tryon_uv_top5'` 固定过滤）
  3. `每日素材下方商品点击UV Top5`（id=716，`ranking_type='product_click_uv_top5'` 固定过滤）
  4. `每日试穿UV Bottom50`（id=713，`ranking_type='tryon_uv_bottom50'` 固定过滤）
  5. `每日负反馈率Top50`（id=714，`ranking_type='negative_feedback_rate_top50'` 固定过滤）
  6. `素材明细`（id=715）
- 图表展示列均使用中文 label（`all_columns` 用 `{expressionType: SQL, label: 中文, sqlExpression: "列名 "}` 形式，`sqlExpression` 末尾补一个空格，这是当前 Superset 实例的已知实测修正方式）
- dashboard：`【AI-Fashion】素材内容数据`（id=128，slug `ai-fashion-asset-content-data`）
- native filter（顺序与作用范围均与 chart 自上而下顺序对齐，且已在 filter 名称中直接标注作用范围）：
  1. `周范围（周总览）`：`filter_select`，作用于 `week_range` 取值（数据集 289），不设默认值，仅作用于 chart 711
  2. `日报日期（榜单+明细）`：`filter_time`，默认「美东昨日」（`DATEADD(DATETRUNC(DATEADD(DATETIME("now"), -5, hour), day), -1, day) : DATETRUNC(DATEADD(DATETIME("now"), -5, hour), day)`，与 dashboard 122《每日排行榜》一致的既有写法），作用于 chart 712/716/713/714/715
  3. `内容模块（全部图表）`：`filter_select`，多选，不设默认值，作用于全部 6 个 chart
