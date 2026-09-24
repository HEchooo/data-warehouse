# 【AI-Fashion】素材内容数据 - 踩坑记录

## 1. 帖子详情页素材曝光的 `args.asset_code` 是标量，不是数组

- 问题：
  `docs/rules/asset-mapping.md` 原有「素材曝光」小节（`v_home_feeds`、`v_closet_feeds`）里 `args.asset_code` 是 JSON 数组，容易想当然认为所有素材曝光事件的 `asset_code` 都是数组。
- 实测：
  `v_brand_post_detail` / `v_magazine_post_detail` / `v_star_post_detail` 三个帖子详情页事件的 `args.asset_code` 实际是**标量字符串**。
- 处理：
  解析时用 `JSON_TYPE(args.asset_code) = 'array'` 判断分支，标量分支包一层 `[args.asset_code]` 再统一 `UNNEST`，兼容两种形态，避免未来埋点改成数组时静默漏数。
- 附加发现：
  该字段是 **2026-07-16 才开始上报**的，之前的事件完全不带 `asset_code`；全历史只有约 2.8 万行带该字段。

## 2. `c_feedback_entry` 目前不带 `asset_code`

- 现象：
  `c_feedback_entry` 全历史仅 179 条，且只带 `args.post` / `args.spu`，没有出现过 `args.asset_code`。
- 处理：
  口径仍按 `args.asset_code` 正常实现（与需求一致），当前负反馈 PV/UV/率全部为 0 属预期表现，不是脚本 bug；待埋点补齐 `asset_code` 字段后会自动产出数据。

## 3. 「有效曝光」500 UV 阈值远高于当前实际量级

- 现象：
  实测素材周曝光 UV 最大值仅 13（`2026-07-13` ~ `2026-07-20` 两周），阈值 500 目前恒定筛不出任何「有效曝光素材」。
- 处理：
  阈值仍写死为 500（脚本常量 `VALID_EXPOSURE_UV_THRESHOLD`），按用户决策保留，后续如需下调需要用户确认并只改常量。

## 4. 周报不能提前物化「未来周」

- 现象：
  `v3_decom.community_post` 中存在大量晚于当前日期的未来 `publish_time`（最远到当前日期 + 2 个月）。这只说明字段填了未来时间，不能据此断定发布状态。如果周入口只按 `dim_asset.publish_dt` 推算待处理周而不加上限，会把还没到来的周也算出「本周新上线素材总数」。
- 处理：
  `ads_weekly_ai_fashion_asset_overview` 的 `get_week_starts_to_process` 在全量和增量两个分支都加了 `week_start_dt <= 当前周` 的上限过滤。
- 仅排除未来周还不够：当前周内未来时刻的素材仍会被提前计入。`new_assets` 同时要求 `publish_time <= CURRENT_TIMESTAMP()`；业务确认到具体发布时间才计入，同一天稍晚发布的素材也不能提前计数。

## 5. `dim_post`/`dim_asset` 类维度表变更需要反查历史分区

- 参考模式（复用自 `ads_daily_post_tryon_confirm.py`）：
  `dim_asset` 是快照表，本身没有 `dt`；识别「哪些历史业务日需要因维度修正而重刷」时，不能对 `dim_asset` 直接按时间过滤，而是先找出最近变更的 `asset_code`，再反查目标 ADS 表里已有哪些 `dt` 用到了这些 `asset_code`。

## 6. 周表只保留一个「周」展示字段，但分区列不能丢

- 需求：
  周报表格里「周」只应该是一个字段（如 `2026-07-13 ~ 2026-07-19`），不要拆成 `week_start_dt` / `week_end_dt` 两列展示。
- 处理：
  `week_start_dt` 仍保留在物理表里作为 `PARTITION BY` 列，同时也是 Superset dataset 的 `main_dttm_col`（周报日期筛选器依赖它）；只是不再把它放进 chart 的 `all_columns` 展示列表。新增 `week_range STRING`（`FORMAT_DATE` 拼接 `week_start_dt` 与内部计算出的 `week_end_dt`）作为唯一对外展示的周字段。`week_end_dt` 仍在 ETL 脚本内部 CTE 里参与日期范围 JOIN，只是不再落到目标表。
- 注意：
  按 `docs/rules/code.md` 的 DDL 规则，字段变更不能直接改 `CREATE TABLE`，要在其下方追加 `ALTER TABLE ... DROP COLUMN` / `ALTER TABLE ... ADD COLUMN`。

## 7. Superset 明细表要用中文列名，正确做法是 `all_columns` 传对象而不是裸字符串

- 现象：
  `query_mode: raw` 时如果 `all_columns` 直接传字符串列名（如 `"asset_code"`），前端只会展示英文原始列名。
- 正确做法（参考已有的 `专栏追踪明细（日×专栏）-明细表`，chart id=546）：
  `all_columns` 每一项改成对象 `{"expressionType": "SQL", "label": "中文名", "sqlExpression": "列名 "}`（`sqlExpression` 末尾必须补一个空格，这是当前 Superset 实例的已知实测修正方式，否则会报错或显示异常）。
  `column_config` 的 key 要用中文 `label`，不是原始列名——因为使用 SQL 表达式列后，查询结果里字段名就是 `label`。
  `order_by_cols` / `orderby` 仍然用原始列名（不带尾随空格、不用 label），例如 `["dt", false]`，不受 label 影响。

## 8. 榜单类 chart 命名统一为「每日 + 指标 + Top/Bottom + 数字」

- 例如：`每日试穿UV Top5`、`每日试穿UV Bottom50`、`每日负反馈率Top50`，不要写成「试穿UV最低素材Bottom50」「负反馈率最高素材Top50」这类修饰词不一致的命名。

## 9. Dashboard native filter 的顺序和描述要能让用户一眼看出作用范围

- 处理：
  filter 顺序与 chart 从上到下的顺序对齐（先周总览对应的周范围，再榜单/明细对应的日报日期，全局的内容模块放最后）；同时把作用范围直接写进 filter 名称（如 `周范围（周总览）`）和 `description` 字段，不要只依赖用户自己猜。

## 10. 榜单类 chart 不要照搬明细表的全部宽表字段

- 现象：
  `ads_daily_ai_fashion_asset_rankings` 物理表为了让三个榜单共用同一份口径，保留了与明细表几乎一样的完整宽表字段（`creator`、`publish_time`、`online_days`、`exposure_pv`、`tryon_count`、`negative_feedback_pv` 等）；但榜单 chart 的价值在于突出「按哪个指标排序」，把全部字段都摆上去反而稀释了重点，也是无意义的信息冗余。
- 处理：
  chart 层的 `all_columns` 只保留「定位素材」的最小字段（日报日期、排名、素材ID、素材名称、内容模块、专栏名称、曝光UV）+「该榜单排序依据」的指标字段（试穿UV，或负反馈UV+负反馈率），物理表本身不需要跟着精简。

## 11. 榜单/明细类日报默认应给「美东昨日」而不是 `Last 30 days`

- 处理：
  沿用 dashboard 122《每日排行榜》已验证过的写法：
  `DATEADD(DATETRUNC(DATEADD(DATETIME("now"), -5, hour), day), -1, day) : DATETRUNC(DATEADD(DATETIME("now"), -5, hour), day)`
  用 UTC now 向后平移 5 小时近似美东时间再截天，取「昨天」。当前 Superset 实例的 `DATETIME()` 不支持直接指定 `America/Toronto`，这是目前代码库里对日榜单类 dashboard 的既有折衷写法，不是本主题独创。

## 12. 新增行为指标类字段，明细表的「当日有行为」过滤条件容易漏改

- 现象：
  `ads_daily_ai_fashion_asset_detail` 只保留当日曝光/试穿/负反馈任一不为 0 的素材（`WHERE d.exposure_pv > 0 OR d.tryon_count > 0 OR d.negative_feedback_pv > 0`）；新增「素材下方商品点击 UV」时如果只加了 SELECT 列、没有同步扩展这条 `WHERE`，会导致「只有点击、无曝光/试穿/负反馈」的素材整行被过滤掉，看起来像点击数据缺失，实际是明细表本身漏行。
- 处理：
  新增任何行为类指标后，先检查明细表末尾的「当日有行为」组合条件是否需要 `OR` 上新指标；本次改动加了 `OR d.product_click_uv > 0`。

## 13. `c_product` 的 `args.asset_code` 同样是标量、同样有明确起报时间

- 现象：
  `c_product`（素材下方商品点击）的 `args.asset_code` 与「帖子详情页素材曝光」小节的形态一致——**标量字符串**、**2026-07-25 起才开始上报**；实测全历史仅约 20~27 行带该字段（截至 2026-07-27），`args.page_key` 全部落在 `star_detail`/`magazine_detail`/`brand_detail` 三个帖子详情页。
- 处理：
  直接复用已有的 `asset_codes_unnest_expr()` 标量/数组兼容分支，不需要再判断 `page_key`（与曝光口径的页面范围天然一致）。新指标只落 UV（`product_click_uv`），DWS 层仍需落 PV（`product_click_pv`）作为用户粒度基础列，避免 ADS 层重复扫描事件表算 UV。

## 14. 「周」这类非时间取值的筛选器，不要硬套 `filter_time`

- 现象：
  周总览的周字段合并成 `week_range`（字符串）后，如果筛选器仍用 `filter_time` 对着 `week_start_dt` 做时间范围选择，本质上是给一个「取值型」维度套了时间控件的交互，用户没法直接按周挑选，还必须带一个不需要的默认时间范围。
- 处理：
  改成 `filter_select`，`targets` 指向 `dataset 289` 的 `week_range` 列，`defaultDataMask` 留空（不设默认值），与「内容模块」筛选器的写法一致。
