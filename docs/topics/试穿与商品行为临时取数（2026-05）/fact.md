# 试穿与商品行为临时取数（2026-05）

## 报表

- 类型：临时取数（后续用于看板需求评估）
- 时间段（固定周桶）：
  - `0427-0503`：`2026-04-27 ~ 2026-05-03`（日均除数 `7`）
  - `0504-0510`：`2026-05-04 ~ 2026-05-10`（日均除数 `7`）
  - `0511-0517`：`2026-05-11 ~ 2026-05-17`（日均除数 `7`）
  - `0518-0524`：`2026-05-18 ~ 2026-05-24`（日均除数 `7`）
- 产物：
  - SQL：`docs/topics/试穿与商品行为临时取数（2026-05）/query.sql`
  - 结果集：
    - 主指标（1~18）
    - 分桶指标（10、19）
    - 事件形状探查（11、12）

补充说明：

- 本需求为临时取数，SQL 直接读取 `decom.dwd_event_log` 与 `v3_decom.common_ext_ai_task`，暂不新增 ADS 表。
- 若后续进入稳定看板，需把可复用分子分母优先下沉到 `DWS` 后再由 `ADS` 消费。

## 字段

### 需求字段（完整清单）

| 序号 | 指标 | 口径 |
|------|------|------|
| 1 | 新用户试穿率 | 新用户试穿 UV / 新用户 DAU；新用户定义：`dim_user_first_active.first_active_date = dt` |
| 2 | 老用户试穿率 | 老用户试穿 UV / 老用户 DAU；老用户定义：`first_active_date < dt` |
| 3 | 首页信息流试穿 UV | `event_name = 'c_tryon' AND args_page_key = 'home_feeds'` 的 UV |
| 4 | 首页试穿渗透率 | 指标 3 / `event_name = 'v_home_feeds'` 的 UV |
| 5 | 帖子试穿 UV | `event_name = 'c_tryon' AND args_page_key NOT IN ('star','magazine','brand','kol')` 的 UV |
| 6 | 帖子试穿渗透率 | 指标 5 / 指定帖子场域曝光事件 UV（`v_star_post_feeds`、`v_star_post_detail`、`v_magazine_post_detail`、`v_magazine_post_feeds`、`v_brand_post_feeds`、`v_brand_post_detail`、`v_kol_post_feeds`、`v_kol_post_detail`） |
| 7 | 商品详情页试穿 UV | `event_name = 'c_tryon' AND args_page_key = 'product_detail'` 的 UV |
| 8 | 商品详情页试穿渗透率 | 指标 7 / `event_name = 'v_product_detail'` 的 UV |
| 9 | styleme 试穿 UV | `v3_decom.common_ext_ai_task.task_scene = 'STYLE_ME'` 的 `user_id` UV（按 `gmt_create` 切天） |
| 10 | 人均试穿次数分布 | 每日 `试穿总次数 / 试穿UV` 分桶：`(0~3]`、`(3~6]`、`(6~10]`、`(10~15]`、`15次以上` |
| 11 | 商品点击 UV | `event_name` 以 `c` 开头且 `product_code` 非空的 UV（并输出事件名形状） |
| 12 | 商品曝光 UV | `event_name` 以 `v` 开头且 `product_code` 非空的 UV（并输出事件名形状） |
| 13 | 首页商品点击 UV | `event_name = 'c_home_feeds' AND product_code` 非空的 UV |
| 14 | 首页商品曝光点击率 | 指标 13 / `event_name = 'v_home_feeds' AND product_code` 非空的 UV |
| 15 | 试穿结果页点击 UV | `event_name = 'c_tryon_result_list'` 的 UV |
| 16 | 试穿结果页曝光点击率 | `c_tryon_result_list` UV / (`v_tryon_result_detail` + `v_tryon_result_list`) UV |
| 17 | 商城商品点击 UV | `event_name = 'c_shop_feeds'` 的 UV |
| 18 | 商城商品曝光点击率 | `c_shop_feeds` UV / `v_shop_feeds` UV |
| 19 | 人均点击次数分布 | 每日 `商品点击 PV / 商品点击 UV` 分桶：`(0~3]`、`(3~5]`、`(5~10]`、`(10~15]`；`PV` 按 `raw_event_id` 去重 |

### 已落地字段

| 需求字段 | 落地位置 | 落地字段 | 口径 |
|------|------|------|------|
| 指标 1~18 | `query.sql` 的主指标结果集 | `metric_name` / `metric_value` | 主指标统一输出为 `period_tag + metric_name + metric_value` |
| 指标 10 | `query.sql` 的分桶结果集 A | `tryon_per_tryon_uv_bucket` / `day_count` / `day_ratio` | 基于每日 `c_tryon` 去重次数（`COALESCE(raw_event_id, hash_id)`）除以每日试穿 UV |
| 指标 19 | `query.sql` 的分桶结果集 B | `click_per_uv_bucket` / `day_count` / `day_ratio` | 基于每日商品点击 `PV(raw_event_id 去重)` / 商品点击 UV |
| 指标 11/12 探查 | `query.sql` 的事件形状结果集 | `event_name` / `row_cnt` / `uv_user` / `pv_raw_event` | 输出各周桶内 `c* / v* + product_code 非空` 的事件名分布 |

补充说明：

- 日均 UV 口径：均为“先按天算 UV，再按区间天数求均值（`SUM(day_uv) / 天数`）”。
- 率类口径：按区间内“每日分子 UV 之和 / 每日分母 UV 之和”计算，等价于“日均分子 UV / 日均分母 UV”。
- UV 去重主键：事件侧统一使用 `prop_user_id`（非空）；服务端 styleme 使用 `user_id`（转字符串后非空）。
- DAU：按“任意活跃事件”的 `prop_user_id` 日去重。

## 依赖

- 上游：`decom.dwd_event_log`、`decom.dim_user_first_active`、`v3_decom.common_ext_ai_task`
- 规则：
  - `docs/rules/data.md`（时区与去重）
  - `docs/rules/layering.md`（分层职责）
  - `docs/rules/topics.md`（主题文档结构）
- 查询文件：`docs/topics/试穿与商品行为临时取数（2026-05）/query.sql`
