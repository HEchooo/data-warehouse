# 平台整体内容表现经验

## 91 报表的内容曝光口径要和帖子内容保持一致

- 关键不是"补一个 `read_rate` 字段"，而是先统一内容曝光分母
- `avg_browse_content_count_per_user`、`like_rate`、`read_rate` 都应共用帖子内容曝光口径
- 顺序上要先把 `v_product_detail` 从分母里排除，再补 `read_rate`

## 先修正现有 dataset，不要平行再造 v2

- Superset 里同名 dataset 曾错误指向 `decom.ads_daily_total`
- 后续再遇到同类问题，优先修正现有对象绑定，不要平行新建副本

## 看板格式问题先看脚本和 dashboard 配置

- 默认时间范围、secondary axis 百分比格式、中文列名、明细表列展示都踩过坑
- 优先检查脚本和 dashboard 配置，不要先怀疑 ADS 表本身

## 专栏关注率要和旧关注率分开

- `follow_rate` 仍然是 `follow_uv / dau_uv`，不能直接改口径
- 新增“专栏关注率”时，单独落字段 `column_follow_rate`
- 该字段分母只包含 7 个专栏内容曝光事件：`v_kol_post_feeds`, `v_kol_post_detail`, `v_star_post_feeds`, `v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_feeds`, `v_brand_post_detail`

## Superset chart 不能只更新 params，不更新 query_context

- 这次 91 看板里“转化趋势 / 明细表”报 `Unexpected error`，先是因为 BigQuery 生产表缺少 `column_follow_rate`
- 补列并回刷后，如果只用 API 更新 chart 的 `params`，老 chart 仍可能继续执行旧 `query_context`，表现为新字段没生效，甚至继续报错
- 保险做法是：补齐底表字段后，同时重建或重写 chart 的 `query_context`
- 后续再遇到新增指标列时，不要只改展示参数，要同步检查并重写 chart 的 `query_context`
