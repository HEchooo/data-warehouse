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

## 试穿口径不要只看 c_tryon

- 平台整体内容表现里的“试穿”业务概念已扩成 `c_styleme + c_tryon`
- 后续再做平台级试穿 UV / 渗透率时，先核对是否沿用这个统一口径，不要只抓旧的 `c_tryon`

## 服务端行为映射平台不能直接连 `dws_user_daily`

- `dws_user_daily` 的粒度是 `dt × prop_user_id × platform`，同一用户同日跨平台活跃时会有多行
- 平台整体内容表现如果直接把 `community_post_like` / `community_star_follow` / `common_ext_ai_task` 连到 `dws_user_daily`，同一条服务端行为会被重复分摊到多个平台
- 正确做法是回到同日登录事件，按行为时间匹配同用户最近的登录事件平台；未命中再回落到 `unknown`

## 内容场景试穿要限定 `postId`

- `common_ext_ai_task` 里不带 `request_param.postId` 的 TRY_ON / STYLE_ME 不能直接并入“平台整体内容表现”
- 这张报表的试穿口径是内容场景试穿，不是全平台所有 AI 试穿任务

## Superset chart 不能只更新 params，不更新 query_context

- 这次 91 看板里“转化趋势 / 明细表”报 `Unexpected error`，先是因为 BigQuery 生产表缺少 `column_follow_rate`
- 补列并回刷后，如果只用 API 更新 chart 的 `params`，老 chart 仍可能继续执行旧 `query_context`，表现为新字段没生效，甚至继续报错
- 保险做法是：补齐底表字段后，同时重建或重写 chart 的 `query_context`
- 后续再遇到新增指标列时，不要只改展示参数，要同步检查并重写 chart 的 `query_context`

## `INSERT` 列顺序和 `SELECT` 列顺序必须一起校对

- 这次 `ads_daily_content_performance` 空表，不是 `tryon_total_count` 口径错了，而是 `INSERT INTO (...)` 的目标列顺序和最终 `SELECT` 的输出列顺序错位
- 实际把 `follow_conversion_rate` 这个 `NUMERIC` 写到了 `tryon_total_count` 这个 `INT64` 位置，触发 BigQuery 类型报错
- 脚本又采用了先 `DELETE` 再 `INSERT` 的写法，所以失败后目标分区已经删掉，91 看板直接读到空表
- 后续新增、删除、重排展示字段时，必须把 `INSERT` 列清单和最终 `SELECT` 做逐列位次校对，不能只看字段名是否都存在
