# 平台整体内容表现经验

## 91 报表的内容曝光口径要和帖子内容保持一致

- 关键不是"补一个 `read_rate` 字段"，而是先统一内容曝光分母
- `avg_browse_content_count_per_user`、`like_rate`、`read_rate` 都应共用帖子内容曝光口径
- 顺序上要先把 `v_product_detail` 从分母里排除，再补 `read_rate`

## 改“已登录用户口径”时，不能只改 UV 不改关联 PV

- 91 报表里 `platform_exposure_uv`、`content_exposure_uv`、`product_detail_exposure_uv`、`read_uv`、`dau_uv` 改成已登录用户后，关联的事件侧 PV 也要一起收口到 `prop_user_id` 非空事件
- 否则会出现 `avg_browse_content_count_per_user = 全量 content_exposure_pv / 已登录 content_exposure_uv`、`product_detail_exposure_pv` 仍混入匿名事件的混合口径
- 正确做法是：事件侧先统一过滤 `prop_user_id IS NOT NULL AND prop_user_id != ''`，再分别沿用各字段原本的 PV 去重键和 `COUNT(DISTINCT prop_user_id)` 的 UV 口径

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

- 平台整体内容表现里的“试穿”当前以服务端 `common_ext_ai_task` 为准
- 91 报表当前统计 `task_type IN ('TRY_ON', 'OUTFIT')` 的平台级试穿任务，不再限定 `request_param.postId`

## 服务端行为映射平台不能直接连 `dws_user_daily`

- `dws_user_daily` 的粒度是 `dt × prop_user_id × platform`，同一用户同日跨平台活跃时会有多行
- 平台整体内容表现如果直接把 `community_post_like` / `community_star_follow` / `common_ext_ai_task` 连到 `dws_user_daily`，同一条服务端行为会被重复分摊到多个平台
- 正确做法是回到同日登录事件，按行为时间匹配同用户最近的登录事件平台；未命中再回落到 `unknown`

## `dau_uv` 不能把被动触达事件算成活跃

- `send_push`、`send_email` 只能说明系统触达用户，不能说明用户真的访问了平台内容
- 如果把它们纳入 `dau_uv` 分母，`product_click_rate`、`follow_rate`、`tryon_penetration_rate` 会被系统性压低

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

## 平台主题基础事实先落 DWS，再让 ADS 只算率

- `platform_exposure_uv / content_exposure_uv / product_click_uv / like_total_count / follow_uv / tryon_uv` 这类平台级基础事实应沉到 `dws_platform_content_daily`
- `avg_browse_content_count_per_user / like_rate / product_click_rate / follow_rate / follow_conversion_rate / read_follow_rate / tryon_penetration_rate` 这类展示率指标留在 `ads_daily_content_performance`
- 这样可以避免平台事实在多个 ADS 里重复拼接事件与服务端行为，也能把增量日期判断收口到单一中层表
