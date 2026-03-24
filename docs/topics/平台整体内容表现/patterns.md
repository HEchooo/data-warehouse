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
