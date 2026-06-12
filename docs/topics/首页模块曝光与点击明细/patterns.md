# 首页模块曝光与点击明细经验

## 模块维度不要做成动态扩散

- 首页模块是少量且稳定的固定映射
- 维护时优先写显式映射，不要为了抽象再包动态 SQL

## 先确认是不是固定四行预期

- 排查"缺行 / 多行"时，先确认当天是不是应该固定出现 star / magazine / brand / feeds 四个模块
- 需要补齐零值模块时，优先 `CROSS JOIN + LEFT JOIN`，不要因为某天没事件就少行

## 这张表直接读 `dwd_event_log`

- 模块口径异常时，先回看事件名映射、`prop_user_id` 登录过滤和 `raw_event_id` 去重，不要先去翻 `dws_*`

## 登录口径要先区分“去重键”与“事件纳入范围”

- 如果需求改成“只看已登录用户”，不能只把 UV 的去重键改成 `prop_user_id`
- 还要先把事件输入范围收口到 `prop_user_id IS NOT NULL AND prop_user_id != ''`
- 否则会出现 UV 已按登录用户统计，但点击 PV 仍混入匿名用户事件，导致点击率、人均点击次数口径不一致

## 首页模块映射和补零骨架放在 DWS，ADS 只算率

- `star / magazine / brand / feeds` 的固定模块映射应在 `dws_home_module_daily` 维护，不要让终层报表自己重复维护事件名映射
- 四模块补零也放在中层统一完成，这样 `ads_daily_home_module_performance` 只保留 `click_rate / avg_click_count_per_user` 两个展示指标
- 后续排查模块日报异常时，先检查 `dws_home_module_daily` 的四行骨架和基础事实，再看 ADS 率指标是否正确

## 首页内容曝光 ID 不能按单值直接清洗

- `v_home_star / v_home_magazine / v_home_brand` 的 `args_star / args_magazine / args_brand` 在实际数据中主要是 JSON 数组。
- 临时取内容 ID 排名时要兼容数组和少量历史单值；必须先拆出每个内容 ID，再按 `用户 × 日期 × 模块 × 内容 ID` 统计 UV。
- 模块归属应以事件名及对应参数字段为准，不能再用 `kol_rel.kol_type` 反向覆盖；实际存在 `v_home_magazine.args_magazine` 上报 `kol_type = 7` ID 的情况。
