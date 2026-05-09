# 衣橱规则

## 主题范围

- 本主题覆盖衣橱首页曝光、衣橱新增使用、衣橱新增 item、累计使用用户和渗透率
- 事件侧主口径来自 `decom.dwd_event_log`
- 服务端主口径来自 `v3_decom.user_fashion_closet`

## 主键与时间映射

- 用户映射：
  - 事件侧用户主键使用 `dwd_event_log.prop_user_id`
  - 服务端用户主键使用 `user_fashion_closet.user_id`
  - 当前已核对样本，两边可直接按字符串等值 join
  - 首次使用口径统一沉淀到 `decom.dim_user_closet_first_use`
- 事件时间：
  - `logAt_timestamp` 不能直接当 UTC 使用
  - 统一先按 `prop_timezone` 解释，再转换为 `TIMESTAMP`
  - 业务日表达式：
    - `DATE(TIMESTAMP(DATETIME(logAt_timestamp, 'UTC'), COALESCE(NULLIF(prop_timezone, ''), 'UTC')), 'America/Toronto')`
- 服务端时间：
  - `user_fashion_closet.gmt_create` 是 UTC `DATETIME`
  - 业务日表达式：
    - `DATE(TIMESTAMP(gmt_create, 'UTC'), 'America/Toronto')`

## 类型映射

- `type = 'SINGLE'`
  - 表示衣橱单品创建
  - 用于：
    - 新增 item 数量
    - 具体按非空 `uuid` 计 1 个 item
- `type = 'OUTFIT'`
  - 表示衣橱穿搭结果创建
  - 用于：
    - 新增衣橱使用用户
    - 总计使用用户
    - 渗透人数的“当日加入衣橱”侧
    - 新增 item 数量；具体按 `item_uuids` 拆数组后的有效元素个数计数
- 服务端相关通用口径
  - 只要指标依赖 `user_fashion_closet`，默认统一使用 `type IN ('SINGLE', 'OUTFIT')`

## 指标口径

- 衣橱首页曝光 UV
  - `event_name = 'v_closet'`
  - `COUNT(DISTINCT prop_user_id)`
  - 必须过滤 `prop_user_id IS NOT NULL AND prop_user_id != ''`
- 新增衣橱使用用户
  - 先取 `decom.dim_user_closet_first_use.first_any_dt`
  - 再按首日做 `COUNT(DISTINCT user_id)`
- 新增 item 数量
  - 当日 `type = 'SINGLE'` 的记录，按非空 `uuid` 计数
  - 当日 `type = 'OUTFIT'` 的记录，按 `item_uuids` 拆数组后的有效元素个数求和
  - `item_uuids` 中若出现 `NULL` / 空字符串等异常元素，需先过滤再计数
  - 不再附着“首日用户”条件
- 总计使用用户
  - 先取 `decom.dim_user_closet_first_use.first_any_dt`
  - 再按业务日累计 `COUNT(DISTINCT user_id)`

## 首次使用维表

- `decom.dim_user_closet_first_use`
  - `user_id`
  - `first_single_dt`：首次 `type = 'SINGLE'` 的业务日
  - `first_any_dt`：首次 `type IN ('SINGLE', 'OUTFIT')` 的业务日
- 维护方式：
  - 首次全量初始化
  - 后续只处理最近 6 小时 `user_fashion_closet` 变更行
  - 用 `MERGE` 仅向更早首日收敛，不在下游日事实里重复全扫源表
- 渗透人数
  - 同日满足两件事的去重用户数：
    - 日志中存在 `event_name = 'v_closet'`
    - 服务端当日存在 `type IN ('SINGLE', 'OUTFIT')`
- 渗透率
  - 分子：渗透人数
  - 分母：日志中当日所有事件的 `COUNT(DISTINCT prop_user_id)`
  - 必须过滤 `prop_user_id IS NOT NULL AND prop_user_id != ''`

## 统计约束

- 本主题所有 UV 指标都统一使用已登录用户口径，不混入 `device_id`
- 当前默认不使用 `is_deleted` 回溯修正历史使用行为
- 原因：
  - 本主题统计的是“创建/使用行为是否发生过”，不是当前衣橱存量
  - 删除动作不应回冲历史发生过的曝光、创建和使用人数
