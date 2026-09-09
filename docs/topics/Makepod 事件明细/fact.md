# Makepod 事件明细

## 检索别名

| 类型 | 标准名称 | 别名 |
|---|---|---|
| 字段 | 事件名 | event_name、埋点事件 |
| 字段 | 设备 ID | prop_device_id、device_id |
| 字段 | 用户 ID | prop_user_id、user_id |
| 字段 | product_code | 商品编码、SPU、args_spu |
| 字段 | sku_code | SKU 编码、args_sku |

## 报表

- 表：`decom.dwd_makepod_event_log`
- 粒度：Makepod 原始事件按 `args.spu` 商品拆行后的事件商品明细
- 原始事件主键：`raw_event_id`

## 字段

| 字段 | 口径 |
|---|---|
| raw_event_id | 拆行前原始事件字段的 MD5；同一事件拆出的商品行共享此值 |
| event_name | 原始事件名，统一转小写 |
| logAt_timestamp | 客户端事件时间 |
| session_id | 客户端会话 ID |
| prop_device_id | 设备 ID |
| prop_user_id | 用户 ID；允许匿名空值 |
| prop_os | 客户端系统类型 |
| prop_url | 事件页面 URL |
| prop_params | ODS 已有 URL 参数字段，当前上游解析来源为 `properties.params` |
| prop_app_type | 应用类型 |
| prop_timezone | 客户端时区 |
| prop_app_version | 客户端版本 |
| prop_share_code | 分享码 |
| invite_user_id | 由 `prop_share_code` 按 Base64 解码后转成 64 位整数，并与固定掩码 `873645731` 做异或得到的邀请用户 ID |
| prop_version_type | 固定筛选 `makepod` |
| country | 顶层国家字段 |
| args | 完整原始 args JSON |
| args_page_key | `args.page_key` |
| args_module | `args.module` |
| args_element | `args.element` 原始 JSON；兼容字符串和数组 |
| args_category_id | `args.category_id` |
| args_spu | `args.spu` 原始 JSON |
| args_sku | `args.sku` 原始 JSON |
| product_code | `args.spu` 标量或数组元素；数组按元素拆行；相同商品/SKU 对只保留第一次出现 |
| sku_code | `args.sku` 标量，或与 `args.spu` 相同 offset 的数组元素 |
| ext | 完整原始 ext JSON |
| oss_key | GCS 日志文件路径 |
| update_time | 本次 DWD 写入时间 |

## 依赖

- 埋点文档：`docs/references/Makepod 数据埋点.csv`
- 上游：`decom.ods_event_log`
- 增量状态：`decom.oss_key_process_log.makepod_analyze_state`
- 状态初始化：ODS 每写入一个日志文件，固定写入 `makepod_analyze_state = 0`，不判断文件内容
- 状态消费：Makepod DWD 独立扫描本批状态为 `0` 的日志文件，完成后将同一批 key 置为 `1`
- ODS 脚本：`jobs/ods/ods_event_log/main.py`
- DWD 脚本：`jobs/dwd/dwd_makepod_event_log/dwd_makepod_event_log.py`
- ODS DDL：`ddl/ods/ods_event_log.sql`, `ddl/ods/oss_key_process_log.sql`
- DWD DDL：`ddl/dwd/dwd_makepod_event_log.sql`
- 编排：`env/etl_config.json`
