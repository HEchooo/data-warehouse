# 新老用户帖子浏览抽样临时取数（2026-06）经验

## 帖子事件已在 DWD 拆行

- `dwd_event_log.post_code` 已由 `args.post` 拆出；一个原始事件包含多个帖子时，会形成多条共享 `raw_event_id` 的明细。
- 帖子粒度浏览次数应在 `用户 × 日期 × post_code` 内按 `raw_event_id` 去重，不能只按 DWD 行数计数。

## 临时抽样必须可复现

- `RAND()` 每次执行都会改变样本，不适合交付后复核。
- 固定盐值的 `FARM_FINGERPRINT` 排序可以在源数据不变时稳定复现每日新老用户样本。

## 素材编码与图片必须按原数组下标对齐

- `mainAssets[*].assetCode` 与 `mainAssets[*].image.url` 是同一个素材对象的属性。
- 不能分别去重或按值排序两个数组，否则可能破坏素材和图片的对应关系；应使用相同过滤条件并按 `WITH OFFSET` 的原始下标排序。
