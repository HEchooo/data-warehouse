# LTA 数据看板 踩坑记录

## DWD 中第三方事件按商品拆多行

- 现象：`v_third_party_goods` 单个 `raw_event_id` 在 `dwd_event_log` 中对应多行（最多 53 行），每行一个 `product_code`，但 `args`（含完整 `spu[]/spu_name[]` 数组）完全一致；`c_third_party_goods` 也有 1~2 行的重复。
- 影响：若直接对逐行 `args.spu` 数组做 UNNEST 会产生 N×N 行爆炸；直接 `COUNT(*)` 会高估曝光次数。
- 处理：DWS 先按 `raw_event_id` 去重（`QUALIFY ROW_NUMBER() = 1`）再展开数组；PV 统一用 `COUNT(DISTINCT raw_event_id)`。

## 不要用小样本给 args.spu 的形态下结论

- 早期只看了一个用 SP 编码的珠宝 feed，就误判成"曝光=SP 编码、点击=外链 URL，二者不同源、无法对齐"，这是错的。
- 实测（近 40 天 mycch）：曝光 `args.spu` 链接 URL 996 个 / SP 编码 95 个，点击链接 URL 189 个 / 品牌名 14 个——**曝光也以链接为主，和点击是同一套取值空间，可在同一 `spu` 上对齐**。
- 结论：`args.spu` 原样当商品 ID 即可，不区分 SP 编码 / 链接；表3 里 30 个商品同时有曝光和点击，转化率有意义。
- 多数商品只落在曝光或点击一侧，是"一次 feed 曝光带大量商品、用户只点少数"的长尾现象，非口径错误。
- 转化率可能 >100%：`PV = COUNT(DISTINCT raw_event_id)` 下点击事件数可多于曝光事件数，属既定口径自然结果，当前未截顶。

## 域名过滤

- `args.current_url` 同时存在 `mycch.me` 与 `alvinclub.*`、`127.0.0.1` 等；`c_third_party_goods` 约两成行 `current_url` 为空。
- 用 `NET.HOST(...) = 'mycch.me'` 精确匹配即可（当前无 `www.mycch.me` 子域）；`NET.HOST(NULL)` 返回 `NULL`，空 URL 自动被过滤。

## kol_rel 存在重复 user_id

- `v3_decom.kol_rel` 有少量重复 `user_id`（约 50 个），直接 join 会造成扇出、放大 PV/UV。
- DWS 关联前先 `GROUP BY user_id` 取 `MAX(nickname)` 去重。
