# 【Social Content】联盟项目数据看板 排查经验

## 对账号不要用 username，对视频不要用广告 video_id

- 广告 `/identity/get/` 的 `identity_id`（BC_AUTH_TT 是 UUID）对不上内部 `users.id` / `open_id` / `union_id`。
- `/ad/get/` 的 `video_id` 实测全空。素材 ID 只在 `tiktok_item_id`，等于 `dim_video.video_id`。
- `username` 只给 ECN affiliation 查询和看板展示用。排查对不上视频时，先看 ODS 的 `tiktok_item_id` 是不是空（历史分区在扩列前回填前会是空）。

## Gross revenue (Shop) 不是 GMV Max 的 gross_revenue

- 后台列 Gross revenue (Shop) 对应 AUCTION 报表 `shop_gross_revenue_by_order_submission`。
- AUCTION 里请求 `gross_revenue` 会被拒。那是 GMV Max 全渠道收入，不能填进本看板。
- ROI 用 `广告 GMV / spend` 现算。接口 `onsite_shopping_roas` 是四舍五入到两位的同一比值，不能拿去 SUM / AVG。

## ECN 的 404 / 409 不是任务失败

- affiliation 查不到或不是美墨联盟：落一行空 `affiliation_label`，region 保持空。
- promotion-entry 尚未投流：落一行空 `ads_first_sent_at`，广告字段保持空，投流前件数走当日 RPA。
- 只有 401 / 5xx / 参数 422 才应让 ODS 失败。

## `/identity/get/` 的 page_size 上限是 100

- 报表 `/report/integrated/get/` 可以 `page_size=1000`。identity 接口传 1000 会报 `code=40002 page_size: number must be most 100`。
- 返回列表在 `data.identity_list` 而不是报表那种 `data.list`；`page_info` 可能全是 0，不能靠 `total_page` 翻页。
