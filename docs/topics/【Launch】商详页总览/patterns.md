# 【Launch】商详页总览 踩坑记录

## DWS 加维度后 UV 不能再用 COUNTIF 按行计数

- 背景：为支撑多维度看板，`dws_launch_product_detail_device_daily` 从 `dt × device_id` 扩展为 `dt × video_id × channel × kol_name × country × merchant × device_id`
- 影响：同一设备同日可能因浏览了不同视频/渠道而产生多行，原先 `COUNTIF(product_detail_pv > 0)` 会把同一设备重复计数
- 修复：改为 `COUNT(DISTINCT IF(product_detail_pv > 0, device_id, NULL))`，对设备去重后再计数
- 排查提示：任何基于该 DWS 的下游报表，凡涉及 UV，都必须对 `device_id` 去重，不能直接对行数或对各维度 UV 求和
