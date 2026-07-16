# 【Social Content】属性维度数据踩坑记录

## 新 Dashboard 不应仅按指标复用关系归入旧 topic

- 本看板虽复用社媒表现数据看板v2 的 8 个指标，但具有独立 Dashboard、独立数据范围、独立属性映射和筛选作用域，应建立独立 topic
- 判断 topic 归属时，不能只看指标是否复用；还要看报表交付物、数据范围与依赖是否具备独立边界

## Superset cohort 多 metric 同图在当前实例返回 500

- 将 `T / T+1 / T+3 / T+7 / T+14` 作为 5 个 metric 直接放入同一张 ECharts 折线图时，`POST /api/v1/chart/data` 返回 500
- 当前实例的稳定方案是把观察窗展开为 `horizon_label + view_count + video_count` 长表，再使用单个平均播放 metric

## 无 cohort 样本时不能把分子分母补成 0

- 物理链路首次回填时，已成熟日期但某属性组合没有发布视频，若将 cohort 的 `view_count / video_count` 补成 `0 / 0`，会与原 virtual dataset 的 `NULL / NULL` 语义不一致
- 正确做法是保留 cohort 分子分母的 `NULL`；只有真实存在 cohort 样本时，才产出播放总和与视频数
