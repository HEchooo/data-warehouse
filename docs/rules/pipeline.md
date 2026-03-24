# Pipeline 规则

## ADS 日报编排

### 目标

统一编排 ADS 日报任务，按天增量刷新以下报表：
- `ads_daily_new`
- `ads_daily_total`
- `ads_daily_content_performance`
- `ads_daily_tryon_confirm`
- `ads_daily_tryon_add_cart_conversion`
- `ads_daily_post_tryon_confirm`
- `ads_daily_post_performance`
- `ads_daily_column_performance`
- `ads_daily_product_tryon_performance`
- `ads_daily_home_module_performance`
- `ads_daily_user_duration_frequency`

### 执行流程

入口脚本：`jobs/ads/ads_daily.py`

执行顺序：
1. 在 `ads_daily.py` 内识别待处理日期
2. 执行 `jobs/ads/ads_daily_new.py`
3. 执行 `jobs/ads/ads_daily_total.py`
4. 执行 `jobs/ads/ads_daily_content_performance.py`
5. 执行 `jobs/ads/ads_daily_tryon_confirm.py`
6. 执行 `jobs/ads/ads_daily_tryon_add_cart_conversion.py`
7. 执行 `jobs/ads/ads_daily_post_tryon_confirm.py`
8. 执行 `jobs/ads/ads_daily_post_performance.py`
9. 执行 `jobs/ads/ads_daily_column_performance.py`
10. 执行 `jobs/ads/ads_daily_product_tryon_performance.py`
11. 执行 `jobs/ads/ads_daily_home_module_performance.py`
12. 执行 `jobs/ads/ads_daily_user_duration_frequency.py`

### 规则

- 主题文档按业务主题维护：每个主题目录统一使用 `fact.md`（既定事实）和 `patterns.md`（踩坑记录）
- 入口聚合：`ads_daily.py` 负责编排与日期识别
- 任务独立：各表脚本自包含，避免强耦合
- 日期处理：默认允许处理到多伦多当天 `dt`（当天数据可能不完整）
  - 为支持 T+0 展示，入口会强制把 DWS 中“当天/昨天”的 `dt` 纳入待处理日期（若 DWS 已存在对应分区）
  - 留存/到期类指标以各任务内部成熟度逻辑为准
