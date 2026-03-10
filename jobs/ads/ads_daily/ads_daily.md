# ADS 日报总览 (ads_daily)

## 目标

统一编排 ADS 日报任务，按天增量刷新以下报表：
- `ads_daily_new`
- `ads_daily_total`
- `ads_daily_content_performance`
- `ads_daily_home_module_performance`
- `ads_daily_user_duration_frequency`

## 目录结构

```text
jobs/ads/
├── ads_daily/
│   ├── ads_daily.py                                 # 入口编排 + 增量日期识别
│   └── ads_daily.md                                 # 总览文档
├── ads_daily_new/
│   ├── ads_daily_new.py                             # ads_daily_new 任务
│   └── ads_daily_new.md                             # ads_daily_new 口径
├── ads_daily_total/
│   ├── ads_daily_total.py                           # ads_daily_total 任务
│   └── ads_daily_total.md                           # ads_daily_total 口径
├── ads_daily_content_performance/
│   ├── ads_daily_content_performance.py             # 内容表现日报任务
│   └── ads_daily_content_performance.md             # 内容表现日报口径
├── ads_daily_home_module_performance/
│   ├── ads_daily_home_module_performance.py         # 首页模块表现日报任务
│   └── ads_daily_home_module_performance.md         # 首页模块表现日报口径
└── ads_daily_user_duration_frequency/
    ├── ads_daily_user_duration_frequency.py         # 用户时长与访问频次日报任务
    └── ads_daily_user_duration_frequency.md         # 用户时长与访问频次日报口径
```

## 执行流程

入口脚本：`jobs/ads/ads_daily/ads_daily.py`

执行顺序：
1. 在 `ads_daily.py` 内识别待处理日期
2. 执行 `jobs/ads/ads_daily_new/ads_daily_new.py`
3. 执行 `jobs/ads/ads_daily_total/ads_daily_total.py`
4. 执行 `jobs/ads/ads_daily_content_performance/ads_daily_content_performance.py`
5. 执行 `jobs/ads/ads_daily_home_module_performance/ads_daily_home_module_performance.py`
6. 执行 `jobs/ads/ads_daily_user_duration_frequency/ads_daily_user_duration_frequency.py`

## 设计原则

- 表级任务隔离：每张表一个子目录，便于独立维护
- 入口聚合：`ads_daily.py` 负责编排与日期识别
- 任务独立：每个子目录内脚本自包含，避免跨目录强依赖
