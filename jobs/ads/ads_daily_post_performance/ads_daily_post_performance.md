# 内容维度日报（日×帖子）(ads_daily_post_performance)

## 报表定义

统计每日帖子维度的曝光、点赞、关注归因与试穿行为，支持按模块（star/magazine/brand）与专栏（对应的 star_id/magazine_id/brand_id）筛选。

**粒度**：`dt × post_id × module × column_id`

## Filters（筛选维度）

- 日期：`dt`
- 帖子ID：`post_id`
- 帖子名称：`post_name`
- 创建者：`creator`
- 模块：`module`（`star` / `magazine` / `brand`）
- 专栏：`column_name`（展示）/ `column_id`（ID）

## 表结构

| 字段 | 类型 | 说明 |
|------|------|------|
| dt | DATE | 日期（多伦多时间） |
| post_id | INT64 | 帖子 ID（由 post_code 映射） |
| post_code | STRING | 帖子 code（埋点字段） |
| post_name | STRING | 帖子名称（由 post_code 映射） |
| creator | STRING | 帖子创建者（由 `v3_decom.community_post.creator` 映射） |
| module | STRING | 模块：`star` / `magazine` / `brand` |
| column_id | STRING | 专栏 ID（对应 star_id/magazine_id/brand_id） |
| column_name | STRING | 专栏名称（`kol_rel.nickname`，仅取 `status=0`） |
| post_exposure_uv | INT64 | 帖子曝光 UV（进入帖子详情页的去重用户数） |
| like_total_count | INT64 | 帖子点赞数（点赞成功次数，PV） |
| like_rate | NUMERIC | 帖子点赞率（点赞 UV / 帖子曝光 UV） |
| read_rate | NUMERIC | 帖子完读率（阅读 UV / 帖子曝光 UV） |
| follow_total_count | INT64 | 帖子相关关注数（关注成功次数，按 last-touch 归因到帖子详情曝光） |
| follow_rate | NUMERIC | 帖子关注转化率（归因关注 UV / 帖子曝光 UV） |
| tryon_total_count | INT64 | 从该帖子发起的试穿次数（try on 成功次数，PV） |
| update_time | TIMESTAMP | 更新时间（UTC） |

**分区**：`dt`

## 指标口径

### 访客去重口径（UV）

`visitor_id = COALESCE(NULLIF(prop_user_id,''), NULLIF(prop_device_id,''))`

### 帖子曝光 UV（post_exposure_uv）

- **定义**：当天进入该帖子详情页的去重用户数
- **事件范围**（仅详情页）：`v_star_post_detail`, `v_magazine_post_detail`, `v_brand_post_detail`
- **去重**：`COUNT(DISTINCT visitor_id)`，按 `dt × post_code × module × column_id` 统计
- **注意**：`dwd_event_log` 会展开 `args_post`，这里按 post_code 统计 UV，不会因为拆行重复计数

### 帖子点赞数（like_total_count）

- **定义**：当天对该帖子点赞成功的次数
- **事件范围**：`c_like`
- **去重**：按 `raw_event_id` 去重后计数（避免 `args_post` 拆行导致重复）

### 帖子点赞率（like_rate）

- **定义**：点赞 UV / 帖子曝光 UV
- **用户侧逻辑**：同一用户同一天同一帖子曝光多次，只要当天点赞过一次，则该用户当天该帖子视为“点赞=1”
- **计算**：`like_uv / post_exposure_uv`

### 帖子完读率（read_rate）

- **定义**：阅读 UV / 帖子曝光 UV
- **事件范围**：`r_star_post_detail`, `r_magazine_post_detail`, `r_brand_post_detail`, `r_kol_post_detail`
- **用户侧逻辑**：同一用户同一天同一帖子曝光多次，只要当天阅读过一次，则该用户当天该帖子视为“阅读=1”
- **计算**：`read_uv / post_exposure_uv`

### 帖子相关关注数（follow_total_count）

- **定义**：由帖子详情页触发的关注成功次数（归因到帖子）
- **事件范围**：`c_follow`
- **归因**：关注事件不包含 post_code，使用同一天内该用户在关注发生前的**最后一次帖子详情页曝光**（last-touch）归因到 post_code/module/column_id
- **专栏约束**：仅当末次曝光能解析出有效的 `module` 和 `column_id` 时，关注才会计入本表；若末次曝光缺少专栏参数，则该关注不纳入本表统计
- **去重**：关注 PV 按 `raw_event_id` 去重后计数

### 帖子关注转化率（follow_rate）

- **定义**：归因关注 UV / 帖子曝光 UV
- **用户侧逻辑**：同一用户同一天同一帖子只要发生过一次“归因关注”，则计为关注 UV=1
- **计算**：`follow_uv / post_exposure_uv`

### 从该帖子发起的试穿次数（tryon_total_count）

- **定义**：当天从该帖子触发的试穿（try on）成功次数
- **事件范围**：`c_tryon`
- **去重**：按 `raw_event_id` 去重后计数

## 维度映射

### post_code → post_id/post_name/creator

使用表：`v3_decom.community_post`（`id`, `post_code`, `title`, `creator`）

### column_id → column_name

使用表：`v3_decom.kol_rel`（`status`, `user_id`, `nickname`, `kol_type`）

- 说明：来自埋点日志的 `column_id` 可能携带首尾双引号（例如 `"2602276189664"`），也可能出现 `'null'`/`'""'` 这类占位字符串；ETL 会在 join 前做清理以保证能匹配到 `kol_rel.user_id`
- 仅取 `status=0`
- `kol_type` 映射：
  - star：2
  - magazine：3
  - brand：4

## 数据来源

```
ads_daily_post_performance
    ├── dwd_event_log
    ├── v3_decom.community_post
    └── v3_decom.kol_rel
```

## 时区说明

| 字段 | 时区 |
|------|------|
| dt | America/Toronto（多伦多时间） |
| update_time | UTC |

## 更新机制

- **调度周期**：T+1（每日更新）
- **更新方式**：DELETE + INSERT（幂等）
- **增量判断**：与 ADS 日报入口一致，由 `jobs/ads/ads_daily/ads_daily.py` 统一识别日期

## 相关文件

- **DDL**：`ddl/ads/ads_daily_post_performance.sql`
- **任务脚本**：`jobs/ads/ads_daily_post_performance/ads_daily_post_performance.py`（函数 `run_ads_daily_post_performance`）
- **入口脚本**：`jobs/ads/ads_daily/ads_daily.py`

## Superset 看板

### 看板名称

- `【AI-Fashion】内容维度日报（日×帖子）`

### 看板信息

- Dashboard ID：`94`
- 访问地址：`https://bi.alvinclub.ca/superset/dashboard/94/`
- Dataset：`decom.ads_daily_post_performance`（dataset_id=`223`，main_dttm_col=`dt`）
- 默认时间范围：`Last week`
- 原生筛选器（Native Filters）：`日期范围` / `模块` / `专栏` / `帖子ID` / `创建者`

### 图表信息

- `【AI-Fashion】内容维度日报（日×帖子）-规模趋势`（chart_id=`547`）
- `【AI-Fashion】内容维度日报（日×帖子）-转化趋势`（chart_id=`548`）
- `【AI-Fashion】内容维度日报（日×帖子）-Top帖子`（chart_id=`549`）
- `【AI-Fashion】内容维度日报（日×帖子）-明细表`（chart_id=`550`）

### 生成脚本

- `.agents/skills/superset/scripts/create_ads_daily_post_performance_dashboard.py`

### 使用方式

默认先 dry-run（只输出计划，不写入 Superset）：

```bash
python3 .agents/skills/superset/scripts/create_ads_daily_post_performance_dashboard.py
```

真正创建/更新（写入 Superset）：

```bash
python3 .agents/skills/superset/scripts/create_ads_daily_post_performance_dashboard.py --apply
```

如需指定 `database_id`（仅在 dataset 不存在且无法自动推断时）：

```bash
python3 .agents/skills/superset/scripts/create_ads_daily_post_performance_dashboard.py --apply --database-id <你的database_id>
```
