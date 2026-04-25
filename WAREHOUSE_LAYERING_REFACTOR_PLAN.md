# Warehouse Layering Refactor Plan

这份文档只做一件事：定义这次仓库重构的执行顺序。

不讲泛泛原则，只回答 3 个问题：

1. 现在先做哪一批
2. 这一批具体改哪些表和脚本
3. 这一批做到什么程度才算完成


## 1. 总目标

当前仓库的主要问题是：很多本该沉在 `DIM / DWS` 的公共语义，直接写在了 `ADS` 里。

重构完成后的目标状态只有 4 条：

- `DIM` 只承接稳定实体定义
- `DWS` 只承接主题事实和可复用聚合
- `ADS` 只承接最终报表粒度、展示字段和报表专用派生指标
- 编排关系尽量写进 `env/etl_config.json`，不再继续扩散新的“编排脚本”


## 2. 执行规则

执行时只遵守下面这些规则。

### 2.1 批次规则

- 一次只做一批，不跨批并行切口径
- 当前批次没写到的表，不顺手改
- 如果某个旧脚本里混着多个批次的逻辑，只拆当前批次相关的那一段
- 旧入口脚本的彻底删除，统一放到最后收尾批次处理


### 2.2 脚本规则

- 一个表级脚本只负责一个正式目标表
- 表级脚本自己负责本表 SQL 和本表待处理日期判断
- 不新增跨主题的公共“日期发现层”
- 不再给 `jobs/dws/dws_daily.py` 这类共享旧入口继续挂新表
- 如果一张表依赖多类上游，待处理 `dt` 必须按该表自己的上游并集单独判断，不能复用整层共享 `dt`
- `env/etl_config.json` 只承接任务元数据，不承接业务语义
- 新脚本目录必须和层级一致：
  - `jobs/dim/` 放 `DIM`
  - `jobs/dws/` 放 `DWS`
  - `jobs/ads/` 放 `ADS`


### 2.3 日志规则

`logs` 不是单独抽一批先改老代码。

正确做法是：

- 哪个批次重写了脚本，哪个批次顺手给这些脚本接日志
- 不要求在 `Batch 0` 给所有旧脚本补日志
- 不要求给当前批次完全没动过的老脚本补日志

日志目录统一为：

```text
logs/
  ods/<table_name>/<current_date>_<table_name>.log
  dwd/<table_name>/<current_date>_<table_name>.log
  dim/<table_name>/<current_date>_<table_name>.log
  dws/<table_name>/<current_date>_<table_name>.log
  ads/<table_name>/<current_date>_<table_name>.log
```


### 2.4 每批固定动作

每一批都按这个顺序推进：

1. 明确本批范围
2. 新增或修改 DDL
3. 新增或重写本批涉及的表级脚本
4. 更新 `env/etl_config.json`
5. 给本批新写或重写的脚本接日志
6. 并跑与对账
7. 切换本批涉及的 `ADS`
8. 判断是否需要回填


## 3. 批次总览

| 批次 | 主题 | 本批核心动作 | 主要切换对象 |
|---|---|---|---|
| Batch 0 | 基础规范 | 定义目标结构和过渡边界，不大动存量代码 | 无 |
| Batch 1 | 实体层 | 先抽 `column / post` 两个核心实体 | 无 |
| Batch 2 | 帖子主题 | 把帖子主题从 `ADS` 抽到 `DWS` | `ads_daily_post_performance` |
| Batch 3 | 专栏主题 | 把专栏行为和专栏帖子快照分层 | `ads_daily_column_performance` |
| Batch 4 | 平台与首页 | 把平台内容和首页模块从事件直算改成中层消费 | `ads_daily_content_performance`、`ads_daily_home_module_performance` |
| Batch 5 | 试穿与商品 | 收口轻量试穿 / 商品主题 | `ads_daily_tryon_confirm`、`ads_daily_post_tryon_confirm`、`ads_daily_tryon_add_cart_conversion`、`ads_daily_product_tryon_performance` |
| Batch 6 | 用户活动与留存 | 把设备 / 用户活动和 retention feature 抽到中层 | `ads_daily_user_duration_frequency`、`ads_daily_retention_user_feature` |
| Batch 7 | 视频与投资人 | 处理跨平台视频映射和投资人 / 社媒跨源语义 | `ads_daily_investor`、`ads_daily_social_content` |
| Batch 8 | 收尾与收口 | 清理遗留耦合和剩余越层逻辑 | `ads_daily_new`，全局收尾 |


## 4. Batch 0：基础规范

### 4.1 这批要做什么

这批不是大改代码，这批只定后面怎么改。

本批只做下面 4 件事：

- 定义 `env/etl_config.json` 目标 schema
- 定义脚本目录和层级边界
- 定义旧入口脚本的处理方式
- 定义日志接入方式


### 4.1.1 本批依赖输入

- 现有目录结构
- 现有入口脚本：
  - `jobs/ads/ads_daily.py`
  - `jobs/dws/dws_daily.py`
- 现有调度配置：`env/etl_config.json`


### 4.1.2 本批输出

- 后续批次统一遵守的目录边界
- 后续批次统一遵守的 `env/etl_config.json` schema
- 旧入口脚本的冻结规则
- 日志接入时机规则


### 4.2 这批具体改法

- 明确后续任务在配置里至少有：
  - `layer`
  - `table_name`
  - `script`
  - `enabled`
  - `depends_on`
  - `schedule_group`
- 明确旧入口脚本：
  - `jobs/ads/ads_daily.py`
  - `jobs/dws/dws_daily.py`
  先冻结，不在这一批拆空
- 冻结的含义包括：
  - 不继续往这些共享旧入口里挂新表
  - 后续新表一律按任务粒度独立推增量 `dt`
- 明确后续新建表级脚本必须放到正确目录：
  - `jobs/dim/`
  - `jobs/dws/`
  - `jobs/ads/`
- 明确后续每个批次都要把“独立任务注册方式”和“增量更新策略”写在本批方案里，不能留到实现时临时判断
- 明确日志只跟随“本批新写 / 重写脚本”一起落，不要求先补老代码日志


### 4.3 这批不做什么

- 不切任何 `ADS`
- 不新建业务表
- 不批量重构现有老脚本
- 不把现有所有任务一次性迁进 `env/etl_config.json`
- 不为了验证结构先给老版本脚本补日志


### 4.4 这批完成标准

- 后续每一批知道自己该把脚本放哪里
- 后续每一批知道该怎么更新 `env/etl_config.json`
- 后续每一批知道什么时候给脚本接日志
- 团队对 `ads_daily.py` / `dws_daily.py` 的处理方式没有歧义


## 5. Batch 1：实体层

### 5.1 本批目标

先把内容主题里重复最多、复用最广的两个实体抽出来：

- `dim_column`
- `dim_post`


### 5.1.1 本批依赖输入

- `v3_decom.kol_rel`
- `v3_decom.community_post`
- 规则文档：`docs/rules/post-mapping.md`
- 当前下游消费方：
  - `ads_daily_post_performance`
  - `ads_daily_column_performance`
  - `ads_daily_post_tryon_confirm`
  - `ads_daily_content_performance`


### 5.1.2 本批新增输出

- `dim_column`
- `dim_post`


### 5.2 本批为什么先做

现在这些表都在重复解释 `kol_rel / community_post`：

- `ads_daily_post_performance`
- `ads_daily_column_performance`
- `ads_daily_post_tryon_confirm`
- `ads_daily_content_performance`

如果不先统一实体，后面每一批都会继续重复映射。


### 5.3 本批具体改法

- 新增 `dim_column` DDL
- 新增 `dim_post` DDL
- 在 `jobs/dim/` 下新增两个表级脚本
- 在 `env/etl_config.json` 中注册两个 `DIM` 任务
- 两个 `DIM` 都按表级任务独立执行，不挂共享旧入口
- 两个 `DIM` 都要显式写清自己的增量更新策略
- 脚本写完后，顺手给这两个 `DIM` 脚本接日志


### 5.3.1 `dim_column` 首版定义

首版只做内容主题后续批次真正需要的稳定字段，不求一步到位。

建议主键：

- `column_id`

建议首版字段：

- `column_id`
- `module`
- `column_name`
- `kol_type`
- `status`
- `update_time`

来源建议：

- 主来源：`v3_decom.kol_rel`
- `module` 先按现有口径映射：
  - `kol_type = 2 -> star`
  - `kol_type = 3 -> magazine`
  - `kol_type = 4 -> brand`

首版要求：

- 先统一 `column_id -> module -> column_name`
- 不在这批顺手补太多和内容主题无关的字段


### 5.3.2 `dim_post` 首版定义

建议主键：

- `post_code`

建议首版字段：

- `post_code`
- `post_id`
- `post_name`
- `creator`
- `column_id`
- `gmt_create`
- `status`
- `update_time`

来源建议：

- 主来源：`v3_decom.community_post`
- `column_id` 先直接用 `poster_user_id`
- 这批不在 `dim_post` 内部顺手做太多行为派生

首版要求：

- 先统一 `post_code -> post_id / post_name / creator / column_id`
- 保证后续帖子主题和试穿主题不再各自重复映射 `community_post`


### 5.3.3 本批落地顺序

1. 先写 `dim_column` DDL 和脚本
2. 再写 `dim_post` DDL 和脚本
3. 把两个任务注册进 `env/etl_config.json`
4. 跑维表并对抽样映射结果
5. 不切任何 `ADS`


### 5.4 本批先不动什么

- 不切 `ads_daily_post_performance`
- 不切 `ads_daily_column_performance`
- 不改其他 `ADS` SQL


### 5.5 本批完成标准

- 仓库里已有统一 `dim_column`
- 仓库里已有统一 `dim_post`
- 后续内容主题脚本默认依赖这两个维表


## 6. Batch 2：帖子主题

### 6.1 本批目标

把帖子主题从 `ADS` 中抽出来，形成统一帖子事实层：

- `dws_post_interaction_daily`


### 6.1.1 本批依赖输入

- `dim_column`
- `dim_post`
- `dwd_event_log`
- `v3_decom.community_post_like`
- `v3_decom.community_star_follow`
- `v3_decom.common_ext_ai_task`
- 当前消费方：`ads_daily_post_performance`


### 6.1.2 本批新增输出

- `dws_post_interaction_daily`


### 6.1.3 本批切换输出

- 重写 `ads_daily_post_performance`


### 6.2 本批承接什么语义

`dws_post_interaction_daily` 统一承接：

- 帖子曝光
- 帖子阅读
- 帖子点赞
- 帖子关注归因
- 帖子试穿

建议粒度：

- `dt × post_code × module × column_id × platform`
- `platform` 除保留具体平台外，还要额外产出 `all` 总计口径，供帖子级 ADS 消费


### 6.2.1 `dws_post_interaction_daily` 首版字段

首版先只承接 `ads_daily_post_performance` 真正在用的帖子主题核心事实。

建议首版字段：

- `dt`
- `post_code`
- `module`
- `column_id`
- `platform`
- `post_exposure_uv`
- `read_uv`
- `like_total_count`
- `like_uv`
- `follow_total_count`
- `follow_uv`
- `tryon_total_count`
- `update_time`

先不在这批加入过多展示层字段：

- `post_name`
- `creator`
- `column_name`

这些字段保留在 `ADS` 通过 `dim_post / dim_column` 补齐。


### 6.3 本批具体改法

- 新增 `dws_post_interaction_daily` DDL
- 在 `jobs/dws/` 下新增 `dws_post_interaction_daily` 脚本
- 在 `env/etl_config.json` 中注册 `dws_post_interaction_daily` 独立任务
- 重写 `ads_daily_post_performance`
- 让 `ads_daily_post_performance` 只消费：
  - `dim_post`
  - `dim_column`
  - `dws_post_interaction_daily`
- 更新 `env/etl_config.json`
- 给这次新增 / 重写的脚本接日志
- `dws_post_interaction_daily` 不挂 `jobs/dws/dws_daily.py`，而是按任务粒度独立推自己的待处理 `dt`
- `ads_daily_post_performance` 也要注册为独立 `ADS` 任务，不再继续挂在 `jobs/ads/ads_daily.py`
- 这批要显式写清 `dws_post_interaction_daily` 的增量更新方式；如果采用固定重刷窗口，也要写清窗口边界
- 这批也要显式写清 `ads_daily_post_performance` 的增量更新方式，不能复用 `dws_device_daily` 共享日期


### 6.3.1 本批迁移顺序

1. 先从现有 `ads_daily_post_performance` SQL 中抽出帖子主题核心事实
2. 把“曝光 / 阅读 / 点赞 / 关注归因 / 试穿”沉到 `dws_post_interaction_daily`
3. 为 `dws_post_interaction_daily` 单独定义自己的增量 `dt` 推导逻辑，不复用整层共享 `dt`
4. 为避免跨平台 UV 直接相加失真，在 `dws_post_interaction_daily` 内从明细额外重算 `platform = 'all'`
5. 保留 `post_name / creator / column_name` 在 `ADS` 通过维表补齐
6. 重写 `ads_daily_post_performance`，只做：
   - 维表 join
   - 最终粒度整理
   - 率指标计算
7. 把 `ads_daily_post_performance` 注册进 `env/etl_config.json`，并从 `jobs/ads/ads_daily.py` 共享入口摘出
8. 并跑新旧 `ads_daily_post_performance` 对账后再切


### 6.3.2 本批对账重点

- `dt × post_code × module × column_id × platform` 粒度行数
- `platform = 'all'` 是否由明细重算，而不是平台 UV 直接相加
- `post_exposure_uv`
- `read_uv`
- `like_total_count`
- `follow_total_count`
- `tryon_total_count`
- `like_rate`
- `read_rate`
- `follow_rate`


### 6.4 本批先不动什么

- 不切 `ads_daily_column_performance`
- 不切 `ads_daily_content_performance`


### 6.5 本批完成标准

- `ads_daily_post_performance` 不再自己承担完整帖子主题建模
- 帖子主题核心事实已经沉到 `DWS`


## 7. Batch 3：专栏主题

### 7.1 本批目标

把专栏行为主题和专栏帖子快照分开：

- 新增 `dws_column_interaction_daily`
- 保留 `dws_column_post_daily`，但明确它只做快照


### 7.1.1 本批依赖输入

- `dim_column`
- `dim_post`
- `dws_column_post_daily`
- `dwd_event_log`
- `v3_decom.community_post_like`
- `v3_decom.community_star_follow`
- 当前消费方：`ads_daily_column_performance`


### 7.1.2 本批新增输出

- `dws_column_interaction_daily`


### 7.1.3 本批切换输出

- 重写 `ads_daily_column_performance`


### 7.2 本批承接什么语义

`dws_column_interaction_daily` 负责：

- 专栏曝光
- 商品点击 / 曝光归因
- 去重阅读帖子数
- 试穿帖子数
- 点赞帖子数
- 新增关注
- 7 日关注转化

`dws_column_post_daily` 只负责：

- 在线帖子快照


### 7.2.1 `dws_column_interaction_daily` 首版字段

首版先承接 `ads_daily_column_performance` 真正在用的行为事实，不把快照事实混进来。

建议首版字段：

- `dt`
- `module`
- `column_id`
- `column_exposure_uv`
- `follow_total_count`
- `follow_uv`
- `product_click_uv`
- `product_click_pv`
- `product_card_exposure_uv`
- `read_post_count`
- `tryon_post_count`
- `tryon_click_uv`
- `like_post_count`
- `like_uv`
- `new_follow_count`
- `follow_like_uv_7d`
- `like_uv_7d`
- `dau_uv`
- `update_time`

保留在 `ADS` 计算的内容：

- `product_exposure_click_rate`
- `product_click_penetration_rate`
- `avg_read_post_count_per_user`
- `tryon_uv_rate`
- `ai_tryon_penetration_rate`
- `like_rate`
- `follow_conversion_rate_7d`


### 7.3 本批具体改法

- 新增 `dws_column_interaction_daily` DDL
- 在 `jobs/dws/` 下新增 `dws_column_interaction_daily` 脚本
- 在 `env/etl_config.json` 中注册独立表级任务，不挂 `jobs/dws/dws_daily.py`
- 重写 `ads_daily_column_performance`
- 让 `ads_daily_column_performance` 只消费行为事实和快照事实
- 更新 `env/etl_config.json`
- 给本批新增 / 重写的脚本接日志
- 明确 `dws_column_interaction_daily` 和 `dws_column_post_daily` 各自的增量 `dt` 或重刷窗口


### 7.3.1 本批迁移顺序

1. 先把现有 `ads_daily_column_performance` 里的行为事实拆出来
2. 明确 `dws_column_post_daily` 继续只保留在线帖子快照
3. 新建 `dws_column_interaction_daily`
4. 重写 `ads_daily_column_performance`，只做：
   - 行为事实 join
   - 快照 join
   - 终层率指标计算
5. 并跑新旧 `ads_daily_column_performance` 再切


### 7.3.2 本批对账重点

- `dt × module × column_id` 粒度行数
- `column_exposure_uv`
- `follow_total_count`
- `product_click_uv`
- `product_click_pv`
- `read_post_count`
- `post_total_count`
- `tryon_post_count`
- `like_post_count`
- `new_follow_count`
- `follow_rate`
- `like_rate`
- `follow_conversion_rate_7d`


### 7.4 本批先不动什么

- 不继续在 `ads_daily_column_performance` 里堆复杂 SQL
- 不把 `post_total_count` 和行为事实继续混在同一层


### 7.5 本批完成标准

- `ads_daily_column_performance` 只做终层聚合和派生率
- 专栏行为事实和帖子快照事实已经分层


## 8. Batch 4：平台内容与首页模块

### 8.1 本批目标

把平台级内容主题和首页模块主题从事件直算切到统一中层：

- `dws_platform_content_daily`
- `dws_home_module_daily`


### 8.1.1 本批依赖输入

- `dwd_event_log`
- 服务端行为表：
  - `v3_decom.community_post_like`
  - `v3_decom.community_star_follow`
  - `v3_decom.common_ext_ai_task`
- 当前消费方：
  - `ads_daily_content_performance`
  - `ads_daily_home_module_performance`


### 8.1.2 本批新增输出

- `dws_platform_content_daily`
- `dws_home_module_daily`


### 8.1.3 本批切换输出

- 重写 `ads_daily_content_performance`
- 重写 `ads_daily_home_module_performance`


### 8.2 本批具体改法

- 新增两张 `DWS` DDL
- 在 `jobs/dws/` 下新增两张 `DWS` 脚本
- 两张 `DWS` 都在 `env/etl_config.json` 注册独立任务，不复用共享 `DWS` 入口
- 重写：
  - `ads_daily_content_performance`
  - `ads_daily_home_module_performance`
- 更新 `env/etl_config.json`
- 给本批新增 / 重写的脚本接日志
- 显式写清 `dws_platform_content_daily`、`dws_home_module_daily` 各自的增量 `dt` 推导或固定重刷窗口


### 8.2.1 `dws_platform_content_daily` 首版字段

建议粒度：

- `dt × platform`

建议首版字段：

- `dt`
- `platform`
- `platform_exposure_uv`
- `content_exposure_uv`
- `content_exposure_pv`
- `product_detail_exposure_uv`
- `product_detail_exposure_pv`
- `product_click_uv`
- `product_click_pv`
- `read_uv`
- `like_total_count`
- `like_uv`
- `follow_total_count`
- `follow_column_count`
- `follow_uv`
- `dau_uv`
- `tryon_total_count`
- `tryon_uv`
- `column_exposure_pv`
- `update_time`

保留在 `ADS` 计算的内容：

- `avg_browse_content_count_per_user`
- `like_rate`
- `product_click_rate`
- `follow_rate`
- `follow_conversion_rate`
- `read_follow_rate`
- `tryon_penetration_rate`


### 8.2.2 `dws_home_module_daily` 首版字段

建议粒度：

- `dt × module`

建议首版字段：

- `dt`
- `module`
- `module_exposure_uv`
- `module_click_uv`
- `click_pv`
- `update_time`

保留在 `ADS` 计算的内容：

- `click_rate`
- `avg_click_count_per_user`


### 8.2.3 本批迁移顺序

1. 先抽 `ads_daily_content_performance` 的平台级事实
2. 再抽 `ads_daily_home_module_performance` 的首页模块事实
3. 保证两张 `ADS` 只做终层展示指标
4. 并跑两张 `ADS` 再切


### 8.2.4 本批对账重点

- `ads_daily_content_performance`
  - `dt × platform` 粒度行数
  - `platform_exposure_uv`
  - `content_exposure_uv`
  - `product_detail_exposure_uv`
  - `product_click_uv`
  - `like_total_count`
  - `follow_uv`
  - `tryon_uv`
- `ads_daily_home_module_performance`
  - `dt × module` 粒度行数
  - `module_exposure_uv`
  - `module_click_uv`
  - `click_pv`
  - `click_rate`


### 8.3 本批先不动什么

- 不处理帖子级和专栏级实体映射
- 不处理留存和用户活动


### 8.4 本批完成标准

- `ads_daily_content_performance` 不再直接拼平台主题原始事实
- `ads_daily_home_module_performance` 不再自己维护首页事件映射


## 9. Batch 5：试穿与商品主题

### 9.1 本批目标

把分散在轻量 `ADS` 中的试穿和商品主题统一收口：

- `dws_tryon_confirm_daily`
- `dws_tryon_confirm_post_daily`
- `dws_tryon_user_daily`
- `dws_product_behavior_daily`


### 9.1.1 本批依赖输入

- `dim_post`
- `dwd_event_log`
- `v3_decom.common_ext_ai_task`
- 当前消费方：
  - `ads_daily_tryon_confirm`
  - `ads_daily_post_tryon_confirm`
  - `ads_daily_tryon_add_cart_conversion`
  - `ads_daily_product_tryon_performance`


### 9.1.2 本批新增输出

- `dws_tryon_confirm_daily`
- `dws_tryon_confirm_post_daily`
- `dws_tryon_user_daily`
- `dws_product_behavior_daily`


### 9.1.3 本批切换输出

- 重写 `ads_daily_tryon_confirm`
- 重写 `ads_daily_post_tryon_confirm`
- 重写 `ads_daily_tryon_add_cart_conversion`
- 重写 `ads_daily_product_tryon_performance`


### 9.2 本批具体改法

- 新增四张 `DWS` DDL
- 在 `jobs/dws/` 下新增四张 `DWS` 脚本
- 四张 `DWS` 都在 `env/etl_config.json` 注册独立任务，不挂共享旧入口
- 重写：
  - `ads_daily_tryon_confirm`
  - `ads_daily_post_tryon_confirm`
  - `ads_daily_tryon_add_cart_conversion`
  - `ads_daily_product_tryon_performance`
- 更新 `env/etl_config.json`
- 给本批新增 / 重写的脚本接日志
- 四张 `DWS` 都要单独写清增量更新策略，不能用一句“按 DWS 日期统一处理”带过


### 9.2.1 四张 `DWS` 的分工

- `dws_tryon_confirm_daily`
  - 粒度：`dt`
  - 承接全量试穿点击使用事实
- `dws_tryon_confirm_post_daily`
  - 粒度：`dt × post_code`
  - 承接帖子级试穿点击使用事实
- `dws_tryon_user_daily`
  - 粒度：`dt × user_id` 或等价用户粒度
  - 承接试穿用户行为事实，服务转化类指标
- `dws_product_behavior_daily`
  - 粒度：`dt × spu × sku`
  - 承接商品曝光、试穿、加购等商品主题事实


### 9.2.2 本批迁移顺序

1. 先抽 `ads_daily_tryon_confirm`
2. 再抽 `ads_daily_post_tryon_confirm`
3. 再抽商品行为和试穿用户行为
4. 最后重写 `ads_daily_tryon_add_cart_conversion`、`ads_daily_product_tryon_performance`
5. 四张 `ADS` 并跑后统一切


### 9.2.3 本批对账重点

- `ads_daily_tryon_confirm`
  - `click_use_pv`
  - `click_use_uv`
- `ads_daily_post_tryon_confirm`
  - `dt × post_id` 行数
  - `click_use_pv`
  - `click_use_uv`
- `ads_daily_tryon_add_cart_conversion`
  - `tryon_uv`
  - `add_cart_uv`
  - `add_cart_rate`
- `ads_daily_product_tryon_performance`
  - `dt × spu × sku` 行数
  - `exposure_uv`
  - `tryon_total_count`
  - `add_cart_uv`


### 9.3 本批先不动什么

- 不扩新的商品主题报表
- 不继续在轻量 `ADS` 里新增事件清洗逻辑


### 9.4 本批完成标准

- 这四张 `ADS` 都不再直接从事件明细拼主题事实
- 试穿和商品主题已经有中层承接


## 10. Batch 6：用户活动与留存主题

### 10.1 本批目标

把设备 / 用户活动和 retention feature 的基础语义从 `ADS` 抽出来：

- `dws_device_activity_daily`
- `dws_user_activity_daily`
- `dws_user_retention_feature_daily`


### 10.1.1 本批依赖输入

- `dwd_event_log`
- 现有活动事实：
  - `dws_device_daily`
  - `dws_user_daily`
- 当前消费方：
  - `ads_daily_user_duration_frequency`
  - `ads_daily_retention_user_feature`


### 10.1.2 本批新增输出

- `dws_device_activity_daily`
- `dws_user_activity_daily`
- `dws_user_retention_feature_daily`


### 10.1.3 本批切换输出

- 重写 `ads_daily_user_duration_frequency`
- 重写 `ads_daily_retention_user_feature`


### 10.2 本批具体改法

- 新增三张 `DWS` DDL
- 在 `jobs/dws/` 下新增三张 `DWS` 脚本
- 三张 `DWS` 都在 `env/etl_config.json` 注册独立任务，不挂共享旧入口
- 重写：
  - `ads_daily_user_duration_frequency`
  - `ads_daily_retention_user_feature`
- 更新 `env/etl_config.json`
- 给本批新增 / 重写的脚本接日志
- 三张 `DWS` 都要单独写清增量 `dt` 推导、窗口边界和历史回刷边界


### 10.2.1 三张 `DWS` 的分工

- `dws_device_activity_daily`
  - 粒度：`dt × user_segment`
  - 承接设备 / 游客 / 已登录用户分群下的日活、启动次数、停留时长、留存分子分母、月活
- `dws_user_activity_daily`
  - 粒度：`dt × user_id`
  - 承接已登录用户 cohort 与活跃事实
- `dws_user_retention_feature_daily`
  - 粒度：`dt × user_group × feature_name`
  - 承接 retention feature 的用户数、特征值、时长分子


### 10.2.2 本批迁移顺序

1. 先抽 `ads_daily_user_duration_frequency` 的活动事实
2. 再抽 `ads_daily_retention_user_feature` 的 cohort 与 feature 事实
3. 保证两张 `ADS` 只做展示层聚合和排序
4. 两张 `ADS` 并跑后再切


### 10.2.3 本批对账重点

- `ads_daily_user_duration_frequency`
  - `dt × user_segment` 行数
  - `dau_uv`
  - `total_duration_min`
  - `app_launch_count`
  - `next_day_retention_rate`
  - `day_7_retention_rate`
  - `day_30_retention_rate`
  - `mau_uv`
- `ads_daily_retention_user_feature`
  - `dt × user_group × feature_name` 行数
  - 四类 retained / not retained user_count
  - 各 feature retained_value / not_retained_value
  - duration_sum 字段


### 10.3 本批先不动什么

- 不提前碰投资人报表
- 不把视频 / 社媒语义混进来


### 10.4 本批完成标准

- `ads_daily_user_duration_frequency` 不再自己解释三类主体活动事实
- `ads_daily_retention_user_feature` 不再同时承担 cohort 构造和 feature 识别


## 11. Batch 7：视频与投资人主题

### 11.1 本批目标

统一视频自动发布跨平台任务映射，并拆开投资人和社媒主题里的跨源公共语义：

- `dim_auto_publish_video_task`
- `dws_registered_app_device_daily`
- `dws_registered_app_user_daily`
- `dws_social_channel_daily`


### 11.1.1 本批依赖输入

- 视频侧源表和映射规则：`docs/rules/video-mapping.md`
- `videos.*`
- `dws_video_daily`
- `dws_appsflyer_download_daily`
- `dws_device_daily`
- `dws_user_daily`
- 当前消费方：
  - `ads_daily_investor`
  - `ads_daily_social_content`


### 11.1.2 本批新增输出

- `dim_auto_publish_video_task`
- `dws_registered_app_device_daily`
- `dws_registered_app_user_daily`
- `dws_social_channel_daily`


### 11.1.3 本批切换输出

- 重写 `ads_daily_investor`
- 重写 `ads_daily_social_content`


### 11.2 为什么这一批把两张 ADS 放一起

- `ads_daily_investor` 和 `ads_daily_social_content` 都会依赖自动发布跨平台映射
- 如果拆开做，很容易两边各写一套去重和映射逻辑


### 11.3 本批具体改法

- 新增一张 `DIM` DDL
- 新增三张 `DWS` DDL
- 在 `jobs/dim/` 和 `jobs/dws/` 下新增对应脚本
- 新增的 `DIM / DWS` 都在 `env/etl_config.json` 注册独立任务，不再继续扩共享旧入口
- 重写：
  - `ads_daily_investor`
  - `ads_daily_social_content`
- 更新 `env/etl_config.json`
- 给本批新增 / 重写的脚本接日志
- 对四张新表分别写清自己的增量更新方式，尤其跨源映射表和渠道事实不能共用一套 `dt`


### 11.3.1 四张表的分工

- `dim_auto_publish_video_task`
  - 统一自动发布跨平台 `task_id -> 统一视频任务`
- `dws_registered_app_device_daily`
  - 承接 app 已注册设备活动事实
- `dws_registered_app_user_daily`
  - 承接 app 已注册用户活动与留存事实
- `dws_social_channel_daily`
  - 承接社媒渠道级视频日事实


### 11.3.2 本批迁移顺序

1. 先做 `dim_auto_publish_video_task`
2. 再把投资人报表里 app 已注册设备 / 用户事实抽到 `DWS`
3. 再把社媒渠道视频事实抽到 `DWS`
4. 最后重写：
   - `ads_daily_investor`
   - `ads_daily_social_content`
5. 两张 `ADS` 并跑后再切


### 11.3.3 本批对账重点

- `ads_daily_investor`
  - `new_video_count`
  - `active_video_count`
  - `new_video_view_count`
  - `new_download_count`
  - `new_device_count`
  - `active_device_count`
  - `active_registered_user_count`
  - `avg_duration_sec`
  - `content_ctr`
  - `next_day_retention_rate`
  - `day_7_retention_rate`
- `ads_daily_social_content`
  - `dt × 展示渠道` 行数
  - 各渠道新增视频数
  - 活跃视频数
  - 播放增量
  - 点赞增量


### 11.4 本批先不动什么

- 不继续让 `ads_daily_investor` 直扫 app 已注册用户明细
- 不继续让 `ads_daily_social_content` 自己维护自动发布主逻辑


### 11.5 本批完成标准

- 自动发布任务映射只有一个 SSOT
- 投资人和社媒主题里共用的跨源语义已经收口


## 12. Batch 8：收尾与收口

### 12.1 本批目标

清理剩余结构尾巴，去掉 `ADS` 之间互相充当公共库的现象。


### 12.1.1 本批依赖输入

- 前面各批已落地的 `DIM / DWS / ADS`
- 仍未清掉的遗留入口：
  - `jobs/ads/ads_daily.py`
  - `jobs/dws/dws_daily.py`
- 当前仍越层的表：`ads_daily_new`


### 12.1.2 本批输出

- 收口后的 `ads_daily_new`
- 清理后的 `jobs/ads`
- 退出主链路的旧入口脚本


### 12.2 本批具体改法

- 扩 `dws_device_daily` 所需补充字段
- 收口 `ads_daily_new`
- 清理 `jobs/ads` 内部对其他 `ads_*.py` 的函数引用
- 清理 `jobs/ads/ads_daily.py`、`jobs/dws/dws_daily.py` 中残留的编排式逻辑
- 删除已经没有必要保留的遗留入口代码
- 更新 `env/etl_config.json`
- 给这一批重写过的脚本补齐日志
- 把仍留在旧入口里的存量表逐个补成独立任务，并补齐各自的增量 `dt` 策略后，再删除旧入口


### 12.2.1 `ads_daily_new` 收口目标

这批不是重写“新增设备 / 新增用户”口径，而是清掉剩余越层逻辑。

目标是：

- `ads_daily_new` 只消费稳定中层
- 不再自己直扫明细事件做主题建模
- 不再继续依赖 `jobs/ads` 里的其他脚本函数


### 12.2.2 本批迁移顺序

1. 先补 `dws_device_daily` 还缺的稳定字段
2. 再收口 `ads_daily_new`
3. 再清理 `jobs/ads` 内部互相 import
4. 最后下掉旧入口脚本主链路角色


### 12.2.3 本批对账重点

- `ads_daily_new`
  - `dt × platform` 行数
  - `device_count`
  - `user_count`
  - `new_user_login_rate`
  - `anonymous_device_rate`
  - `anonymous_old_device_rate`
  - `avg_duration_sec`
  - `avg_content_consume_count`
  - `next_day_retention_rate`
  - `new_download_count`


### 12.3 本批完成标准

- `ads_daily_new` 不再直读明细事件做主题建模
- `jobs/ads` 不再像公共函数目录
- 遗留入口脚本退出主链路
- `ADS` 层整体只保留终层逻辑


## 13. ADS 与批次映射

| ADS 表 | 所属批次 |
|---|---|
| `ads_daily_total` | 不作为独立批次改造，只做健康检查 |
| `ads_daily_new` | Batch 8 |
| `ads_daily_content_performance` | Batch 4 |
| `ads_daily_home_module_performance` | Batch 4 |
| `ads_daily_tryon_confirm` | Batch 5 |
| `ads_daily_tryon_add_cart_conversion` | Batch 5 |
| `ads_daily_post_tryon_confirm` | Batch 5 |
| `ads_daily_post_performance` | Batch 2 |
| `ads_daily_column_performance` | Batch 3 |
| `ads_daily_product_tryon_performance` | Batch 5 |
| `ads_daily_user_duration_frequency` | Batch 6 |
| `ads_daily_investor` | Batch 7 |
| `ads_daily_retention_user_feature` | Batch 6 |
| `ads_daily_social_content` | Batch 7 |


## 14. 回填规则

回填不在本文件里默认批准，但每一批都要单独判断。

统一规则：

- 新增字段且 dashboard 已在用：必须评估回填
- 改粒度：必须评估回填
- 改去重口径或归因口径：默认建议回填
- 只改代码结构、不改结果口径：可以不回填

回填前必须完成：

1. 新旧同粒度对账
2. 回填范围确认
3. 用户确认
