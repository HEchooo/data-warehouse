# Handoff

## 1. Project Overview
- **Project**: `/Users/sinn/Documents/guyin/data-warehouse`
- **Tech Stack**: Python, BigQuery, Superset REST API
- **Brief Description**: 数据工程仓库，维护 ETL、主题 ADS 表与 Superset BI 看板

## 2. Current Session Goals
- 原始目标：在 `【Social Content】社媒表现数据看板v2 copy` 中新增“发布后3天全量 Views 分桶贡献占比”图
- 衍生目标：修正 `T+3` 分桶图的 bucket 展示顺序问题，确保 `0 -> (0,100] -> ... -> (10000,+∞)` 的顺序稳定
- 衍生目标：把这次 Superset 分桶排序与校验经验补进 `superset` skill
- 用户当前要求：提交代码，并重建 `HANDOFF.md`

## 3. What Was Done
- Superset 侧新增 chart：
  - dashboard：`【Social Content】社媒表现数据看板v2 copy`（id=`119`）
  - 新 chart：`Social Content v2（日表）-发布后3天全量 Views 分桶贡献占比`（id=`666`）
- 复用并扩展虚拟 dataset：
  - dataset：`virtual_social_content_v2_published_day3_view_bucket_sorted`（id=`266`）
  - 在原有 `video_count` 基础上补充 `view_count`，表示 `dt + channel + bucket` 下视频在 `T+3` 观察日的累计播放总和
- 调整 `T+3` 分桶图查询配置：
  - `665`：`Social Content v2（日表）-发布后3天全量 Views 分桶视频占比`
  - `666`：`Social Content v2（日表）-发布后3天全量 Views 分桶贡献占比`
  - 两张图都改为以当前 chart `query_context` 直接查询，不再依赖原先不稳定的 `post_processing(rename/pivot)` 方案
- 修正 bucket 顺序策略：
  - 不再使用可见的 `01|bucket` 文本前缀
  - 也不再使用零宽空格前缀
  - 改为“不可见控制字符前缀 + 原展示标签”方案，保持前端排序稳定，同时尽量不污染 tooltip / legend 展示
- 更新主题文档 [docs/topics/【Social Content】社媒表现数据看板/fact.md](/Users/sinn/Documents/guyin/data-warehouse/docs/topics/【Social%20Content】社媒表现数据看板/fact.md)
  - 补充 `view_count` 口径
  - 明确 `视频占比` 与 `Views 贡献占比` 的计算差异
  - 更新 v2 Superset chart 资产列表，加入 `666`
- 更新 skill 文档 [superset skill](/Users/sinn/Documents/guyin/data-warehouse/.agents/skills/superset/SKILL.md)
  - 增补“固定分桶排序不要依赖 rename/pivot 隐前缀”的规则
  - 增补“优先用 `POST /api/v1/chart/data` 校验 query_context”的规则

## 4. Key Decisions & Rationale
- 新图继续复用 dataset `266`，不新建第二份 dataset
  - 原因：分桶逻辑完全一致，只是 metric 从 `SUM(video_count)` 换成 `SUM(view_count)`，复用更稳，也减少 dashboard filter target 维护成本
- 没有把这次需求下沉到新的物理 ADS 表
  - 原因：当前需求只涉及 Superset 展示口径扩展；dataset `266` 已能承载，且项目规则要求生产回填前先确认
- 放弃 `01|bucket` + `rename` 的老做法
  - 原因：当前实例里 `post_processing` 会出现 dataframe 字段不匹配，且 tooltip / legend 顺序容易漂移
- 验证方式以 `POST /api/v1/chart/data` 为准，而不是 `GET /api/v1/chart/{id}/data/`
  - 原因：本实例中后者可能在 chart 实际可用时仍返回 dataframe / post-processing 错误，不能作为唯一验收标准

## 5. Current State
- **Working / Stable**
  - dashboard `119` 已挂载 chart `665` 与 `666`
  - dataset `266` 已输出 `video_count` 与 `view_count`
  - `665` / `666` 的 `POST /api/v1/chart/data` 都返回 `200`
  - 主题文档已同步新增图表与口径
  - `superset` skill 已同步此次踩坑经验
- **In Progress**
  - 无功能性未完成项
- **Broken / Failing**
  - 无代码级阻塞
  - `GET /api/v1/chart/{pk}/data/` 在当前 Superset 实例上对这类图不可靠，属于实例行为限制，不代表图表不可用

## 6. Unfinished Work & Next Steps
1. 按用户要求完成本次 `git commit`
2. 如果用户后续仍反馈 tooltip / legend 里存在不可见前缀泄漏或排序偶发漂移，下一步应改成“排序键列 + 展示标签列”双字段方案，不再复用单字段排序
3. 如果后续要把 `view_count` 从虚拟 dataset 收口到物理 ADS 表，需要先和用户确认是否允许生产回填

## 7. Blockers & Open Questions
- 当前没有阻塞
- 唯一保留风险：前端是否会把控制字符在某些视图中原样显示，尚未做浏览器截图级全量验收；目前 API 级验证通过，且用户已在页面上人工看到旧问题并提出修正

## 8. Important Context & Gotchas
- `.agents/` 默认被 `.gitignore` 忽略；如果要提交 skill 变更，需要 `git add -f .agents/skills/superset/SKILL.md`
- 当前仓库工作区用户可见变更应包括：
  - [docs/topics/【Social Content】社媒表现数据看板/fact.md](/Users/sinn/Documents/guyin/data-warehouse/docs/topics/【Social%20Content】社媒表现数据看板/fact.md)
  - [HANDOFF.md](/Users/sinn/Documents/guyin/data-warehouse/HANDOFF.md)
  - [superset skill](/Users/sinn/Documents/guyin/data-warehouse/.agents/skills/superset/SKILL.md)
- 新图 Superset 资产：
  - dashboard `119`
  - dataset `266`
  - chart `665`：视频占比
  - chart `666`：Views 贡献占比
- 当前 `665` / `666` 两张图都使用同一个 bucket 展示字段；如果以后要彻底消除前缀兼容性风险，优先拆成排序列与展示列

## 9. Environment & Configuration Notes
- 当前分支：`main`
- Superset 凭据读取自 `env/superset.json`
- 当前涉及的线上 Superset 对象：
  - dataset `266`
  - chart `665`
  - chart `666`
  - dashboard `119`

## 10. Key Files & Locations
- 主题文档：
  - [docs/topics/【Social Content】社媒表现数据看板/fact.md](/Users/sinn/Documents/guyin/data-warehouse/docs/topics/【Social%20Content】社媒表现数据看板/fact.md)
- skill 文档：
  - [.agents/skills/superset/SKILL.md](/Users/sinn/Documents/guyin/data-warehouse/.agents/skills/superset/SKILL.md)
- 会话交接：
  - [HANDOFF.md](/Users/sinn/Documents/guyin/data-warehouse/HANDOFF.md)

## 11. Quick-Start Summary
本次会话在 `【Social Content】社媒表现数据看板v2 copy`（dashboard `119`）中新增了 `发布后3天全量 Views 分桶贡献占比` 图（chart `666`），逻辑复用原 `T+3` 分桶图，只把 metric 从 `SUM(video_count)` 切为 `SUM(view_count)`，其中 `view_count` 表示 `dt + channel + bucket` 下视频在 `T+3` 观察日的累计播放总和。为支持该图，虚拟 dataset `266` 补充了 `view_count` 输出；同时把 `665` 和 `666` 都改成直接用当前 `query_context` 查询，不再依赖不稳定的 `rename/pivot` 后处理。随后又修了用户在页面里发现的 bucket 排序问题：当前策略使用不可见控制字符前缀保证顺序为 `0 -> (0,100] -> (100,500] -> (500,1000] -> (1000,3000] -> (3000,10000] -> (10000,+∞)`。最后把这次经验沉淀进了 `.agents/skills/superset/SKILL.md`，明确要求类似分桶图优先使用 dataset 排序字段，并用 `POST /api/v1/chart/data` 做真实查询校验。当前没有功能阻塞，剩余动作就是按用户要求把这些文件提交入库。
