## 项目信息

- 这是一个数据工程项目，包含整个 ETL pipeline 以及最终的 Superset BI 看板展示
- 技术栈为 Python + BigQuery
- 必须要有架构换性能的思路，不能只关注功能实现

## SSOT 路由表

1. **数仓分层架构（分层职责、是否建层、层间依赖、每层校验）**参考 [docs/rules/layering.md](./docs/rules/layering.md)
2. **编写脚本和表字段变更等逻辑**参考 [docs/rules/code.md](./docs/rules/code.md)
3. **数据指标计算逻辑**参考 [docs/rules/data.md](./docs/rules/data.md)
4. **运行环境**参考 [docs/rules/environment.md](./docs/rules/environment.md)
5. **post 和 column 基础映射关系**参考 [docs/rules/post-mapping.md](./docs/rules/post-mapping.md)
6. **asset 基础映射与互动口径**参考 [docs/rules/asset-mapping.md](./docs/rules/asset-mapping.md)
7. **试穿任务映射关系**参考 [docs/rules/tryon-mapping.md](./docs/rules/tryon-mapping.md)
8. **点赞、关注等社区互动映射关系**参考 [docs/rules/interaction-mapping.md](./docs/rules/interaction-mapping.md)
9. **视频数据映射关系**参考 [docs/rules/video-mapping.md](./docs/rules/video-mapping.md)
10. **衣橱主题范围与口径**参考 [docs/rules/closet.md](./docs/rules/closet.md)
11. **push 消息映射关系**参考 [docs/rules/push.md](./docs/rules/push.md)
12. **Makepod 独立站口径**参考 [docs/rules/makepod.md](./docs/rules/makepod.md)
13. **ods_event_log 及其下游埋点事件文档**参考 [docs/rules/references.md](./docs/rules/references.md)
14. **ADS 日报编排与 pipeline 规则**参考 [docs/rules/pipeline.md](./docs/rules/pipeline.md)
15. **维表窗口增量更新规则**参考 [docs/rules/window-incremental.md](./docs/rules/window-incremental.md)
16. **主题文档结构与 `fact.md` / `patterns.md` 写法**参考 [docs/rules/topics.md](./docs/rules/topics.md)
17. **已有主题**的导航入口参考 [docs/topics/index.md](./docs/topics/index.md)
18. [jobs/](./jobs/)为 **ADS,DWS,DIM,DWD,ODS** 层表的生成逻辑
19. **ddl** 目录为 **ADS,DWS,DIM,DWD,ODS** 层表的 DDL 语句

## 行为规则

- 不在未验证前声称任务完成
- 本项目直接在 `main` 分支提交并推送，不新建 feature/fix 分支（除非用户明确要求走分支或 PR）
- 严格遵守 [docs/rules](./docs/rules) 中的规则
- **新建表或调整链路前**，先按 [docs/rules/layering.md](./docs/rules/layering.md) 判断该表属于哪一层、这一层是否真的需要存在；空转层（内容等价于 `SELECT * FROM 上一层`）不要建
- **新增字段时**，先按 [docs/rules/layering.md](./docs/rules/layering.md) 判断该字段的空值语义（不可能为空 / 空就是 0 / 业务上不适用），再同步给链路上每一层补校验或补文档说明；只改最下游会导致上游断了下游仍报告成功
- 回填数据前必须和用户确认,不能直接修改生产环境数据
- `ADS` 层默认只做报表粒度聚合和轻派生，禁止直接读取 `ODS` / `DWD` / 外部源表；新增业务口径优先下沉到 `DIM` / `DWS`
- 跨主题复用的名单、团队边界、映射关系，不要在多个脚本里各维护一份常量；优先沉淀到维表、稳定中间层或规则文档
- 禁止把真实凭据、token、refresh token、私钥、service account key 提交到仓库；具体约束以 [docs/rules/environment.md](./docs/rules/environment.md) 为准
- **新增需求**时，先判断是否属于已有主题；已有主题优先更新对应 `fact.md`，新主题再创建主题目录并补充 `docs/topics/index.md` 导航
- 修改 `fact.md` 时，默认遵循 [docs/rules/topics.md](./docs/rules/topics.md)；优先使用 `报表 / 字段 / 依赖` 结构
- 如果主题只有部分字段已落地，`fact.md` 仍需保留完整需求字段清单，并单列 `已落地字段`，不能只写当前实现部分
- **真实踩坑或可复用的排查经验**，写回对应主题的 `patterns.md`
- **跨主题的通用规则**，优先沉淀到 `docs/rules/*.md`，新增前必须先和用户确认
- 当业务和 Makepod 数据相关时，建 DDL 需要在表名中加上 makepod，例如 `ods_makepod_event_log`
