# 主题文档规则

## 目标

`docs/topics/*` 用来承载**业务主题级**知识，而不是通用规则，也不是统一编排说明。

## 目录结构

每个主题目录包含两个文件：

- `fact.md`
  - 主题的既定事实文档，必须存在
- `patterns.md`
  - 踩坑记录与排查经验，在真实踩坑后沉淀

## 文件职责

### `fact.md`

承载表级规格，建议包含：

- 报表信息（表名、粒度、Dashboard 链接）
- 字段口径表格
- 依赖（上游表、脚本位置、DDL）

`fact.md` 不应承载：

- 大段排查经验（写入 `patterns.md`）
- 统一编排说明（在 `docs/references/*`）
- 跨主题规则（在 `docs/rules/*`）
- Filters 列表、更新机制、使用示例等（agent 读脚本可推断）

### `patterns.md`

用于沉淀真实出现过的问题与排查经验，适合放：

- 已发生的问题与根因
- 修复方式
- 排查顺序

如果某条经验已经跨多个主题重复出现，应从 `topics` 晋升到 `docs/rules/*`。

## 不该放在 `topics` 的内容

- 统一编排说明 → `docs/references/*`
- 跨主题复用规则 → `docs/rules/*`
- 运行环境约定 → `docs/rules/environment.md`
- 全局映射规则 → `docs/rules/mapping.md`

## 写作原则

- `fact.md` 尽量精简，只放字段口径和依赖关系
- 同一个信息只保留一个主落点，其他地方只做引用
- 多表主题可以在 `fact.md` 内用二级标题分开
