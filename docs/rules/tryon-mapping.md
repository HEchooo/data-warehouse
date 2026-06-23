# 试穿任务映射关系

## 适用范围

- 本文档覆盖 `v3_decom.common_ext_ai_task` 里的试穿、创建模特与 `STYLE_ME` 相关口径
- 帖子和专栏基础映射见 `docs/rules/post-mapping.md`

## 基础表

- 表：`v3_decom.common_ext_ai_task`

## 试穿任务类型

- 平台默认试穿任务：
  - `task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')`
- 创建模特：
  - `task_type = 'MODEL'`

## 帖子试穿映射

- 试穿帖子：
  - `json_extract(request_param, '$.postId')`
  - 同时要求 `task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')`

## 常用指标映射

- 试穿任务数：
  - `task_id`
  - 条件：`task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')`
- 试穿人数：
  - `user_id`
  - 条件：`task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT')`
- 试穿成功：
  - `user_id`
  - 条件：`task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT') AND status = 'SUCCESS'`
- 试穿任务完成时长：
  - `json_value(output, '$.totalDuration')`
  - 条件：`task_type IN ('QUICK_TRY_ON', 'TRY_ON', 'OUTFIT') AND status = 'SUCCESS'`
  - `totalDuration` 单位是毫秒
- style me 人数：
  - `user_id`
  - 条件：`task_scene = 'STYLE_ME'`
