# 环境规则

## Python 环境

- 执行 Python 前，先判断当前机器。
- 再决定使用：
  - `/Users/sinn/Documents/work_py_venv`
  - `/Users/sinn/Documents/family_py_venv`

## 执行入口

- 执行脚本入口是 `etl_run.py`。

## PATH_MODE_OVERRIDE

- `etl_run.py` 优先读取环境变量 `PATH_MODE_OVERRIDE`。
- 未设置时，回落到 `env/etl_config.json` 里的 `path_mode`。
- 本地测试时设置：`PATH_MODE_OVERRIDE="local"`
- 生产环境设置：`PATH_MODE_OVERRIDE="remote"`
- 生产环境的 cron / 定时任务必须显式传入 `PATH_MODE_OVERRIDE="remote"`，不能依赖 `env/etl_config.json` 默认值

## Superset

- 使用 `superset skill`。
- 看板维护统一沿用 Superset REST API，优先修现有 dataset / chart / dashboard，不平行再造一套 v2。
- 当前实例的 API `base_url` 不一定是 `/superset/`，出现 `Invalid login` 时先核对凭据和站点根路径。
- 当前实例的 dataset refresh 使用 `PUT`，不是 `POST`。
