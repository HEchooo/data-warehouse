# 环境规则

## Python 环境

- 执行 Python 前，先判断当前机器。
- 再决定使用：
  - `/Users/sinn/Documents/work_py_venv`
  - `/Users/sinn/Documents/family_py_venv`

## 执行入口

- 执行脚本入口是 `etl_run.py`。

## PATH_MODE_OVERRIDE

- 本地测试时设置：`PATH_MODE_OVERRIDE="local"`
- 生产环境设置：`PATH_MODE_OVERRIDE="remote"`
