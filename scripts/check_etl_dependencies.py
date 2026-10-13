#!/usr/bin/env python3
"""静态核对当前脚本的表名引用，不导入任务、不连接数据库。

覆盖 SQL 字符串、f-string 片段和表名常量；忽略文档串和 SQL 注释。
动态拼表名、外部 SQL 文件或外部调度的数据源仍需人工核对。
"""
import ast
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
TABLE_NAME = re.compile(r"\b(?:ods|dwd|dim|dws|ads)_[a-z0-9_]+\b")


def referenced_tables(source: str) -> set[str]:
    tree = ast.parse(source)
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            if node.body and isinstance(node.body[0], ast.Expr):
                value = node.body[0].value
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    docstrings.add(id(value))
    refs = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstrings:
            text = re.sub(r"/\*.*?\*/|--[^\n]*", "", node.value, flags=re.DOTALL)
            refs.update(TABLE_NAME.findall(text))
    return refs


def check(root: Path = ROOT) -> list[str]:
    tasks = json.loads((root / 'env/etl_config.json').read_text())['tasks']
    managed = {task['table_name'] for task in tasks}
    errors = []
    for task in tasks:
        path = root / task['script']
        refs = referenced_tables(path.read_text(encoding='utf-8'))
        missing = (refs & managed) - {task['table_name']} - set(task['depends_on'])
        if missing:
            errors.append(f"{task['table_name']} 缺少直接依赖: {', '.join(sorted(missing))}")
    return errors


if __name__ == '__main__':
    errors = check()
    print('\n'.join(errors) if errors else 'ETL 表引用依赖校验通过')
    raise SystemExit(bool(errors))
