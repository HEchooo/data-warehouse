#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPT_DIR / "env" / "etl_config.json"

# 在脚本内覆盖运行模式：None / "remote" / "local"
PATH_MODE_OVERRIDE = "remote"


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def now_clock() -> str:
    return datetime.now().strftime("%H:%M:%S")


def write_both(log_file, text: str) -> None:
    log_file.write(text)
    log_file.flush()
    sys.stdout.write(text)
    sys.stdout.flush()


def log_line(log_file, message: str) -> None:
    write_both(log_file, f"[{now_clock()}] {message}\n")


def log_section(log_file, title: str) -> None:
    text = "========================================\n"
    text += f"{title}\n"
    text += "========================================\n"
    write_both(log_file, text)


def must_non_empty_str(value, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        fail(f"{field} 必须是非空字符串")
    return value.strip()


def load_config(config_path: Path) -> dict:
    if not config_path.exists():
        fail(f"配置文件不存在: {config_path}")

    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        fail(f"配置文件 JSON 解析失败: {config_path} ({exc})")

    mode = PATH_MODE_OVERRIDE or data.get("path_mode")
    if mode not in {"remote", "local"}:
        fail("运行模式必须是 remote 或 local")

    paths = data.get("paths")
    if not isinstance(paths, dict):
        fail("配置项 paths 必须是对象")

    runtime = paths.get(mode)
    if not isinstance(runtime, dict):
        fail(f"paths.{mode} 必须是对象")

    project_dir = Path(
        must_non_empty_str(runtime.get("project_dir"), f"paths.{mode}.project_dir")
    ).expanduser()
    python_bin = must_non_empty_str(runtime.get("python"), f"paths.{mode}.python")
    log_dir = Path(
        must_non_empty_str(runtime.get("log_dir"), f"paths.{mode}.log_dir")
    ).expanduser()
    runtime_env = runtime.get("env", {})
    if runtime_env is None:
        runtime_env = {}
    if not isinstance(runtime_env, dict):
        fail(f"paths.{mode}.env 必须是对象")

    normalized_env: dict[str, str] = {}
    for key, value in runtime_env.items():
        if not isinstance(key, str) or not key.strip():
            fail(f"paths.{mode}.env 的键必须是非空字符串")
        normalized_env[key.strip()] = must_non_empty_str(
            value, f"paths.{mode}.env.{key}"
        )

    tasks = data.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        fail("配置项 tasks 必须是非空数组")

    normalized_tasks: list[dict] = []
    for i, task in enumerate(tasks):
        if not isinstance(task, dict):
            fail(f"tasks[{i}] 必须是对象")

        enabled = task.get("enabled", True)
        if not isinstance(enabled, bool):
            fail(f"tasks[{i}].enabled 必须是布尔值")

        normalized_tasks.append(
            {
                "stage": must_non_empty_str(task.get("stage"), f"tasks[{i}].stage"),
                "name": must_non_empty_str(task.get("name"), f"tasks[{i}].name"),
                "script": must_non_empty_str(task.get("script"), f"tasks[{i}].script"),
                "enabled": enabled,
            }
        )

    return {
        "mode": mode,
        "project_dir": project_dir,
        "python_bin": python_bin,
        "log_dir": log_dir,
        "env": normalized_env,
        "tasks": normalized_tasks,
    }


def resolve_script_path(project_dir: Path, script: str) -> Path:
    script_path = Path(script).expanduser()
    if script_path.is_absolute():
        return script_path
    return project_dir / script_path


def run_etl(config: dict, config_path: Path) -> int:
    project_dir: Path = config["project_dir"]
    python_bin: str = config["python_bin"]
    log_dir: Path = config["log_dir"]
    runtime_env: dict[str, str] = config["env"]
    tasks: list[dict] = config["tasks"]

    if not project_dir.exists():
        fail(f"project_dir 不存在: {project_dir}")

    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"etl_{datetime.now().strftime('%Y%m%d')}.log"

    with log_path.open("a", encoding="utf-8") as log_file:
        log_section(log_file, f"ETL 开始: {now_text()}")
        log_line(log_file, f"使用配置文件: {config_path}")
        log_line(log_file, f"运行模式: {config['mode']}")
        write_both(log_file, "\n")

        current_stage = ""
        for task in tasks:
            if task["stage"] != current_stage:
                current_stage = task["stage"]
                write_both(log_file, "\n")
                log_section(log_file, f"开始执行 {current_stage} ETL")

            if not task["enabled"]:
                log_line(log_file, f"{task['name']} 已禁用，跳过")
                continue

            script_path = resolve_script_path(project_dir, task["script"])
            if not script_path.exists():
                log_line(log_file, f"{task['name']} 失败，脚本不存在: {script_path}")
                log_line(log_file, f"ETL 结束: {now_text()}")
                return 1

            process = subprocess.Popen(
                [python_bin, str(script_path)],
                cwd=str(project_dir),
                env={**os.environ, "PYTHONUNBUFFERED": "1", **runtime_env},
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            if process.stdout is None:
                log_line(log_file, f"{task['name']} 失败，无法读取输出")
                log_line(log_file, f"ETL 结束: {now_text()}")
                return 1

            for line in process.stdout:
                write_both(log_file, line)

            result_code = process.wait()
            if result_code == 0:
                log_line(log_file, f"{task['name']} 成功")
            else:
                log_line(log_file, f"{task['name']} 失败，退出")
                log_line(log_file, f"ETL 结束: {now_text()}")
                return result_code

        write_both(log_file, "\n")
        log_section(log_file, f"ETL 全部完成: {now_text()}")
        write_both(log_file, "\n")

    return 0


def main() -> int:
    config_path = CONFIG_PATH.expanduser()
    config = load_config(config_path)
    return run_etl(config, config_path)


if __name__ == "__main__":
    raise SystemExit(main())
