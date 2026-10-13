#!/usr/bin/env python3

from __future__ import annotations

import fcntl
import json
import signal
import urllib.request
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPT_DIR / "env" / "etl_config.json"

# 优先读取环境变量 PATH_MODE_OVERRIDE；未设置时回落到配置文件 path_mode
PATH_MODE_OVERRIDE = os.environ.get("PATH_MODE_OVERRIDE")
if PATH_MODE_OVERRIDE:
    PATH_MODE_OVERRIDE = PATH_MODE_OVERRIDE.strip().lower()


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
    seen: set[str] = set()
    for i, task in enumerate(tasks):
        if not isinstance(task, dict):
            fail(f"tasks[{i}] 必须是对象")

        enabled = task.get("enabled", True)
        if not isinstance(enabled, bool):
            fail(f"tasks[{i}].enabled 必须是布尔值")

        table_name = must_non_empty_str(task.get("table_name"), f"tasks[{i}].table_name")
        if table_name in seen:
            fail(f"重复任务: {table_name}")
        dependencies = task.get("depends_on", [])
        if not isinstance(dependencies, list) or any(
            not isinstance(dep, str) or dep not in seen for dep in dependencies
        ):
            fail(f"{table_name}.depends_on 必须引用配置中排在前面的任务")
        seen.add(table_name)
        normalized_tasks.append(
            {
                "table_name": table_name,
                "depends_on": dependencies,
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


def send_failure_alert(config: dict, statuses: dict[str, str]) -> None:
    """只发送任务标识和状态，不向群发送日志或异常中的凭据。"""
    webhook_path = config["project_dir"] / "env" / "lark_webhook.json"
    data = json.loads(webhook_path.read_text(encoding="utf-8"))
    webhook = data.get(config["mode"])
    if not isinstance(webhook, str) or not webhook.strip():
        raise ValueError("未配置告警 webhook")
    details = "\n".join(
        f"{name}: {status}" for name, status in statuses.items()
        if status in {"failed", "blocked"}
    )
    payload = {"msg_type": "text", "content": {"text":
        f"ETL 失败告警 ({config['mode']}) {now_text()}\n{details}\n请检查 ETL 日志。"}}
    request = urllib.request.Request(
        webhook, data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        body = json.load(response)
    if body.get("code", body.get("StatusCode")) != 0:
        raise RuntimeError("告警服务未确认发送成功")


def run_task(config: dict, task: dict, log_file, lock_fd: int) -> int:
    script_path = resolve_script_path(config["project_dir"], task["script"])
    if not script_path.is_file():
        raise FileNotFoundError(script_path)
    process = subprocess.Popen(
        [config["python_bin"], str(script_path)],
        cwd=str(config["project_dir"]),
        env={**os.environ, "PYTHONUNBUFFERED": "1", **config["env"]},
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace", bufsize=1,
        start_new_session=True, pass_fds=(lock_fd,),
    )
    try:
        assert process.stdout is not None
        for line in process.stdout:
            write_both(log_file, line)
        return process.wait()
    finally:
        # 中断时先收掉子进程组，再释放锁。父进程被 SIGKILL 时子进程仍持锁。
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
        if process.stdout is not None:
            process.stdout.close()


def run_etl(config: dict, config_path: Path) -> int:
    project_dir: Path = config["project_dir"]
    log_dir: Path = config["log_dir"]
    if not project_dir.is_dir():
        fail(f"project_dir 不存在: {project_dir}")

    # 锁绑定项目目录，不随日期、日志目录或 local/remote 模式变化；不可删除锁文件。
    with (project_dir / ".etl_run.lock").open("a") as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("已有 ETL 在运行，本轮跳过（未执行任何任务）")
            return 0
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"etl_{datetime.now().strftime('%Y%m%d')}.log"
        with log_path.open("a", encoding="utf-8") as log_file:
            log_section(log_file, f"ETL 开始: {now_text()}")
            log_line(log_file, f"使用配置文件: {config_path}")
            log_line(log_file, f"运行模式: {config['mode']}")
            statuses: dict[str, str] = {}
            for task in config["tasks"]:
                name = task["table_name"]
                if not task["enabled"]:
                    statuses[name] = "disabled"
                    log_line(log_file, f"{task['name']} 已禁用，跳过")
                    continue
                blocked_by = [dep for dep in task["depends_on"]
                              if statuses.get(dep) != "success"]
                if blocked_by:
                    statuses[name] = "blocked"
                    log_line(log_file, f"{task['name']} 阻断，上游未成功: {', '.join(blocked_by)}")
                    continue
                log_line(log_file, f"开始执行 {task['name']}")
                try:
                    code = run_task(config, task, log_file, lock_file.fileno())
                except OSError as exc:
                    # 不输出可能含凭据的异常正文。
                    log_line(log_file, f"{task['name']} 启动或读取失败: {type(exc).__name__}")
                    code = 1
                statuses[name] = "success" if code == 0 else "failed"
                label = "成功" if code == 0 else "失败，继续独立链路"
                log_line(log_file, f"{task['name']} {label} (exit={code})")

            unsuccessful = any(s in {"failed", "blocked"} for s in statuses.values())
            log_line(log_file, "任务汇总: " + json.dumps(statuses, ensure_ascii=False))
            if unsuccessful:
                try:
                    send_failure_alert(config, statuses)
                    log_line(log_file, "失败告警已发送")
                except Exception as exc:
                    log_line(log_file, f"失败告警发送失败: {type(exc).__name__}，请检查通知配置/服务")
                log_section(log_file, f"ETL 存在失败或阻断: {now_text()}")
            else:
                log_section(log_file, f"ETL 全部完成: {now_text()}")
            return 1 if unsuccessful else 0


def main() -> int:
    def stop(signum, frame):
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    config_path = CONFIG_PATH.expanduser()
    config = load_config(config_path)
    return run_etl(config, config_path)


if __name__ == "__main__":
    raise SystemExit(main())
