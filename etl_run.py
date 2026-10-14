#!/usr/bin/env python3

from __future__ import annotations

import fcntl
import json
import signal
import os
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timezone
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPT_DIR / "env" / "etl_config.json"

# 不是数据依赖：共享状态表的写入以及同一提供方的请求不能同时执行。
# 资源名不带凭据；任务自身的目标表和脚本也自动互斥。
SHARED_RESOURCES = {
    "dwd_event_log": ("table:oss_key_process_log",),
    "dwd_makepod_event_log": ("table:oss_key_process_log",),
    "ods_ecn_account_affiliation": ("api:ecn",),
    "ods_ecn_video_publication_owner": ("api:ecn",),
    "ods_tiktok_ad_daily": ("api:tiktok",),
    "ods_tiktok_gmv_max_daily": ("api:tiktok",),
}
FAILURE_STATUSES = {"failed", "blocked", "timeout", "cancelled", "recovery_required", "lock_skipped"}


def positive_int(value, field: str, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        fail(f"{field} 必须是 1 到 {maximum} 的整数")
    return value


def scheduler_options(data: dict) -> dict:
    options = data.get("scheduler", {})
    if not isinstance(options, dict):
        fail("scheduler 必须是对象")
    defaults = {"max_workers": (4, 4), "task_timeout_seconds": (900, 86400),
                "terminate_grace_seconds": (5, 30)}
    if set(options) - set(defaults):
        fail("scheduler 存在未知配置项")
    return {key: positive_int(options.get(key, default), f"scheduler.{key}", maximum)
            for key, (default, maximum) in defaults.items()}


def task_resources(task: dict, project_dir: Path = SCRIPT_DIR) -> set[str]:
    name = task["table_name"]
    script = (project_dir / Path(task['script']).expanduser()).resolve()
    return {f"table:{name}", f"script:{script}",
            *SHARED_RESOURCES.get(name, ()), *task.get("resources", [])}

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
        if not all(c.isalnum() or c == "_" for c in table_name):
            fail("table_name 只能包含字母、数字、下划线")
        if table_name in seen:
            fail(f"重复任务: {table_name}")
        dependencies = task.get("depends_on", [])
        if not isinstance(dependencies, list) or any(
            not isinstance(dep, str) or dep not in seen for dep in dependencies
        ):
            fail(f"{table_name}.depends_on 必须引用配置中排在前面的任务")
        seen.add(table_name)
        resources = task.get("resources", [])
        if not isinstance(resources, list) or any(not isinstance(r, str) or not r.strip() for r in resources):
            fail(f"{table_name}.resources 必须是非空字符串数组")
        normalized_tasks.append(
            {
                "table_name": table_name,
                "depends_on": dependencies,
                "stage": must_non_empty_str(task.get("stage"), f"tasks[{i}].stage"),
                "name": must_non_empty_str(task.get("name"), f"tasks[{i}].name"),
                "script": must_non_empty_str(task.get("script"), f"tasks[{i}].script"),
                "enabled": enabled,
                "resources": [r.strip() for r in resources],
                "timeout_seconds": positive_int(task.get("timeout_seconds", 900),
                                                f"{table_name}.timeout_seconds", 86400)
                    if "timeout_seconds" in task else None,
            }
        )

    return {
        "mode": mode,
        "project_dir": project_dir,
        "python_bin": python_bin,
        "log_dir": log_dir,
        "env": normalized_env,
        "tasks": normalized_tasks,
        "scheduler": scheduler_options(data),
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
        if status in FAILURE_STATUSES
    )
    payload = {"msg_type": "text", "content": {"text":
        f"ETL 失败告警 ({config['mode']}) {now_text()}\n"
        f"run_id={config.get('_run_id', 'unknown')}\n{details}\n请检查 ETL 日志。"}}
    # socket timeout 不是总期限（DNS / 慢速响应可拖住线程）。独立进程设置硬期限；
    # webhook 仅经 stdin 传入，不出现在命令行，子进程不继承 ETL 锁。
    transport = '''import json, sys, urllib.request
data = json.load(sys.stdin)
request = urllib.request.Request(data['webhook'],
    data=json.dumps(data['payload']).encode('utf-8'),
    headers={'Content-Type': 'application/json'}, method='POST')
with urllib.request.urlopen(request, timeout=15) as response:
    body = response.read(65537)
    if len(body) > 65536:
        raise ValueError('oversized response')
    sys.stdout.buffer.write(body)
'''
    result = subprocess.run(
        [sys.executable, "-c", transport],
        input=json.dumps({"webhook": webhook, "payload": payload}),
        text=True, capture_output=True, timeout=20, check=True,
        close_fds=True,
    )
    body = json.loads(result.stdout)
    if body.get("code", body.get("StatusCode")) != 0:
        raise RuntimeError("告警服务未确认发送成功")


class TaskTimeout(Exception):
    pass


class TaskCancelled(Exception):
    pass


class TaskUncertain(Exception):
    pass


def stop_process_group(process, grace: int) -> None:
    # 即使组长已经退出，也清理留在同一组的子进程，避免占用锁/继续写入。
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        process.wait()
        return
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        process.poll()  # 回收组长，避免将 zombie 当作存活进程。
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            process.wait()
            return
        time.sleep(.05)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    # SIGKILL 也可能因内核 I/O 等原因不能立即完成；不能无限卡住工作线程。
    try:
        process.wait(timeout=1)
    except subprocess.TimeoutExpired as exc:
        raise TaskUncertain() from exc


def run_task(config: dict, task: dict, log_file, lock_fd: int) -> int:
    script_path = resolve_script_path(config["project_dir"], task["script"])
    if not script_path.is_file():
        raise FileNotFoundError(script_path)
    options = scheduler_options(config)
    stop_event = config["_stop_event"]
    if stop_event.is_set():
        raise TaskCancelled()
    # 每个任务使用独立文件，避免并发交错、PIPE 填满或无换行输出阻塞超时。
    output_path = config["_run_dir"] / (task["table_name"] + ".log")
    with output_path.open("ab", buffering=0) as output:
        process = subprocess.Popen(
            [config["python_bin"], str(script_path)],
            cwd=str(config["project_dir"]),
            env={**os.environ, **config["env"], "PYTHONUNBUFFERED": "1",
                 "ETL_RUN_ID": config["_run_id"], "ETL_TASK_ID": task["table_name"]},
            stdout=output, stderr=subprocess.STDOUT,
            start_new_session=True, pass_fds=(lock_fd,),
        )
        timeout = task.get("timeout_seconds") or options["task_timeout_seconds"]
        deadline = time.monotonic() + timeout
        try:
            while True:
                if stop_event.is_set():
                    raise TaskCancelled()
                if process.poll() is not None:
                    try:
                        os.killpg(process.pid, 0)
                    except ProcessLookupError:
                        pass
                    else:
                        # 组长退出却仍有后台子进程，不能把写入状态当作已确认。
                        raise TaskUncertain()
                    return process.returncode
                if time.monotonic() >= deadline:
                    raise TaskTimeout()
                stop_event.wait(.05)
        except (TaskTimeout, TaskCancelled, TaskUncertain):
            raise
        except Exception as exc:
            raise TaskUncertain() from exc
        finally:
            try:
                stop_process_group(process, options["terminate_grace_seconds"])
            except Exception as exc:
                raise TaskUncertain() from exc


def save_state(path: Path, state: dict) -> None:
    # 先落盘再启动子进程；父进程被 SIGKILL 后，下一轮仍能识别未确认的任务。
    temp = path.with_suffix(".tmp")
    with temp.open("w", encoding="utf-8") as out:
        json.dump(state, out, ensure_ascii=False)
        out.flush()
        os.fsync(out.fileno())
    temp.replace(path)


def run_etl(config: dict, config_path: Path) -> int:
    project_dir: Path = config["project_dir"]
    log_dir: Path = config["log_dir"]
    if not project_dir.is_dir():
        fail(f"project_dir 不存在: {project_dir}")
    options = scheduler_options(config)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ_") + uuid.uuid4().hex[:12]
    run_dir = log_dir / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    config = {**config, "_run_id": run_id, "_run_dir": run_dir,
              "_stop_event": threading.Event()}
    # 所有调度状态与日志由主线程拥有；工作线程只运行子进程。
    with (project_dir / ".etl_run.lock").open("a") as lock_file, \
            (run_dir / "events.jsonl").open("a", encoding="utf-8") as metrics, \
            (log_dir / f"etl_{datetime.now().strftime('%Y%m%d')}.log").open("a", encoding="utf-8") as log_file:
        def event(kind, **fields):
            record = {"event": kind, "run_id": run_id,
                      "timestamp": datetime.now(timezone.utc).isoformat(), **fields}
            metrics.write(json.dumps(record, ensure_ascii=False) + "\n")
            metrics.flush()

        def alert(snapshot):
            try:
                send_failure_alert(config, snapshot)
                return True, None
            except Exception as exc:
                return False, type(exc).__name__

        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            event("run_skipped", reason="lock_busy")
            log_line(log_file, "已有 ETL 在运行，本轮跳过（未执行任何任务）")
            ok, error = alert({"scheduler": "lock_skipped"})
            event("alert", success=ok, error_type=error)
            return 0

        state_path = project_dir / ".etl_task_state.json"
        # 获得锁后仍残留 running，说明上轮没有确认完成。不得盲目重新提交远端写作业。
        try:
            state = json.loads(state_path.read_text()) if state_path.exists() else {}
            if not isinstance(state, dict) or any(
                not isinstance(entry, dict) or not isinstance(entry.get("run_id"), str)
                or not isinstance(entry.get("resources"), list)
                or not all(isinstance(r, str) for r in entry["resources"])
                or f"table:{name}" not in entry["resources"]
                for name, entry in state.items()
            ):
                raise ValueError("invalid state")
        except (OSError, ValueError):
            event("run_finished", status="failed", reason="invalid_recovery_state")
            log_line(log_file, "恢复状态不可读，拒绝启动任务；请核对 .etl_task_state.json")
            ok, error = alert({"scheduler": "failed"})
            event("alert", success=ok, error_type=error)
            return 1
        for entry in state.values():
            entry["status"] = "recovery_required"
        try:
            save_state(state_path, state)
        except OSError as exc:
            event("run_finished", status="failed", reason="recovery_state_write_failed",
                  error_type=type(exc).__name__)
            ok, error = alert({"scheduler": "failed"})
            event("alert", success=ok, error_type=error)
            return 1
        tasks = {task["table_name"]: task for task in config["tasks"]}
        statuses = {name: "pending" if task["enabled"] else "disabled" for name, task in tasks.items()}
        running = {}
        ready_at = {}
        durations = {}
        peak_running = 0
        alert_future = None
        sent = {}
        alert_attempted = {}
        alert_ok = True
        retry_at = None
        retried = False
        workers = ThreadPoolExecutor(max_workers=options["max_workers"])
        notifications = ThreadPoolExecutor(max_workers=1)
        log_section(log_file, f"ETL 开始: {now_text()}")
        log_line(log_file, f"run_id={run_id}，模式={config['mode']}，并发上限={options['max_workers']}")
        event("run_started", max_workers=options["max_workers"], task_count=len(tasks))
        for name, status in statuses.items():
            if status == "disabled":
                event("task_finished", task=name, status=status, duration_seconds=0)

        def check_alert():
            nonlocal alert_future, alert_ok, sent, retry_at
            if alert_future is not None and alert_future.done():
                alert_ok, error = alert_future.result()
                event("alert", success=alert_ok, error_type=error, statuses=alert_attempted)
                if alert_ok:
                    sent = dict(alert_attempted)
                else:
                    log_line(log_file, f"失败告警发送失败: {error}，请检查通知配置/服务")
                    retry_at = time.monotonic() + 1
                alert_future = None

        def submit_alert(final=False):
            nonlocal alert_future, alert_attempted, retried, retry_at
            snapshot = {n: s for n, s in statuses.items() if s in FAILURE_STATUSES}
            changed = snapshot != alert_attempted
            retry_due = not retried and retry_at is not None and (final or time.monotonic() >= retry_at)
            if snapshot and alert_future is None and snapshot != sent and (changed or retry_due):
                retried = False if changed else True
                retry_at = None
                alert_attempted = dict(snapshot)
                alert_future = notifications.submit(alert, dict(snapshot))

        try:
            while any(s in {"pending", "running"} for s in statuses.values()):
                check_alert()
                for future, (name, began) in list(running.items()):
                    if not future.done():
                        continue
                    del running[future]
                    code = None
                    error = None
                    try:
                        code = future.result()
                        # 被外部 SIGKILL/OOM/信号终止也不能证明远端写入已结束。
                        status = "success" if code == 0 else "recovery_required" if code < 0 else "failed"
                    except TaskTimeout:
                        status = "timeout"
                    except TaskCancelled:
                        status = "cancelled"
                    except TaskUncertain:
                        status = "recovery_required"
                    except Exception as exc:
                        status, error = "failed", type(exc).__name__
                    statuses[name] = status
                    elapsed = time.monotonic() - began
                    durations[name] = elapsed
                    if status in {"timeout", "cancelled", "recovery_required"}:
                        state[name]["status"] = "recovery_required"
                    else:
                        state.pop(name, None)
                    save_state(state_path, state)
                    event("task_finished", task=name, status=status, exit_code=code,
                          duration_seconds=round(elapsed, 3), error_type=error)
                    log_line(log_file, f"{name} {status} (exit={code}, duration={elapsed:.2f}s)")

                quarantined_resources = set().union(*(set(entry["resources"]) for entry in state.values()
                                                     if entry["status"] == "recovery_required")) if state else set()
                busy = set().union(*(task_resources(tasks[n], project_dir) for n, _ in running.values())) if running else set()
                for name, task in tasks.items():
                    if statuses[name] != "pending":
                        continue
                    bad_deps = [dep for dep in task["depends_on"]
                                if statuses[dep] not in {"pending", "running", "success"}]
                    conflict = sorted(task_resources(task, project_dir) & quarantined_resources)
                    if bad_deps or conflict:
                        statuses[name] = "recovery_required" if conflict else "blocked"
                        event("task_finished", task=name, status=statuses[name], blocked_by=bad_deps,
                              recovery_resources=conflict, duration_seconds=0)
                        log_line(log_file, f"{name} {statuses[name]}，上游={bad_deps}，待核对资源={conflict}")
                        continue
                    if not all(statuses[dep] == "success" for dep in task["depends_on"]):
                        continue
                    ready_at.setdefault(name, time.monotonic())
                    resources = task_resources(task, project_dir)
                    if len(running) >= options["max_workers"] or resources & busy:
                        continue
                    began = time.monotonic()
                    state[name] = {"status": "running", "run_id": run_id, "resources": sorted(resources)}
                    save_state(state_path, state)
                    statuses[name] = "running"
                    event("task_started", task=name, stage=task["stage"],
                          dependency_wait_seconds=round(ready_at[name] - started, 3),
                          queue_seconds=round(began - ready_at[name], 3),
                          timeout_seconds=task.get("timeout_seconds") or options["task_timeout_seconds"],
                          log_path=str(run_dir / (name + ".log")))
                    log_line(log_file, f"开始执行 {name}")
                    future = workers.submit(run_task, config, task, log_file, lock_file.fileno())
                    running[future] = (name, began)
                    busy.update(resources)
                    peak_running = max(peak_running, len(running))
                submit_alert()
                if running:
                    wait(running, timeout=.05, return_when=FIRST_COMPLETED)
                elif any(s == "pending" for s in statuses.values()):
                    raise RuntimeError("任务图无法推进")
            check_alert()
            if alert_future is not None:
                alert_future.result()
                check_alert()
            # 收尾的新汇总也有一次重试机会；同一快照绝不重复无限重试。
            for _ in range(2):
                submit_alert(final=True)
                if alert_future is None:
                    break
                alert_future.result()
                check_alert()
            unsuccessful = any(s in FAILURE_STATUSES for s in statuses.values())
            event("run_finished", status="failed" if unsuccessful else "success", statuses=statuses,
                  duration_seconds=round(time.monotonic() - started, 3), peak_running=peak_running,
                  task_seconds=round(sum(durations.values()), 3), alert_success=alert_ok)
            log_line(log_file, "任务汇总: " + json.dumps(statuses, ensure_ascii=False))
            log_section(log_file, f"{'ETL 存在失败或阻断' if unsuccessful else 'ETL 全部完成'}: {now_text()}")
            return 1 if unsuccessful else 0
        except Exception as exc:
            statuses["scheduler"] = "failed"
            event("scheduler_failed", error_type=type(exc).__name__)
            return 1
        except BaseException:
            # 信号可能恰好发生在两个任务之间，此时 running 为空也必须通知。
            statuses["scheduler"] = "cancelled"
            raise
        finally:
            config["_stop_event"].set()
            # 先让所有工作线程终止并回收子进程，最后才关闭持锁文件。
            workers.shutdown(wait=True)
            notifications.shutdown(wait=True)
            if running or statuses.get("scheduler") in {"failed", "cancelled"}:
                for _, (name, _) in running.items():
                    statuses[name] = "cancelled"
                    state[name]["status"] = "recovery_required"
                # 写盘失败也不能跳过通知；启动前的持久记录仍保留为 running。
                try:
                    if running:
                        save_state(state_path, state)
                except OSError as exc:
                    event("recovery_state_write_failed", error_type=type(exc).__name__)
                event("run_interrupted", statuses=statuses, duration_seconds=round(time.monotonic() - started, 3))
                ok, error = alert({n: s for n, s in statuses.items() if s in FAILURE_STATUSES})
                event("alert", success=ok, error_type=error)


def main() -> int:
    def stop(signum, frame):
        # 清理期间重复信号不能再次打断 shutdown，提前释放父进程资源。
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    config_path = CONFIG_PATH.expanduser()
    config = load_config(config_path)
    return run_etl(config, config_path)


if __name__ == "__main__":
    raise SystemExit(main())
