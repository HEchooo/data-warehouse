"""初始化并启动本地 Superset；不运行 ETL，不修改线上 Superset。

依赖安装见 demo/ever_pretty/README.md。仅监听 127.0.0.1:8089。
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "logs/ever_pretty_demo"
VENV = RUNTIME / ".venv"


def local_env():
    home = RUNTIME / "superset"
    home.mkdir(parents=True, exist_ok=True, mode=0o700)
    for filename, content in [
        ("secret_key", secrets.token_urlsafe(48)),
        ("login.json", json.dumps({"username": "local_demo", "password": secrets.token_urlsafe(24)})),
    ]:
        path = home / filename
        if not path.exists():
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w") as out:
                out.write(content)
    return {**os.environ,
        "EVER_PRETTY_SUPERSET_HOME": str(home),
        "SUPERSET_HOME": str(home),
        "SUPERSET_CONFIG_PATH": str(ROOT / "demo/ever_pretty/superset_config.py"),
        "FLASK_APP": "superset",
        "PYTHONDONTWRITEBYTECODE": "1",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--init", action="store_true")
    args = parser.parse_args()
    env = local_env()
    binary = VENV / "bin/superset"
    if not binary.exists():
        raise SystemExit("请先按 demo/ever_pretty/README.md 安装本地依赖")
    if args.init:
        subprocess.run([str(binary), "db", "upgrade"], env=env, check=True)
        # 凭据由子进程从受限本地文件读取，不打印或放进命令参数。
        create_user = """
import json, os
from pathlib import Path
from superset.app import create_app
from superset import security_manager
app = create_app()
with app.app_context():
    login = json.loads((Path(os.environ['EVER_PRETTY_SUPERSET_HOME']) / 'login.json').read_text())
    if not security_manager.find_user(username=login['username']):
        security_manager.add_user(username=login['username'], first_name='Local', last_name='Demo', email='demo@localhost', role=security_manager.find_role('Admin'), password=login['password'])
"""
        subprocess.run([str(VENV / "bin/python"), "-B", "-c", create_user], env=env, check=True)
        subprocess.run([str(binary), "init"], env=env, check=True)
        return
    os.execve(str(VENV / "bin/gunicorn"), [
        str(VENV / "bin/gunicorn"), "--bind", "127.0.0.1:8089", "--workers", "1",
        "--threads", "8", "--timeout", "120", "superset.app:create_app()",
    ], env)


if __name__ == "__main__":
    main()
