#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse


def resolve_project_root() -> Path:
    env_root = os.environ.get("CLAUDE_PROJECT_DIR")
    if env_root:
        return Path(env_root).expanduser().resolve()
    return Path.cwd().resolve()


def normalize_base_url(raw_url: str) -> str:
    url = raw_url.strip()
    if not url:
        raise ValueError("superset url is empty")
    if not url.endswith("/"):
        url = f"{url}/"
    return url


def join_under_origin(origin: str, base_path: str, suffix: str) -> str:
    suffix = suffix.lstrip("/")
    if base_path in ("", "/"):
        return f"{origin}/{suffix}"
    return f"{origin}{base_path.rstrip('/')}/{suffix}"


def build_login_candidates(base_url: str) -> list[str]:
    parsed = urlparse(base_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    base_path = parsed.path.rstrip("/")
    candidates = [
        join_under_origin(origin, base_path, "login/"),
        join_under_origin(origin, base_path, "login"),
        join_under_origin(origin, base_path, "welcome/"),
        f"{origin}/login/",
        f"{origin}/login",
        f"{origin}/superset/welcome/",
    ]
    return dedupe(candidates)


def build_sql_lab_candidates(base_url: str) -> list[str]:
    parsed = urlparse(base_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    base_path = parsed.path.rstrip("/")
    candidates = [
        join_under_origin(origin, base_path, "sqllab/"),
        join_under_origin(origin, base_path, "sql_lab/"),
        f"{origin}/superset/sqllab/",
        f"{origin}/superset/sql_lab/",
    ]
    return dedupe(candidates)


def dedupe(items: list[str]) -> list[str]:
    seen = set()
    ordered = []
    for item in items:
        if item not in seen:
            ordered.append(item)
            seen.add(item)
    return ordered


def main() -> int:
    project_root = resolve_project_root()
    config_path = project_root / "superset.json"

    if not config_path.exists():
        print(json.dumps({"error": f"Missing config file: {config_path}"}, ensure_ascii=False))
        return 1

    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        print(json.dumps({"error": f"Invalid JSON in {config_path}: {exc}"}, ensure_ascii=False))
        return 1

    url = str(config.get("url", "")).strip()
    username = str(config.get("username", "")).strip()
    password = str(config.get("password", ""))

    missing = [key for key, value in (("url", url), ("username", username), ("password", password)) if not value]
    if missing:
        print(json.dumps({"error": f"Missing required keys in {config_path}: {', '.join(missing)}"}, ensure_ascii=False))
        return 1

    base_url = normalize_base_url(url)
    result = {
        "config_path": str(config_path),
        "url": url,
        "base_url": base_url,
        "username": username,
        "password": password,
        "login_candidates": build_login_candidates(base_url),
        "sql_lab_candidates": build_sql_lab_candidates(base_url),
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
