#!/usr/bin/env python3
"""校验 topic 索引、ADS 归属、依赖路径和已声明字段。"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parents[1]
TABLE_RE = re.compile(r"\bads_[a-z0-9_]+\b")
ANY_TABLE_RE = re.compile(r"\b(?:ads|dws|dim|dwd|ods)_[a-z0-9_]+\b")
DECLARATION_RE = re.compile(r"^\s*[-*]\s*表\s*[：:](.*)$")
REFERENCE_RE = re.compile(r"\b(?:ddl|jobs|docs/rules)/[A-Za-z0-9_./-]+\.(?:sql|py|md)\b")
FIELD_HEADERS = {"字段", "ADS 字段", "落地字段"}


def cells(line: str) -> list[str]:
    line = line.strip()
    return [cell.strip() for cell in line.removeprefix("|").removesuffix("|").split("|")]


def cell_text(cell: str) -> str:
    match = re.fullmatch(r"\[([^]]+)]\([^)]*\)", cell.strip())
    return unquote((match.group(1) if match else cell).replace("`", "").strip())


def index_topics(topics: Path, errors: list[str]) -> set[str]:
    path = topics / "index.md"
    if not path.is_file():
        errors.append("docs/topics/index.md: 文件不存在")
        return set()
    lines = path.read_text(encoding="utf-8").splitlines()
    for line_no, line in enumerate(lines, 1):
        header = cells(line) if line.strip().startswith("|") else []
        if "主题" not in header:
            continue
        column = header.index("主题")
        result: set[str] = set()
        for row_no, row in enumerate(lines[line_no:], line_no + 1):
            if not row.strip().startswith("|"):
                break
            values = cells(row)
            if all(re.fullmatch(r":?-{3,}:?", value) for value in values):
                continue
            if len(values) <= column:
                errors.append(f"docs/topics/index.md:{row_no}: 主题索引列数不足")
                continue
            topic = cell_text(values[column])
            if topic in result:
                errors.append(f"docs/topics/index.md:{row_no}: 重复主题 {topic}")
            result.add(topic)
        return result
    errors.append("docs/topics/index.md: 缺少“主题”列")
    return set()


def ddl_fields(path: Path) -> set[str]:
    text = path.read_text(encoding="utf-8")
    match = re.search(r"\bCREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+`?[^`(]+`?\s*\(", text, re.I)
    if not match:
        return set()
    end = text.find(")", match.end())
    if end < 0:
        return set()
    fields = {
        field.group(1)
        for line in text[match.end() : end].splitlines()
        if (field := re.match(r"\s*`?([A-Za-z_][A-Za-z0-9_]*)`?\s+[A-Za-z]", line))
    }
    for field in re.finditer(r"\bADD\s+COLUMN(?:\s+IF\s+NOT\s+EXISTS)?\s+`?([A-Za-z_][A-Za-z0-9_]*)", text, re.I):
        fields.add(field.group(1))
    for field in re.finditer(r"\bDROP\s+COLUMN(?:\s+IF\s+EXISTS)?\s+`?([A-Za-z_][A-Za-z0-9_]*)", text, re.I):
        fields.discard(field.group(1))
    return fields


def table_blocks(lines: list[str], fact: str, errors: list[str]) -> list[dict[str, object]]:
    blocks: list[dict[str, object]] = []
    for index, line in enumerate(lines):
        declaration = DECLARATION_RE.match(line)
        if not declaration:
            continue
        end = next((i for i in range(index + 1, len(lines)) if re.match(r"^#{1,3}\s+", lines[i])), len(lines))
        block = "\n".join(lines[index:end])
        status_match = re.search(r"^\s*[-*]\s*状态\s*[：:]\s*`?([a-z]+)", block, re.M | re.I)
        status = status_match.group(1).lower() if status_match else ""
        for table in sorted(set(TABLE_RE.findall(declaration.group(1)))):
            if not status:
                errors.append(f"{fact}:{index + 1}: {table} 缺少状态")
            replacement = re.search(r"^\s*[-*]\s*替代表\s*[：:]\s*(.+)$", block, re.M)
            if status == "deprecated":
                if not replacement:
                    errors.append(f"{fact}:{index + 1}: deprecated 表 {table} 缺少替代表或无替代原因")
                elif re.fullmatch(r"`?无`?[。.]?", replacement.group(1).strip()):
                    errors.append(f"{fact}:{index + 1}: deprecated 表 {table} 声明无替代但未说明原因")
            blocks.append({"table": table, "status": status, "fact": fact, "line": index + 1})
    return blocks


def field_names(cell: str) -> set[str]:
    quoted = re.findall(r"`([^`]+)`", cell)
    if quoted:
        names: set[str] = set()
        for value in quoted:
            parts = [part.strip() for part in re.split(r"[/+]", value)]
            if "*" not in value and all(re.fullmatch(r"[a-z][a-z0-9_]*", part) for part in parts):
                names.update(parts)
        return names
    plain = cell.strip()
    return {plain} if re.fullmatch(r"[a-z][a-z0-9_]*", plain) else set()


def check_fields(
    lines: list[str], fact: str, declared: set[str], schemas: dict[str, set[str]], errors: list[str]
) -> None:
    if not declared:
        return
    heading = ""
    index = 0
    while index < len(lines):
        if match := re.match(r"^#{1,6}\s+(.+)$", lines[index]):
            heading = match.group(1)
        if not lines[index].strip().startswith("|"):
            index += 1
            continue
        table_start = index
        rows: list[tuple[int, list[str]]] = []
        while index < len(lines) and lines[index].strip().startswith("|"):
            rows.append((index + 1, cells(lines[index])))
            index += 1
        if len(rows) < 2:
            continue
        header = rows[0][1]
        field_column = next((i for i, value in enumerate(header) if cell_text(value) in FIELD_HEADERS), None)
        if field_column is None:
            continue
        table_column = next(
            (i for i, value in enumerate(header) if cell_text(value) in {"落地表", "落地位置"}), None
        )
        heading_tables = {table for table in declared if table in heading}
        for line_no, row in rows[2:]:
            if len(row) <= field_column:
                continue
            names = field_names(row[field_column])
            if not names:
                continue
            targets: set[str]
            if table_column is not None and len(row) > table_column:
                targets = set(TABLE_RE.findall(row[table_column])) & declared
                if not targets:
                    continue
            elif (
                any(marker in heading for marker in ("中间表", "不落库", "未落地", "待落地"))
                or (ANY_TABLE_RE.search(heading) and not heading_tables)
            ):
                continue
            else:
                targets = heading_tables or declared
            available = set().union(*(schemas.get(table, set()) for table in targets))
            for name in sorted(names - available):
                errors.append(
                    f"{fact}:{line_no}: ADS 字段 {name} 不存在于 "
                    f"{', '.join(sorted(targets)) or '该文档声明的 ADS DDL'}"
                )
        if index == table_start:
            index += 1


def check(root: Path = ROOT) -> list[str]:
    root = root.resolve()
    topics = root / "docs/topics"
    errors: list[str] = []
    topic_dirs = {path.name for path in topics.iterdir() if path.is_dir()} if topics.is_dir() else set()
    for topic in sorted(topic_dirs):
        for name in ("fact.md", "patterns.md"):
            if not (topics / topic / name).is_file():
                errors.append(f"docs/topics/{topic}/{name}: 文件不存在")

    indexed = index_topics(topics, errors)
    for topic in sorted(topic_dirs - indexed):
        errors.append(f"docs/topics/index.md: 缺少主题目录 {topic}")
    for topic in sorted(indexed - topic_dirs):
        errors.append(f"docs/topics/index.md: 索引主题没有对应目录 {topic}")

    ddl_paths = {path.stem: path for path in (root / "ddl/ads").glob("*.sql")}
    schemas = {table: ddl_fields(path) for table, path in ddl_paths.items()}
    declarations: dict[str, list[dict[str, object]]] = {}

    for topic in sorted(topic_dirs):
        path = topics / topic / "fact.md"
        if not path.is_file():
            continue
        fact = path.relative_to(root).as_posix()
        content = path.read_text(encoding="utf-8")
        lines = content.splitlines()
        blocks = table_blocks(lines, fact, errors)
        for block in blocks:
            declarations.setdefault(str(block["table"]), []).append(block)

        for match in REFERENCE_RE.finditer(content):
            target = match.group()
            if not (root / target).is_file():
                line_no = content.count("\n", 0, match.start()) + 1
                errors.append(f"{fact}:{line_no}: 引用路径不存在 {target}")

        declared = {str(block["table"]) for block in blocks if str(block["table"]) in ddl_paths}
        check_fields(lines, fact, declared, schemas, errors)

    for table in sorted(ddl_paths):
        owners = declarations.get(table, [])
        active = [owner for owner in owners if owner["status"] != "deprecated"]
        if not active and any(owner["status"] == "deprecated" for owner in owners):
            continue
        if len(active) != 1:
            locations = ", ".join(f'{owner["fact"]}:{owner["line"]}' for owner in active) or "无"
            errors.append(f"ddl/ads/{table}.sql: active ADS owner 应为 1 个，实际 {len(active)} 个（{locations}）")
    return sorted(set(errors))


def main() -> int:
    errors = check()
    if errors:
        print("知识校验失败：", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("知识校验通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
