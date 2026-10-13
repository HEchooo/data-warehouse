#!/usr/bin/env python3

import tempfile
import unittest
from pathlib import Path

from check_knowledge import check


FACT = """# 示例

## 报表

### ads_daily_example

- 表：`project.dataset.ads_daily_example`
- 状态：`active`

## 字段

| 指标 | 字段 | 统计主体 | 聚合限制 |
|---|---|---|---|
| 示例 | `value` | 示例实体 | 可求和 |

## 依赖

- DDL：`ddl/ads/ads_daily_example.sql`
- 脚本：`jobs/ads/ads_daily_example.py`
- 规则：`docs/rules/data.md`
"""


class KnowledgeCheckTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        for path in ("docs/topics/示例", "docs/rules", "ddl/ads", "jobs/ads"):
            (self.root / path).mkdir(parents=True, exist_ok=True)
        (self.root / "docs/topics/index.md").write_text(
            "| 主题 | 业务线 |\n|---|---|\n| 示例 | 测试 |\n", encoding="utf-8"
        )
        (self.root / "docs/topics/示例/fact.md").write_text(FACT, encoding="utf-8")
        (self.root / "docs/topics/示例/patterns.md").touch()
        (self.root / "docs/rules/data.md").touch()
        (self.root / "jobs/ads/ads_daily_example.py").touch()
        (self.root / "ddl/ads/ads_daily_example.sql").write_text(
            "CREATE TABLE `project.dataset.ads_daily_example` (dt DATE,\nvalue INT64);\n",
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_valid_repository(self) -> None:
        self.assertEqual(check(self.root), [])

    def test_missing_reference(self) -> None:
        (self.root / "docs/rules/data.md").unlink()
        self.assertTrue(any("引用路径不存在" in error for error in check(self.root)))

    def test_unknown_ads_field(self) -> None:
        fact = self.root / "docs/topics/示例/fact.md"
        fact.write_text(FACT.replace("`value`", "`missing_value`"), encoding="utf-8")
        self.assertTrue(any("ADS 字段 missing_value" in error for error in check(self.root)))

    def test_duplicate_ads_owner(self) -> None:
        other = self.root / "docs/topics/重复"
        other.mkdir()
        (other / "fact.md").write_text(FACT, encoding="utf-8")
        (other / "patterns.md").touch()
        index = self.root / "docs/topics/index.md"
        index.write_text(index.read_text(encoding="utf-8") + "| 重复 | 测试 |\n", encoding="utf-8")
        self.assertTrue(any("实际 2 个" in error for error in check(self.root)))


if __name__ == "__main__":
    unittest.main()
