"""Structural contracts for issue #573's typed and dead-code boundaries."""

from __future__ import annotations

import ast
from pathlib import Path
import re
import unittest


class TestDispatchImportCoreCallBoundary(unittest.TestCase):
    def test_production_calls_use_explicit_typed_keywords(self) -> None:
        """Production must not hide dispatch arguments behind ``Any`` splats."""
        for relative_path in (
            "lib/dispatch/entry_points.py",
            "lib/download_processing.py",
        ):
            source = Path(relative_path).read_text(encoding="utf-8")
            tree = ast.parse(source, filename=relative_path)
            self.assertNotIn("core_kwargs", source, relative_path)
            if relative_path == "lib/download_processing.py":
                self.assertIn("dispatch_fn: DispatchCoreFn | None", source)
            calls = [
                node for node in ast.walk(tree)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "dispatch_import_core"
            ]
            self.assertTrue(calls, relative_path)
            for call in calls:
                self.assertTrue(all(keyword.arg is not None for keyword in call.keywords))
                self.assertIn("path", {keyword.arg for keyword in call.keywords})
                self.assertIn("prevalidated_candidate_result", {
                    keyword.arg for keyword in call.keywords
                })


class TestVultureProductionLivenessPolicy(unittest.TestCase):
    def test_dead_code_scan_does_not_treat_tests_as_production_callers(self) -> None:
        """Tests may exercise an API, but cannot by themselves keep it alive."""
        source = Path("scripts/find_dead_code.sh").read_text(encoding="utf-8")
        match = re.search(r"SOURCES=\((.*?)\)\n", source, flags=re.DOTALL)
        self.assertIsNotNone(match)
        assert match is not None
        source_roots = match.group(1).split()
        self.assertNotIn("tests", source_roots)
        self.assertIn("lib", source_roots)
        self.assertIn("web", source_roots)


if __name__ == "__main__":
    unittest.main()
