"""Structural contracts for issue #573's typed and dead-code boundaries."""

from __future__ import annotations

import ast
from pathlib import Path
import re
import unittest


EXPECTED_VULTURE_SOURCE_ROOTS = (
    "lib",
    "web",
    "harness",
    "scripts",
    "cratedigger.py",
    "album_source.py",
)


def _vulture_source_roots(source: str) -> tuple[str, ...]:
    """Parse the simple shell SOURCES array, discarding comments/whitespace."""
    match = re.search(
        r"^[ \t]*SOURCES=\((.*?)^[ \t]*\)",
        source,
        flags=re.DOTALL | re.MULTILINE,
    )
    if match is None:
        raise AssertionError("scripts/find_dead_code.sh has no SOURCES array")
    roots: list[str] = []
    for line in match.group(1).splitlines():
        value = line.split("#", 1)[0].strip()
        if value:
            roots.extend(value.split())
    return tuple(roots)


def assert_vulture_production_roots(roots: tuple[str, ...]) -> None:
    """Assert every and only canonical production root is scanned, in order."""
    assert roots == EXPECTED_VULTURE_SOURCE_ROOTS
    assert "tests" not in roots


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

    def test_production_callable_has_a_pyright_conformance_binding(self) -> None:
        source = Path("lib/dispatch/__init__.py").read_text(encoding="utf-8")
        self.assertIn(
            "_dispatch_core_conformance: DispatchCoreFn = dispatch_import_core",
            source,
        )


class TestVultureProductionLivenessPolicy(unittest.TestCase):
    def test_dead_code_scan_does_not_treat_tests_as_production_callers(self) -> None:
        """Tests may exercise an API, but cannot by themselves keep it alive."""
        source = Path("scripts/find_dead_code.sh").read_text(encoding="utf-8")
        assert_vulture_production_roots(_vulture_source_roots(source))

    def test_source_root_checker_rejects_a_dropped_production_root(self) -> None:
        for index, root in enumerate(EXPECTED_VULTURE_SOURCE_ROOTS):
            with self.subTest(root=root), self.assertRaises(AssertionError):
                assert_vulture_production_roots(
                    EXPECTED_VULTURE_SOURCE_ROOTS[:index]
                    + EXPECTED_VULTURE_SOURCE_ROOTS[index + 1:]
                )

    def test_source_parser_ignores_inline_and_whole_line_comments(self) -> None:
        source = """SOURCES=(
          # production roots only
          lib # package
          web
          harness scripts
          cratedigger.py
          album_source.py
        )
        """
        self.assertEqual(_vulture_source_roots(source), EXPECTED_VULTURE_SOURCE_ROOTS)


if __name__ == "__main__":
    unittest.main()
