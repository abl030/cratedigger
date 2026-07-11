"""Structural contracts for issue #573's typed and dead-code boundaries."""

from __future__ import annotations

import ast
import importlib
from pathlib import Path
import unittest


EXPECTED_VULTURE_SOURCE_ROOTS = (
    "lib",
    "web",
    "harness",
    "scripts",
    "cratedigger.py",
    "album_source.py",
)


def _production_source_roots(source: str) -> tuple[str, ...]:
    """Parse the shared production-root file, discarding comments/blanks."""
    return tuple(
        line.strip()
        for line in source.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )


def assert_vulture_production_roots(roots: tuple[str, ...]) -> None:
    """Assert every and only canonical production root is scanned, in order."""
    assert roots == EXPECTED_VULTURE_SOURCE_ROOTS
    assert "tests" not in roots


def assert_completion_orchestrator_responsibilities(source: str) -> None:
    """Keep completion orchestration free of validation implementation."""
    tree = ast.parse(source, filename="lib/download_processing.py")
    classes = {
        node.name for node in tree.body if isinstance(node, ast.ClassDef)
    }
    functions = {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    assert classes == {
        "Completed",
        "CompletionFailed",
        "CompletionDispatched",
        "CompletionDeferred",
    }
    assert functions == {"process_completed_album"}


class TestDispatchImportCoreCallBoundary(unittest.TestCase):
    def test_production_calls_use_explicit_typed_keywords(self) -> None:
        """Production must not hide dispatch arguments behind ``Any`` splats."""
        for relative_path in (
            "lib/dispatch/entry_points.py",
            "lib/download_validation.py",
        ):
            source = Path(relative_path).read_text(encoding="utf-8")
            tree = ast.parse(source, filename=relative_path)
            self.assertNotIn("core_kwargs", source, relative_path)
            if relative_path == "lib/download_validation.py":
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


class TestDownloadCompletionOwnership(unittest.TestCase):
    def test_processing_is_only_the_completion_orchestrator(self) -> None:
        source = Path("lib/download_processing.py").read_text(encoding="utf-8")
        assert_completion_orchestrator_responsibilities(source)
        self.assertIn("from lib import download_validation", source)

    def test_processing_does_not_compatibly_export_moved_validation_names(self) -> None:
        processing = importlib.import_module("lib.download_processing")
        for moved_name in (
            "_check_staged_audio_manifest",
            "_process_beets_validation",
            "_handle_valid_result",
        ):
            self.assertFalse(hasattr(processing, moved_name), moved_name)

    def test_validation_functions_have_executable_protocol_bindings(self) -> None:
        source = Path("lib/download_validation.py").read_text(encoding="utf-8")
        self.assertIn(
            "_validate_conformance: ValidateFn = _process_beets_validation",
            source,
        )
        self.assertIn(
            "_handle_valid_conformance: HandleValidFn = _handle_valid_result",
            source,
        )

    def test_responsibility_checker_rejects_validation_creep(self) -> None:
        source = Path("lib/download_processing.py").read_text(encoding="utf-8")
        planted = source + "\ndef _process_beets_validation():\n    pass\n"
        with self.assertRaises(AssertionError):
            assert_completion_orchestrator_responsibilities(planted)


class TestVultureProductionLivenessPolicy(unittest.TestCase):
    def test_dead_code_scan_does_not_treat_tests_as_production_callers(self) -> None:
        """Tests may exercise an API, but cannot by themselves keep it alive."""
        source = Path("tools/production_python_sources.txt").read_text(
            encoding="utf-8"
        )
        assert_vulture_production_roots(_production_source_roots(source))

    def test_source_root_checker_rejects_a_dropped_production_root(self) -> None:
        for index, root in enumerate(EXPECTED_VULTURE_SOURCE_ROOTS):
            with self.subTest(root=root), self.assertRaises(AssertionError):
                assert_vulture_production_roots(
                    EXPECTED_VULTURE_SOURCE_ROOTS[:index]
                    + EXPECTED_VULTURE_SOURCE_ROOTS[index + 1:]
                )

    def test_source_parser_ignores_comments_and_blank_lines(self) -> None:
        source = """# production roots only
lib
web

harness
scripts
cratedigger.py
album_source.py
"""
        self.assertEqual(
            _production_source_roots(source),
            EXPECTED_VULTURE_SOURCE_ROOTS,
        )


if __name__ == "__main__":
    unittest.main()
