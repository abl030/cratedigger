"""Structural contracts for issue #573's typed and dead-code boundaries."""

from __future__ import annotations

import ast
import importlib
import unittest
from pathlib import Path

from tests._source_pins import pinned_source

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
        "ProcessAlbumFn",
    }
    assert functions == {"process_completed_album"}


class TestDispatchImportCoreCallBoundary(unittest.TestCase):
    def test_production_calls_use_explicit_typed_keywords(self) -> None:
        """Production must not hide dispatch arguments behind ``Any`` splats.

        Since issue #1277 the description of the import is one positional
        ``DispatchRequest``, so the explicit-keyword requirement moved with
        it: neither the dispatch call nor the request construction may use a
        ``**`` splat, and the request must still name ``path`` and
        ``prevalidated_candidate_result`` outright. ``lib/download_validation``
        builds the request into a local first (one construction feeding both
        the injected seam and the direct call), so the audit accepts a
        request built at the call site OR a same-file
        ``<name> = DispatchRequest(...)`` assignment naming that local.
        """
        for relative_path in (
            "lib/dispatch/entry_points.py",
            "lib/download_validation.py",
        ):
            source = pinned_source(Path(relative_path))
            tree = ast.parse(source, filename=relative_path)
            self.assertNotIn("core_kwargs", source, relative_path)
            if relative_path == "lib/download_validation.py":
                self.assertIn("dispatch_fn: DispatchCoreFn | None", source)
            requests_by_local: dict[str, ast.Call] = {}
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Assign)
                    and len(node.targets) == 1
                    and isinstance(node.targets[0], ast.Name)
                    and isinstance(node.value, ast.Call)
                    and isinstance(node.value.func, ast.Name)
                    and node.value.func.id == "DispatchRequest"
                ):
                    requests_by_local[node.targets[0].id] = node.value
            calls = [
                node for node in ast.walk(tree)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id in {"dispatch_import_core", "dispatch"}
            ]
            self.assertTrue(calls, relative_path)
            for call in calls:
                self.assertTrue(
                    all(keyword.arg is not None for keyword in call.keywords),
                    relative_path,
                )
                self.assertEqual(len(call.args), 2, relative_path)
                request_arg = call.args[0]
                if isinstance(request_arg, ast.Name):
                    request = requests_by_local.get(request_arg.id)
                    self.assertIsNotNone(request, relative_path)
                    assert request is not None
                else:
                    self.assertIsInstance(request_arg, ast.Call, relative_path)
                    assert isinstance(request_arg, ast.Call)
                    self.assertIsInstance(request_arg.func, ast.Name)
                    assert isinstance(request_arg.func, ast.Name)
                    self.assertEqual(
                        request_arg.func.id, "DispatchRequest", relative_path,
                    )
                    request = request_arg
                self.assertFalse(request.args, relative_path)
                named = {keyword.arg for keyword in request.keywords}
                self.assertNotIn(None, named, relative_path)
                self.assertIn("path", named, relative_path)
                self.assertIn(
                    "prevalidated_candidate_result", named, relative_path,
                )

    def test_production_callable_has_a_pyright_conformance_binding(self) -> None:
        source = pinned_source(Path("lib/dispatch/__init__.py"))
        self.assertIn(
            "_dispatch_core_conformance: DispatchCoreFn = dispatch_import_core",
            source,
        )


class TestDownloadCompletionOwnership(unittest.TestCase):
    def test_processing_is_only_the_completion_orchestrator(self) -> None:
        source = pinned_source(Path("lib/download_processing.py"))
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
        source = pinned_source(Path("lib/download_validation.py"))
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
