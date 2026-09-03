"""Structural contracts for issue #633's process-album callable seam."""

from __future__ import annotations

import ast
import unittest
from pathlib import Path

from tests._source_pins import pinned_source


def _annotation(node: ast.expr | None) -> str | None:
    return ast.unparse(node) if node is not None else None


class TestProcessAlbumProtocolBoundary(unittest.TestCase):
    def test_protocol_exactly_matches_process_completed_album(self) -> None:
        source = Path("lib/download_processing.py").read_text(encoding="utf-8")
        tree = ast.parse(source, filename="lib/download_processing.py")
        protocol = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == "ProcessAlbumFn"
        )
        call = next(
            node
            for node in protocol.body
            if isinstance(node, ast.FunctionDef) and node.name == "__call__"
        )
        production = next(
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "process_completed_album"
        )

        self.assertEqual([_annotation(base) for base in protocol.bases], ["Protocol"])
        self.assertIsNone(call.args.vararg)
        self.assertIsNone(call.args.kwarg)
        self.assertIsNone(production.args.vararg)
        self.assertIsNone(production.args.kwarg)
        self.assertEqual(
            [argument.arg for argument in call.args.args],
            ["self", "album_data", "ctx"],
        )
        self.assertEqual(
            [_annotation(argument.annotation) for argument in call.args.args],
            [None, "GrabListEntry", "CratediggerContext"],
        )
        self.assertEqual(
            [argument.arg for argument in call.args.kwonlyargs],
            [
                "import_job_id",
                "validate_fn",
                "handle_valid_fn",
                "dispatch_fn",
                "materialize_before_file_copy",
                "materialize_fn",
                "cancellation_token",
                "owner_proof",
            ],
        )
        self.assertEqual(
            [_annotation(argument.annotation) for argument in call.args.kwonlyargs],
            [
                "int",
                "download_validation.ValidateFn | None",
                "download_validation.HandleValidFn | None",
                "DispatchCoreFn | None",
                "Callable[[], None] | None",
                "Callable[..., download_materialization.MaterializeResult] | None",
                "CancellationToken | None",
                "ExecutionOwnerProof | None",
            ],
        )
        self.assertEqual(_annotation(call.returns), "CompletionResult")
        self.assertEqual(
            [argument.arg for argument in call.args.args[1:]],
            [argument.arg for argument in production.args.args],
        )
        self.assertEqual(
            [_annotation(argument.annotation) for argument in call.args.args[1:]],
            [_annotation(argument.annotation) for argument in production.args.args],
        )
        self.assertEqual(
            [argument.arg for argument in call.args.kwonlyargs],
            [argument.arg for argument in production.args.kwonlyargs],
        )
        self.assertEqual(
            [_annotation(argument.annotation) for argument in call.args.kwonlyargs],
            [_annotation(argument.annotation) for argument in production.args.kwonlyargs],
        )
        self.assertEqual(
            [_annotation(default) for default in call.args.kw_defaults],
            [_annotation(default) for default in production.args.kw_defaults],
        )
        self.assertEqual(_annotation(call.returns), _annotation(production.returns))

    def test_production_function_has_pyright_conformance_binding(self) -> None:
        source = pinned_source(Path("lib/download_processing.py"))
        self.assertIn(
            "_process_completed_album_conformance: ProcessAlbumFn = "
            "process_completed_album",
            source,
        )

    def test_download_seam_has_no_ellipsis_callable_escape(self) -> None:
        source = pinned_source(Path("lib/download.py"))
        self.assertNotIn("Callable[..., CompletionResult]", source)
        self.assertIn("process_album_fn: ProcessAlbumFn | None", source)

    def test_test_double_is_protocol_checked_without_broad_splats(self) -> None:
        fake_source = pinned_source(Path("tests/fakes/download.py"))
        self.assertIn(
            "_recorder_conformance: ProcessAlbumFn = RecordingProcessAlbum()",
            fake_source,
        )
        tree = ast.parse(
            fake_source,
            filename="tests/fakes/download.py",
        )
        recorder = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef)
            and node.name == "RecordingProcessAlbum"
        )
        call = next(
            node
            for node in recorder.body
            if isinstance(node, ast.FunctionDef) and node.name == "__call__"
        )
        self.assertIsNone(call.args.vararg)
        self.assertIsNone(call.args.kwarg)

        terminal_source = Path("tests/test_terminal_outcomes.py").read_text(
            encoding="utf-8",
        )
        terminal_tree = ast.parse(
            terminal_source,
            filename="tests/test_terminal_outcomes.py",
        )
        helper = next(
            node
            for node in ast.walk(terminal_tree)
            if isinstance(node, ast.FunctionDef)
            and node.name == "_run_job_backed_automation_result"
        )
        for function in (
            node
            for node in ast.walk(helper)
            if isinstance(node, ast.FunctionDef)
        ):
            self.assertIsNone(function.args.vararg)
            self.assertIsNone(function.args.kwarg)

        injection = next(
            node
            for node in ast.walk(helper)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "execute_automation_import_job"
        )
        injected = {
            keyword.arg: _annotation(keyword.value)
            for keyword in injection.keywords
        }
        self.assertEqual(injected["process_album_fn"], "process_album")
        self.assertEqual(injected["execution_lease"], "execution_lease")
        self.assertEqual(injected["cancellation_token"], "cancellation_token")
        self.assertEqual(
            injected["owner_session_identity"],
            "owner_session_identity",
        )


if __name__ == "__main__":
    unittest.main()
