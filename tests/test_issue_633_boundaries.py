"""Structural contracts for issue #633's process-album callable seam."""

from __future__ import annotations

import ast
from pathlib import Path
import unittest


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
        source = Path("lib/download_processing.py").read_text(encoding="utf-8")
        self.assertIn(
            "_process_completed_album_conformance: ProcessAlbumFn = "
            "process_completed_album",
            source,
        )

    def test_download_seam_has_no_ellipsis_callable_escape(self) -> None:
        source = Path("lib/download.py").read_text(encoding="utf-8")
        self.assertNotIn("Callable[..., CompletionResult]", source)
        self.assertIn("process_album_fn: ProcessAlbumFn | None", source)

    def test_test_doubles_are_protocol_checked_without_broad_splats(self) -> None:
        fake_source = Path("tests/fakes/download.py").read_text(encoding="utf-8")
        self.assertIn(
            "_recorder_conformance: ProcessAlbumFn = RecordingProcessAlbum()",
            fake_source,
        )

        integration_source = Path("tests/test_integration_slices.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("process_album_fn=lambda", integration_source)
        tree = ast.parse(
            integration_source,
            filename="tests/test_integration_slices.py",
        )
        exceptional_stub = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
            and node.name == "reject_inside_process"
        )
        self.assertIsNone(exceptional_stub.args.vararg)
        self.assertIsNone(exceptional_stub.args.kwarg)


if __name__ == "__main__":
    unittest.main()
