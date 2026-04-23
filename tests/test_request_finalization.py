"""Contract tests for the shared request-finalization seam."""

from __future__ import annotations

import ast
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from lib.import_dispatch import DispatchOutcome, finalize_request, transition_request

REPO_ROOT = Path(__file__).resolve().parent.parent
PRODUCTION_ROOTS = ("lib", "web", "harness", "scripts")
PRODUCTION_FILES = ("album_source.py", "cratedigger.py")
RAW_SQL_ALLOWED_PATHS = {"lib/pipeline_db.py"}


class TestFinalizeRequest(unittest.TestCase):
    """Unit tests for the shared request-finalization seam."""

    @patch("lib.import_dispatch.apply_transition")
    def test_deferred_outcome_skips_transition(self, mock_transition: MagicMock) -> None:
        finalize_request(
            MagicMock(),
            42,
            DispatchOutcome(
                success=False,
                message="busy",
                deferred=True,
            ),
        )

        mock_transition.assert_not_called()

    @patch("lib.import_dispatch.apply_transition")
    def test_outcome_without_target_status_skips_transition(
        self,
        mock_transition: MagicMock,
    ) -> None:
        finalize_request(
            MagicMock(),
            42,
            DispatchOutcome(success=True, message="ok"),
        )

        mock_transition.assert_not_called()

    @patch("lib.import_dispatch.apply_transition")
    def test_forwards_transition_fields_and_attempt_type(
        self,
        mock_transition: MagicMock,
    ) -> None:
        db = MagicMock()

        finalize_request(
            db,
            42,
            DispatchOutcome.transition(
                to_status="wanted",
                success=False,
                message="retry",
                from_status="downloading",
                attempt_type="download",
                transition_fields={
                    "search_filetype_override": "flac,mp3 v0",
                    "min_bitrate": 245,
                },
            ),
        )

        mock_transition.assert_called_once_with(
            db,
            42,
            "wanted",
            from_status="downloading",
            attempt_type="download",
            search_filetype_override="flac,mp3 v0",
            min_bitrate=245,
        )

    @patch("lib.import_dispatch.apply_transition")
    def test_transition_request_routes_explicit_fields_through_finalize_request(
        self,
        mock_transition: MagicMock,
    ) -> None:
        db = MagicMock()

        transition_request(
            db,
            42,
            "wanted",
            from_status="downloading",
            attempt_type="download",
            search_filetype_override="flac,mp3 v0",
            min_bitrate=245,
        )

        mock_transition.assert_called_once_with(
            db,
            42,
            "wanted",
            from_status="downloading",
            attempt_type="download",
            search_filetype_override="flac,mp3 v0",
            min_bitrate=245,
        )

    @patch("lib.import_dispatch.apply_transition")
    def test_rejects_reserved_transition_fields_in_outcome(
        self,
        mock_transition: MagicMock,
    ) -> None:
        with self.assertRaisesRegex(ValueError, "reserved keys: from_status"):
            finalize_request(
                MagicMock(),
                42,
                DispatchOutcome.transition(
                    to_status="wanted",
                    success=False,
                    from_status="downloading",
                    transition_fields={"from_status": "manual"},
                ),
            )

        mock_transition.assert_not_called()

        with self.assertRaisesRegex(ValueError, "reserved keys: state_json"):
            finalize_request(
                MagicMock(),
                42,
                DispatchOutcome.transition(
                    to_status="downloading",
                    success=False,
                    transition_fields={"state_json": "{}"},
                ),
            )

        self.assertEqual(mock_transition.call_count, 0)


class _RequestStatusWriteVisitor(ast.NodeVisitor):
    """Collect request-status writes that bypass the shared finalization seam."""

    def __init__(self, rel_path: str) -> None:
        self.rel_path = rel_path
        self.offending: list[tuple[int, str, str]] = []

    def _status_arg(self, func_name: str, node: ast.Call) -> ast.expr | None:
        arg_index = 2 if func_name == "apply_transition" else 1
        if len(node.args) > arg_index:
            return node.args[arg_index]
        kw_name = "to_status" if func_name == "apply_transition" else "status"
        for kw in node.keywords:
            if kw.arg == kw_name:
                return kw.value
        return None

    def _allow_direct_transition_call(self, func_name: str, node: ast.Call) -> bool:
        if self.rel_path in {
            "lib/import_dispatch.py",
            "lib/pipeline_db.py",
            "lib/transitions.py",
        }:
            return True

        if self.rel_path != "lib/download.py":
            return False

        status_expr = self._status_arg(func_name, node)
        return (
            isinstance(status_expr, ast.Constant)
            and status_expr.value == "downloading"
        )

    def _maybe_record_raw_sql(self, node: ast.Call) -> None:
        func_name: str | None = None
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
        elif isinstance(node.func, ast.Attribute):
            func_name = node.func.attr

        if func_name not in {"execute", "_execute"}:
            return
        if self.rel_path in RAW_SQL_ALLOWED_PATHS or not node.args:
            return

        sql = node.args[0]
        if not isinstance(sql, ast.Constant) or not isinstance(sql.value, str):
            return

        normalized = " ".join(sql.value.lower().split())
        if "update album_requests" in normalized and "set status" in normalized:
            self.offending.append((
                node.lineno,
                "raw SQL status write",
                ast.unparse(node),
            ))

    def visit_Call(self, node: ast.Call) -> None:
        func_name: str | None = None
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
        elif isinstance(node.func, ast.Attribute):
            func_name = node.func.attr

        if func_name in {"apply_transition", "update_status"}:
            if not self._allow_direct_transition_call(func_name, node):
                self.offending.append((
                    node.lineno,
                    "direct transition call",
                    ast.unparse(node),
                ))
        self._maybe_record_raw_sql(node)
        self.generic_visit(node)


class TestTerminalTransitionContract(unittest.TestCase):
    """Production request-state writes must route through finalize_request()."""

    def test_no_direct_request_status_writes_outside_the_shared_seam(self) -> None:
        offending: list[tuple[str, int, str, str]] = []

        for rel_path in PRODUCTION_FILES:
            path = REPO_ROOT / rel_path
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=rel_path)
            visitor = _RequestStatusWriteVisitor(rel_path)
            visitor.visit(tree)
            for lineno, reason, snippet in visitor.offending:
                offending.append((rel_path, lineno, reason, snippet))

        for root_name in PRODUCTION_ROOTS:
            root = REPO_ROOT / root_name
            for path in sorted(root.rglob("*.py")):
                rel = path.relative_to(REPO_ROOT).as_posix()
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=rel)
                visitor = _RequestStatusWriteVisitor(rel)
                visitor.visit(tree)
                for lineno, reason, snippet in visitor.offending:
                    offending.append((rel, lineno, reason, snippet))

        if offending:
            lines = [
                f"  {rel}:{lineno}: {reason}: {snippet}"
                for rel, lineno, reason, snippet in offending
            ]
            self.fail(
                "Direct request status writes remain outside the shared seam. "
                "Route them through lib.import_dispatch.finalize_request(...).\n"
                + "\n".join(lines)
            )


if __name__ == "__main__":
    unittest.main()
