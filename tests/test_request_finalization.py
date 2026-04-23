"""Contract tests for the shared request-finalization seam."""

from __future__ import annotations

import ast
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from lib.import_dispatch import DispatchOutcome, finalize_request

REPO_ROOT = Path(__file__).resolve().parent.parent


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


class _TerminalTransitionVisitor(ast.NodeVisitor):
    """Collect direct terminal status writes in production Python modules."""

    def __init__(self, rel_path: str) -> None:
        self.rel_path = rel_path
        self.offending: list[tuple[int, str]] = []

    def visit_Call(self, node: ast.Call) -> None:
        func_name: str | None = None
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
        elif isinstance(node.func, ast.Attribute):
            func_name = node.func.attr

        if func_name not in {"apply_transition", "update_status"}:
            self.generic_visit(node)
            return

        statuses: list[str] = []
        for arg in node.args:
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                statuses.append(arg.value)
        for kw in node.keywords:
            if kw.arg == "status" and isinstance(kw.value, ast.Constant):
                if isinstance(kw.value.value, str):
                    statuses.append(kw.value.value)

        if any(status in {"wanted", "imported"} for status in statuses):
            snippet = ast.unparse(node)
            self.offending.append((node.lineno, snippet))

        self.generic_visit(node)


class TestTerminalTransitionContract(unittest.TestCase):
    """Terminal request-state writes must route through finalize_request()."""

    def test_no_direct_terminal_status_writes_in_lib_or_web(self) -> None:
        offending: list[tuple[str, int, str]] = []

        for root_name in ("lib", "web"):
            root = REPO_ROOT / root_name
            for path in sorted(root.rglob("*.py")):
                rel = path.relative_to(REPO_ROOT).as_posix()
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=rel)
                visitor = _TerminalTransitionVisitor(rel)
                visitor.visit(tree)
                for lineno, snippet in visitor.offending:
                    offending.append((rel, lineno, snippet))

        if offending:
            lines = [f"  {rel}:{lineno}: {snippet}" for rel, lineno, snippet in offending]
            self.fail(
                "Direct terminal request status writes remain in production code. "
                "Route wanted/imported transitions through lib.import_dispatch."
                "finalize_request(...).\n"
                + "\n".join(lines)
            )


if __name__ == "__main__":
    unittest.main()
