"""Contract tests for the shared request-finalization seam."""

from __future__ import annotations

import ast
import unittest
from pathlib import Path
from typing import Any, cast

from lib.dispatch import DispatchOutcome
from lib.transitions import RequestTransition, TransitionApplied, finalize_request
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row

REPO_ROOT = Path(__file__).resolve().parent.parent
PRODUCTION_ROOTS = ("lib", "web", "harness", "scripts")
PRODUCTION_FILES = ("album_source.py", "cratedigger.py")


def _is_pipeline_db_seam(rel_path: str) -> bool:
    """The PostgreSQL persistence layer is the allowed home for raw request-
    status SQL and direct transition calls. Originally one file
    (``lib/pipeline_db.py``); decomposed into the ``lib/pipeline_db/`` package
    of cluster mixins (#379). Either form is the seam."""
    return rel_path == "lib/pipeline_db.py" or rel_path.startswith("lib/pipeline_db/")


def _ignored_finalize_request_calls(tree: ast.AST) -> list[ast.Call]:
    aliases = {"finalize_request"}
    if isinstance(tree, ast.Module):
        for node in tree.body:
            if not isinstance(node, ast.ImportFrom):
                continue
            if node.module != "lib.transitions":
                continue
            for imported in node.names:
                if imported.name == "finalize_request":
                    aliases.add(imported.asname or imported.name)

    ignored: list[ast.Call] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Expr):
            continue
        call = node.value
        if not isinstance(call, ast.Call):
            continue
        if isinstance(call.func, ast.Name) and call.func.id in aliases:
            ignored.append(call)
        elif (
            isinstance(call.func, ast.Attribute)
            and call.func.attr == "finalize_request"
        ):
            ignored.append(call)
    return ignored


class TestDispatchOutcomeSummary(unittest.TestCase):
    """Import summaries must not carry request-transition commands."""

    def test_dispatch_outcome_has_no_transition_command_fields(self) -> None:
        outcome = DispatchOutcome(success=True, message="ok")

        self.assertFalse(hasattr(outcome, "target_status"))
        self.assertFalse(hasattr(outcome, "from_status"))
        self.assertFalse(hasattr(outcome, "attempt_type"))
        self.assertFalse(hasattr(outcome, "transition_fields"))
        self.assertFalse(hasattr(DispatchOutcome, "transition"))


class TestFinalizeRequest(unittest.TestCase):
    """Unit tests for the shared request-finalization seam."""

    def test_forwards_transition_fields_and_attempt_type(self) -> None:
        """``finalize_request`` writes through to the DB so the row reflects
        the transition. Asserts on resulting DB state instead of mock call
        args — the contract is "the row got these fields", not "this Python
        function got these positional args."

        ``prev_min_bitrate`` is intentionally not passed: the production
        ``reset_downloading_to_wanted`` derives it via
        ``prev_min_bitrate = COALESCE(min_bitrate, prev_min_bitrate)``,
        i.e. the previous ``min_bitrate`` becomes ``prev_min_bitrate``.
        Seeding with ``min_bitrate=320`` and transitioning to
        ``min_bitrate=245`` exercises that derivation end-to-end.
        """
        db = FakePipelineDB()
        db.seed_request(
            make_request_row(
                id=42,
                status="downloading",
                search_filetype_override=None,
                min_bitrate=320,
                prev_min_bitrate=None,
            ),
        )

        result = finalize_request(
            cast(Any, db),
            42,
            RequestTransition.to_wanted(
                from_status="downloading",
                attempt_type="download",
                search_filetype_override="flac,mp3 v0",
                min_bitrate=245,
            ),
        )

        self.assertIsInstance(result, TransitionApplied)
        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], "flac,mp3 v0")
        self.assertEqual(row["min_bitrate"], 245)
        self.assertEqual(row["prev_min_bitrate"], 320)
        # attempt_type="download" → an attempt counter advanced
        self.assertEqual(row["download_attempts"], 1)

    def test_rejects_target_specific_wrong_fields_at_construction(self) -> None:
        """RequestTransition rejects target-specific kwargs at construction
        time, before any DB call. The previous version of this test patched
        ``apply_transition`` to verify it wasn't reached — but the
        construction failure happens upstream of ``finalize_request``
        entirely, so the patch was provably redundant.
        """
        with self.assertRaises(TypeError):
            RequestTransition.to_wanted(beets_distance=0.12)  # type: ignore[call-arg]


class _RequestStatusWriteVisitor(ast.NodeVisitor):
    """Collect request-status writes that bypass the shared finalization seam."""

    def __init__(
        self,
        rel_path: str,
        module_string_constants: dict[str, str] | None = None,
        transition_aliases: dict[str, str] | None = None,
    ) -> None:
        self.rel_path = rel_path
        self.module_string_constants = module_string_constants or {}
        self.transition_aliases = transition_aliases or {}
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

    def _resolve_status_value(self, expr: ast.expr | None) -> str | None:
        if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
            return expr.value
        if isinstance(expr, ast.Name):
            return self.module_string_constants.get(expr.id)
        return None

    def _allow_direct_transition_call(self, func_name: str, node: ast.Call) -> bool:
        if func_name == "publish_initialized_request":
            return self.rel_path in {
                "lib/transitions.py",
                "lib/request_creation_service.py",
            }
        if _is_pipeline_db_seam(self.rel_path) or self.rel_path == "lib/transitions.py":
            return True

        return False

    def _maybe_record_raw_sql(self, node: ast.Call) -> None:
        func_name: str | None = None
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
        elif isinstance(node.func, ast.Attribute):
            func_name = node.func.attr

        if func_name not in {"execute", "_execute"}:
            return
        if _is_pipeline_db_seam(self.rel_path) or not node.args:
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
            func_name = self.transition_aliases.get(node.func.id, node.func.id)
        elif isinstance(node.func, ast.Attribute):
            func_name = node.func.attr

        if func_name in {
            "apply_transition",
            "reset_downloading_to_wanted",
            "reset_to_wanted",
            "set_downloading",
            "update_status",
            "publish_initialized_request",
        }:
            if not self._allow_direct_transition_call(func_name, node):
                self.offending.append((
                    node.lineno,
                    "direct transition call",
                    ast.unparse(node),
                ))
        self._maybe_record_raw_sql(node)
        self.generic_visit(node)


class TestTerminalTransitionContract(unittest.TestCase):
    """Production request-state writes must route through lib.transitions."""

    def test_no_direct_request_status_writes_outside_the_shared_seam(self) -> None:
        offending: list[tuple[str, int, str, str]] = []

        for rel_path in PRODUCTION_FILES:
            path = REPO_ROOT / rel_path
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=rel_path)
            visitor = _RequestStatusWriteVisitor(
                rel_path,
                _module_string_constants(tree),
                _transition_aliases(tree),
            )
            visitor.visit(tree)
            for lineno, reason, snippet in visitor.offending:
                offending.append((rel_path, lineno, reason, snippet))

        for root_name in PRODUCTION_ROOTS:
            root = REPO_ROOT / root_name
            for path in sorted(root.rglob("*.py")):
                rel = path.relative_to(REPO_ROOT).as_posix()
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=rel)
                visitor = _RequestStatusWriteVisitor(
                    rel,
                    _module_string_constants(tree),
                    _transition_aliases(tree),
                )
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
                "Route them through lib.transitions.finalize_request(...).\n"
                + "\n".join(lines)
            )

    def test_initialization_publication_is_reserved_to_creation_service(self) -> None:
        allowed = ast.parse(
            "transitions.publish_initialized_request(db, 1, fields={})",
        )
        service_visitor = _RequestStatusWriteVisitor(
            "lib/request_creation_service.py", {}, {},
        )
        service_visitor.visit(allowed)
        self.assertEqual(service_visitor.offending, [])

        unrelated_visitor = _RequestStatusWriteVisitor(
            "lib/download.py", {}, {},
        )
        unrelated_visitor.visit(allowed)
        self.assertEqual(
            unrelated_visitor.offending[0][1],
            "direct transition call",
        )

    def test_every_production_finalize_request_result_is_consumed(self) -> None:
        offending: list[tuple[str, int, str]] = []

        def scan(rel_path: str) -> None:
            path = REPO_ROOT / rel_path
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=rel_path)
            for call in _ignored_finalize_request_calls(tree):
                offending.append((rel_path, call.lineno, ast.unparse(call)))

        for rel_path in PRODUCTION_FILES:
            scan(rel_path)
        for root_name in PRODUCTION_ROOTS:
            for path in sorted((REPO_ROOT / root_name).rglob("*.py")):
                scan(path.relative_to(REPO_ROOT).as_posix())

        self.assertEqual(
            offending,
            [],
            "Every finalize_request caller must consume its typed result; "
            f"ignored calls: {offending}",
        )

    def test_finalize_result_audit_rejects_known_bad_ignored_call(self) -> None:
        tree = ast.parse(
            "from lib.transitions import finalize_request as finish\n"
            "finish(db, 42, transition)\n"
        )
        ignored = _ignored_finalize_request_calls(tree)

        self.assertEqual(len(ignored), 1)


def _module_string_constants(tree: ast.AST) -> dict[str, str]:
    constants: dict[str, str] = {}
    if not isinstance(tree, ast.Module):
        return constants

    for node in tree.body:
        if isinstance(node, ast.Assign):
            if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
                continue
            if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                constants[node.targets[0].id] = node.value.value
        elif isinstance(node, ast.AnnAssign):
            if not isinstance(node.target, ast.Name):
                continue
            if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                constants[node.target.id] = node.value.value

    return constants


def _transition_aliases(tree: ast.AST) -> dict[str, str]:
    aliases: dict[str, str] = {}
    if not isinstance(tree, ast.Module):
        return aliases

    for node in tree.body:
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.module != "lib.transitions":
            continue
        for imported in node.names:
            if imported.name != "apply_transition":
                continue
            aliases[imported.asname or imported.name] = imported.name

    return aliases


class TestRequestStatusWriteVisitor(unittest.TestCase):
    def test_rejects_module_constant_for_downloading_in_download_module(self) -> None:
        tree = ast.parse(
            "STATUS_DOWNLOADING = 'downloading'\n"
            "apply_transition(db, 42, STATUS_DOWNLOADING)\n"
        )
        visitor = _RequestStatusWriteVisitor(
            "lib/download.py",
            _module_string_constants(tree),
            _transition_aliases(tree),
        )

        visitor.visit(tree)

        self.assertEqual(len(visitor.offending), 1)

    def test_rejects_non_downloading_module_constant_in_download_module(self) -> None:
        tree = ast.parse(
            "STATUS_UNSEARCHABLE = 'unsearchable'\n"
            "apply_transition(db, 42, STATUS_UNSEARCHABLE)\n"
        )
        visitor = _RequestStatusWriteVisitor(
            "lib/download.py",
            _module_string_constants(tree),
            _transition_aliases(tree),
        )

        visitor.visit(tree)

        self.assertEqual(len(visitor.offending), 1)

    def test_rejects_aliased_apply_transition_import(self) -> None:
        tree = ast.parse(
            "from lib.transitions import apply_transition as _do_transition\n"
            "_do_transition(db, 42, 'wanted')\n"
        )
        visitor = _RequestStatusWriteVisitor(
            "web/routes/pipeline.py",
            _module_string_constants(tree),
            _transition_aliases(tree),
        )

        visitor.visit(tree)

        self.assertEqual(len(visitor.offending), 1)

    def test_rejects_aliased_apply_transition_for_downloading_in_download_module(self) -> None:
        tree = ast.parse(
            "from lib.transitions import apply_transition as _do_transition\n"
            "STATUS_DOWNLOADING = 'downloading'\n"
            "_do_transition(db, 42, STATUS_DOWNLOADING)\n"
        )
        visitor = _RequestStatusWriteVisitor(
            "lib/download.py",
            _module_string_constants(tree),
            _transition_aliases(tree),
        )

        visitor.visit(tree)

        self.assertEqual(len(visitor.offending), 1)

    def test_rejects_direct_reset_to_wanted_call(self) -> None:
        tree = ast.parse("db.reset_to_wanted(42)\n")
        visitor = _RequestStatusWriteVisitor(
            "lib/download.py",
            _module_string_constants(tree),
            _transition_aliases(tree),
        )

        visitor.visit(tree)

        self.assertEqual(len(visitor.offending), 1)

    def test_rejects_direct_set_downloading_call(self) -> None:
        tree = ast.parse('db.set_downloading(42, "{}")\n')
        visitor = _RequestStatusWriteVisitor(
            "lib/download.py",
            _module_string_constants(tree),
            _transition_aliases(tree),
        )

        visitor.visit(tree)

        self.assertEqual(len(visitor.offending), 1)

    def test_rejects_direct_reset_downloading_to_wanted_call(self) -> None:
        tree = ast.parse("db.reset_downloading_to_wanted(42)\n")
        visitor = _RequestStatusWriteVisitor(
            "lib/download.py",
            _module_string_constants(tree),
            _transition_aliases(tree),
        )

        visitor.visit(tree)

        self.assertEqual(len(visitor.offending), 1)


if __name__ == "__main__":
    unittest.main()
