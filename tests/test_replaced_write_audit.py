"""Structural ratchet: every ``album_requests`` UPDATE freezes replacements."""

from __future__ import annotations

import ast
from pathlib import Path
import re
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_ROOTS = ("lib", "scripts", "web")


def _static_sql(node: ast.expr) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
            elif isinstance(value, ast.FormattedValue):
                parts.append("{dynamic}")
            else:
                return None
        return "".join(parts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _static_sql(node.left)
        right = _static_sql(node.right)
        if left is not None and right is not None:
            return left + right
    return None


def _unguarded_album_request_updates(source: str) -> list[tuple[int, str]]:
    tree = ast.parse(source)
    offending: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        name = (
            node.func.attr
            if isinstance(node.func, ast.Attribute)
            else node.func.id
            if isinstance(node.func, ast.Name)
            else None
        )
        if name not in {"execute", "_execute"}:
            continue
        sql = _static_sql(node.args[0])
        if sql is None:
            continue
        normalized = " ".join(sql.lower().split())
        if "update album_requests" not in normalized:
            continue
        _, separator, where_clause = normalized.partition(" where ")
        if not separator:
            offending.append((node.lineno, normalized))
            continue
        has_terminal_guard = bool(re.search(
            r"\bstatus\s*(?:!=|<>)\s*'replaced'",
            where_clause,
        ))
        has_exact_active_guard = bool(re.search(
            r"\bstatus\s*=\s*(?:%s|'(?:wanted|downloading|imported|manual)')",
            where_clause,
        ))
        if not (has_terminal_guard or has_exact_active_guard):
            offending.append((node.lineno, normalized))
    return offending


class TestReplacedWriteAudit(unittest.TestCase):
    def test_every_album_request_update_has_terminal_or_exact_status_guard(self):
        offending: list[str] = []
        for root_name in PRODUCTION_ROOTS:
            for path in sorted((REPO_ROOT / root_name).rglob("*.py")):
                rel = path.relative_to(REPO_ROOT).as_posix()
                for line, sql in _unguarded_album_request_updates(
                    path.read_text(encoding="utf-8")
                ):
                    offending.append(f"{rel}:{line}: {sql}")
        self.assertEqual(
            offending,
            [],
            "Every album_requests UPDATE must prove the row is not replaced; "
            "unguarded writes:\n" + "\n".join(offending),
        )

    def test_known_bad_unguarded_update_is_rejected(self):
        source = """
def thaw(cur, request_id):
    cur.execute(
        \"UPDATE album_requests SET current_evidence_id = %s WHERE id = %s\",
        (9, request_id),
    )
"""
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_status_assignment_does_not_masquerade_as_where_guard(self):
        source = """
def thaw(cur, request_id):
    cur.execute(
        "UPDATE album_requests SET status = %s WHERE id = %s",
        ("wanted", request_id),
    )
"""
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_exact_active_status_guard_is_accepted(self):
        source = """
def guarded(cur, request_id):
    cur.execute(
        \"UPDATE album_requests SET current_evidence_id = %s \"
        \"WHERE id = %s AND status = 'imported'\",
        (9, request_id),
    )
"""
        self.assertEqual(_unguarded_album_request_updates(source), [])


if __name__ == "__main__":
    unittest.main()
