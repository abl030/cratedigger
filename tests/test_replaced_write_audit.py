"""Structural ratchet: every ``album_requests`` UPDATE freezes replacements."""

from __future__ import annotations

import ast
from pathlib import Path
import re
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_ROOTS = ("lib", "scripts", "web")
_SCOPE_NODES = (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)
_SQL_IDENT = r'(?:"[^"]+"|[a-z_][\w$]*)'
_UPDATE_ALBUM_REQUESTS = re.compile(
    rf"\bupdate\s+(?:{_SQL_IDENT}\s*\.\s*)?"
    r'(?:"album_requests"|album_requests)(?=\s|$)',
    re.IGNORECASE,
)
_STATUS_REF = (
    rf'(?<![\w$])(?:{_SQL_IDENT}\s*\.\s*)?'
    r'(?:"status"|status)(?![\w$])'
)


def _enclosing_scope(
    node: ast.AST,
    parents: dict[ast.AST, ast.AST],
) -> ast.AST:
    current = node
    while not isinstance(current, _SCOPE_NODES):
        current = parents[current]
    return current


def _simple_assignments(
    tree: ast.Module,
    call: ast.Call,
    parents: dict[ast.AST, ast.AST],
) -> dict[str, ast.expr]:
    """Resolve simple module/local SQL constants visible at ``call``."""
    call_scope = _enclosing_scope(call, parents)
    values: dict[str, ast.expr] = {}
    module_candidates: list[tuple[int, str, ast.expr]] = []
    local_candidates: list[tuple[int, str, ast.expr]] = []
    for node in ast.walk(tree):
        target: ast.expr | None = None
        value: ast.expr | None = None
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target, value = node.targets[0], node.value
        elif isinstance(node, ast.AnnAssign):
            target, value = node.target, node.value
        if not isinstance(target, ast.Name) or value is None:
            continue
        scope = _enclosing_scope(node, parents)
        line = getattr(node, "lineno", 0)
        if scope is tree:
            module_candidates.append((line, target.id, value))
        elif scope is call_scope and line < call.lineno:
            local_candidates.append((line, target.id, value))
    for candidates in (module_candidates, local_candidates):
        for _, name, value in sorted(candidates):
            values[name] = value
    return values


def _static_sql(
    node: ast.expr,
    values: dict[str, ast.expr],
    resolving: frozenset[str] = frozenset(),
) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        if node.id in resolving or node.id not in values:
            return None
        return _static_sql(
            values[node.id],
            values,
            resolving | {node.id},
        )
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
            elif isinstance(value, ast.FormattedValue):
                resolved = _static_sql(value.value, values, resolving)
                parts.append(resolved if resolved is not None else "{dynamic}")
            else:
                return None
        return "".join(parts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _static_sql(node.left, values, resolving)
        right = _static_sql(node.right, values, resolving)
        if left is None and right is None:
            return None
        return (left or "{dynamic}") + (right or "{dynamic}")
    return None


def _unguarded_album_request_updates(source: str) -> list[tuple[int, str]]:
    tree = ast.parse(source)
    parents = {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }
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
        sql = _static_sql(
            node.args[0],
            _simple_assignments(tree, node, parents),
        )
        if sql is None:
            continue
        normalized = " ".join(sql.lower().split())
        if not _UPDATE_ALBUM_REQUESTS.search(normalized):
            continue
        _, separator, where_clause = normalized.partition(" where ")
        if not separator:
            offending.append((node.lineno, normalized))
            continue
        has_terminal_guard = bool(re.search(
            rf"{_STATUS_REF}\s*(?:!=|<>)\s*'replaced'",
            where_clause,
            re.IGNORECASE,
        ))
        has_exact_active_guard = bool(re.search(
            rf"{_STATUS_REF}\s*=\s*'(?:wanted|downloading|imported|manual)'",
            where_clause,
            re.IGNORECASE,
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

    def test_local_sql_variable_is_resolved(self):
        source = '''
def thaw(cur, request_id):
    sql = "UPDATE album_requests SET current_evidence_id = %s WHERE id = %s"
    cur.execute(sql, (9, request_id))
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_module_sql_constant_concatenation_is_resolved(self):
        source = '''
PREFIX = "UPDATE album_requests SET "
SQL = PREFIX + "current_evidence_id = %s WHERE id = %s"

def thaw(cur, request_id):
    cur.execute(SQL, (9, request_id))
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_schema_qualified_quoted_update_is_rejected(self):
        source = '''
def thaw(cur, request_id):
    cur.execute(
        'UPDATE "public"."album_requests" SET current_evidence_id = %s '
        'WHERE id = %s',
        (9, request_id),
    )
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_placeholder_status_guard_is_rejected(self):
        source = '''
def thaw(cur, request_id):
    cur.execute(
        "UPDATE album_requests SET current_evidence_id = %s "
        "WHERE id = %s AND status = %s",
        (9, request_id, "imported"),
    )
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_partially_dynamic_sql_with_known_target_fails_closed(self):
        source = '''
def thaw(cur, request_id):
    suffix = build_suffix()
    sql = "UPDATE album_requests " + suffix
    cur.execute(sql, (request_id,))
'''
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

    def test_terminal_guard_with_qualified_status_is_accepted(self):
        source = """
def guarded(cur, request_id):
    cur.execute(
        "UPDATE public.album_requests SET current_evidence_id = %s "
        "WHERE id = %s AND album_requests.status <> 'replaced'",
        (9, request_id),
    )
"""
        self.assertEqual(_unguarded_album_request_updates(source), [])


if __name__ == "__main__":
    unittest.main()
