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
_GUARDED_WRITE_METHODS = frozenset({
    "record_field_resolution",
    "update_request_fields",
    "update_spectral_state",
    "update_track_artists",
})


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
) -> dict[str, tuple[ast.expr, ...]]:
    """Resolve simple module/local SQL constants visible at ``call``."""
    call_scope = _enclosing_scope(call, parents)
    values: dict[str, tuple[ast.expr, ...]] = {}
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
        grouped: dict[str, list[ast.expr]] = {}
        for _, name, value in sorted(candidates):
            grouped.setdefault(name, []).append(value)
        for name, definitions in grouped.items():
            # A local assignment shadows a module constant, but every local
            # reaching definition remains possible. This is deliberately
            # branch-conservative: an unguarded initial SQL value cannot be
            # hidden by a guarded assignment in only one branch.
            values[name] = tuple(definitions)
    return values


def _sql_variants(
    node: ast.expr,
    values: dict[str, tuple[ast.expr, ...]],
    resolving: frozenset[str] = frozenset(),
) -> tuple[set[str], bool]:
    """Return conservative SQL strings plus whether any part is unresolved."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return {node.value}, False
    if isinstance(node, (ast.Name, ast.Attribute)):
        name = node.id if isinstance(node, ast.Name) else node.attr
        if name in resolving or name not in values:
            return set(), True
        variants: set[str] = set()
        unresolved = False
        for definition in values[name]:
            found, partial = _sql_variants(
                definition, values, resolving | {name},
            )
            variants.update(found)
            unresolved = unresolved or partial
        return variants, unresolved
    if isinstance(node, ast.JoinedStr):
        variants = {""}
        unresolved = False
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                fragments = {value.value}
            elif isinstance(value, ast.FormattedValue):
                fragments, partial = _sql_variants(
                    value.value, values, resolving,
                )
                if not fragments:
                    fragments = {"{dynamic}"}
                unresolved = unresolved or partial
            else:
                fragments = {"{dynamic}"}
                unresolved = True
            variants = {
                prefix + fragment
                for prefix in variants
                for fragment in fragments
            }
        return variants, unresolved
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, left_partial = _sql_variants(node.left, values, resolving)
        right, right_partial = _sql_variants(node.right, values, resolving)
        if not left:
            left = {"{dynamic}"}
        if not right:
            right = {"{dynamic}"}
        return (
            {a + b for a in left for b in right},
            left_partial or right_partial,
        )
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "format"
    ):
        templates, partial = _sql_variants(node.func.value, values, resolving)
        if not templates:
            return set(), True
        return {
            re.sub(r"\{[^{}]*\}", "{dynamic}", template)
            for template in templates
        }, True
    return set(), True


def _is_execute_forwarder(node: ast.expr, scope: ast.AST) -> bool:
    """The DB primitive forwards SQL; its production callers are audited."""
    return (
        isinstance(node, ast.Name)
        and isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef))
        and scope.name == "_execute"
        and any(arg.arg == node.id for arg in scope.args.args)
    )


def _name_is_file_read(
    node: ast.expr,
    values: dict[str, tuple[ast.expr, ...]],
) -> bool:
    if not isinstance(node, ast.Name):
        return False
    definitions = values.get(node.id, ())
    return bool(definitions) and all(
        isinstance(definition, ast.Call)
        and isinstance(definition.func, ast.Attribute)
        and definition.func.attr == "read"
        for definition in definitions
    )


def _scope_sets_read_only(scope: ast.AST) -> bool:
    for child in ast.walk(scope):
        if not isinstance(child, ast.Call) or not child.args:
            continue
        first = child.args[0]
        if (
            isinstance(first, ast.Constant)
            and isinstance(first.value, str)
            and "default_transaction_read_only = on" in first.value.lower()
        ):
            return True
    return False


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
        values = _simple_assignments(tree, node, parents)
        variants, _unresolved = _sql_variants(node.args[0], values)
        scope = _enclosing_scope(node, parents)
        if not variants:
            if (
                _is_execute_forwarder(node.args[0], scope)
                or _name_is_file_read(node.args[0], values)
                or _scope_sets_read_only(scope)
            ):
                continue
            offending.append((
                node.lineno,
                "<unresolved dynamic SQL capable of updating album_requests>",
            ))
            continue
        for sql in sorted(variants):
            normalized = " ".join(sql.lower().split())
            if normalized.startswith("{dynamic}"):
                offending.append((node.lineno, normalized))
                continue
            if re.search(r"\bupdate\s+\{dynamic\}(?=\s|$)", normalized):
                offending.append((node.lineno, normalized))
                continue
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


def _ignored_guarded_write_results(source: str) -> list[tuple[int, str]]:
    """Find guarded writers invoked as bare expressions."""
    tree = ast.parse(source)
    parents = {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }
    ignored: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in _GUARDED_WRITE_METHODS
        ):
            continue
        if (
            node.func.attr == "record_field_resolution"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "deferred"
        ):
            # `_DeferredRecorder` is an in-memory queue whose append always
            # succeeds; only its later `pdb.record_field_resolution` flush is
            # a guarded database write.
            continue
        current: ast.AST = node
        while current in parents and not isinstance(current, ast.stmt):
            current = parents[current]
        if isinstance(current, ast.Expr):
            ignored.append((node.lineno, node.func.attr))
    return ignored


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

    def test_every_guarded_write_result_is_consumed(self):
        ignored: list[str] = []
        for root_name in PRODUCTION_ROOTS:
            for path in sorted((REPO_ROOT / root_name).rglob("*.py")):
                rel = path.relative_to(REPO_ROOT).as_posix()
                for line, method in _ignored_guarded_write_results(
                    path.read_text(encoding="utf-8"),
                ):
                    ignored.append(f"{rel}:{line}: {method}")
        self.assertEqual(
            ignored,
            [],
            "Guarded writer results must be handled explicitly:\n"
            + "\n".join(ignored),
        )

    def test_known_bad_ignored_guarded_write_is_rejected(self):
        source = '''
def thaw(db, request_id):
    db.update_request_fields(request_id, release_group_year=1999)
'''
        self.assertEqual(len(_ignored_guarded_write_results(source)), 1)

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

    def test_unknown_sql_builder_fails_closed(self):
        source = '''
def thaw(cur):
    sql = build_sql()
    cur.execute(sql)
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_unresolved_sql_parameter_fails_closed(self):
        source = '''
def thaw(cur, sql):
    cur.execute(sql)
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_dynamic_f_string_table_fails_closed(self):
        source = '''
def thaw(cur, table):
    cur.execute(f"UPDATE {table} SET current_evidence_id = NULL")
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_dynamic_format_suffix_with_known_target_fails_closed(self):
        source = '''
def thaw(cur, suffix):
    cur.execute("UPDATE album_requests SET {}".format(suffix))
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_fully_dynamic_format_statement_fails_closed(self):
        source = '''
def thaw(cur, verb, table, suffix):
    cur.execute("{} {} SET {}".format(verb, table, suffix))
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_conditional_guarded_reassignment_does_not_hide_unguarded_sql(self):
        source = '''
def thaw(cur, request_id, guarded):
    sql = "UPDATE album_requests SET current_evidence_id = %s WHERE id = %s"
    if guarded:
        sql = (
            "UPDATE album_requests SET current_evidence_id = %s "
            "WHERE id = %s AND status != 'replaced'"
        )
    cur.execute(sql, (9, request_id))
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_clearly_unrelated_dynamic_update_is_accepted(self):
        source = '''
def update_log(cur, suffix):
    cur.execute("UPDATE download_log SET {}".format(suffix))
'''
        self.assertEqual(_unguarded_album_request_updates(source), [])

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
