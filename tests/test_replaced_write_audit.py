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
        elif (
            isinstance(node, ast.AugAssign)
            and isinstance(node.target, ast.Name)
        ):
            # Retain the mutation as a reaching definition.  Referring back
            # to the same name deliberately resolves as partial/unknown;
            # ``sql = 'SELECT 1'; sql += dynamic`` must not be mistaken for
            # the original static SELECT.
            if isinstance(node.value, (ast.Name, ast.Attribute)):
                target = node.target
                value = ast.BinOp(
                    left=ast.Name(id=node.target.id, ctx=ast.Load()),
                    op=node.op,
                    right=node.value,
                )
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


def _sql_argument(node: ast.Call) -> ast.expr | None:
    """Return an execute call's SQL expression, positional or keyword."""
    if node.args:
        return node.args[0]
    for keyword in node.keywords:
        if keyword.arg in {"sql", "query"}:
            return keyword.value
    return None


def _strip_sql_comments(sql: str) -> str:
    """Remove SQL comments without treating comment text as a predicate."""
    output: list[str] = []
    index = 0
    quote: str | None = None
    while index < len(sql):
        char = sql[index]
        following = sql[index + 1] if index + 1 < len(sql) else ""
        if quote is not None:
            output.append(char)
            if char == quote:
                if following == quote:
                    output.append(following)
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if char in {"'", '"'}:
            quote = char
            output.append(char)
            index += 1
            continue
        if char == "-" and following == "-":
            index += 2
            while index < len(sql) and sql[index] not in "\r\n":
                index += 1
            output.append(" ")
            continue
        if char == "/" and following == "*":
            index += 2
            while index + 1 < len(sql):
                if sql[index] == "*" and sql[index + 1] == "/":
                    index += 2
                    break
                index += 1
            output.append(" ")
            continue
        output.append(char)
        index += 1
    return "".join(output)


_SQL_TOKEN = re.compile(
    r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|<>|!=|"
    r"[a-z_][\w$]*|\{dynamic\}|[().,;=*{}]",
    re.IGNORECASE,
)


def _sql_tokens(sql: str) -> list[tuple[str, int]]:
    """Tokenise enough PostgreSQL UPDATE syntax to correlate its guard."""
    tokens: list[tuple[str, int]] = []
    depth = 0
    for match in _SQL_TOKEN.finditer(_strip_sql_comments(sql)):
        token = match.group(0)
        if token == ")":
            depth = max(0, depth - 1)
        tokens.append((token, depth))
        if token == "(":
            depth += 1
    return tokens


def _identifier(token: str) -> str | None:
    if token.startswith("'"):
        return None
    if token.startswith('"') and token.endswith('"'):
        return token[1:-1].replace('""', '"').lower()
    if re.fullmatch(r"[a-z_][\w$]*", token, re.IGNORECASE):
        return token.lower()
    return None


def _target_status_predicate(
    tokens: list[tuple[str, int]],
    *,
    start: int,
    end: int,
    depth: int,
    target_alias: str,
    target_name: str,
) -> bool:
    """Only accept a top-level predicate on the UPDATE target's status."""
    active_values = {"'wanted'", "'downloading'", "'imported'", "'manual'"}
    index = start
    while index < end:
        token, token_depth = tokens[index]
        if token_depth != depth:
            index += 1
            continue
        left_end = index
        left_is_target = False
        if (
            _identifier(token) == "status"
            and not (
                index > start
                and tokens[index - 1] == (".", depth)
            )
        ):
            left_is_target = True
        elif index + 2 < end:
            qualifier = _identifier(token)
            dot, dot_depth = tokens[index + 1]
            column, column_depth = tokens[index + 2]
            if (
                qualifier in {target_alias, target_name}
                and dot == "."
                and dot_depth == depth
                and _identifier(column) == "status"
                and column_depth == depth
            ):
                left_is_target = True
                left_end = index + 2
        if not left_is_target or left_end + 2 >= end:
            index += 1
            continue
        operator, operator_depth = tokens[left_end + 1]
        value, value_depth = tokens[left_end + 2]
        normalized_value = value.lower()
        if operator_depth == depth and value_depth == depth:
            if operator in {"!=", "<>"} and normalized_value == "'replaced'":
                return True
            if operator == "=" and normalized_value in active_values:
                return True
        index = left_end + 1
    return False


def _album_request_update_guards(sql: str) -> list[bool]:
    """Return guard validity for every statically targeted request UPDATE."""
    tokens = _sql_tokens(sql)
    results: list[bool] = []
    for update_index, (token, depth) in enumerate(tokens):
        if token.lower() != "update":
            continue
        cursor = update_index + 1
        if cursor < len(tokens) and tokens[cursor][0].lower() == "only":
            cursor += 1
        target_parts: list[str] = []
        identifier = (
            _identifier(tokens[cursor][0])
            if cursor < len(tokens) and tokens[cursor][1] == depth
            else None
        )
        if identifier is None:
            continue
        target_parts.append(identifier)
        cursor += 1
        while (
            cursor + 1 < len(tokens)
            and tokens[cursor] == (".", depth)
            and tokens[cursor + 1][1] == depth
            and _identifier(tokens[cursor + 1][0]) is not None
        ):
            target_parts.append(_identifier(tokens[cursor + 1][0]) or "")
            cursor += 2
        target_name = target_parts[-1]
        if target_name != "album_requests":
            continue

        target_alias = target_name
        if cursor < len(tokens) and tokens[cursor][1] == depth:
            if tokens[cursor][0].lower() == "as":
                cursor += 1
                if cursor < len(tokens):
                    target_alias = _identifier(tokens[cursor][0]) or target_name
                    cursor += 1
            elif tokens[cursor][0].lower() != "set":
                possible_alias = _identifier(tokens[cursor][0])
                if possible_alias is not None:
                    target_alias = possible_alias
                    cursor += 1

        set_index = next((
            index for index in range(cursor, len(tokens))
            if tokens[index][1] == depth and tokens[index][0].lower() == "set"
        ), None)
        if set_index is None:
            results.append(False)
            continue
        statement_end = next((
            index for index in range(set_index + 1, len(tokens))
            if (
                tokens[index][1] < depth
                or (tokens[index][1] == depth and tokens[index][0] == ";")
            )
        ), len(tokens))
        where_index = next((
            index for index in range(set_index + 1, statement_end)
            if tokens[index][1] == depth and tokens[index][0].lower() == "where"
        ), None)
        results.append(
            where_index is not None
            and _target_status_predicate(
                tokens,
                start=where_index + 1,
                end=statement_end,
                depth=depth,
                target_alias=target_alias,
                target_name=target_name,
            )
        )
    return results


def _unresolved_sql_is_bounded_to_guarded_set(sql: str) -> bool:
    """Allow only the production pattern: dynamic SET, static target/WHERE."""
    normalized = " ".join(_strip_sql_comments(sql).lower().split())
    if "{dynamic}" not in normalized:
        return True
    guards = _album_request_update_guards(normalized)
    if not guards:
        # Unknown fragments in statically unrelated statements stay outside
        # this target-table audit. Direct concatenation and augmented-
        # assignment escapes are rejected from their source AST below.
        return (
            not normalized.startswith("{dynamic}")
            and not re.search(r"\bupdate\s+\{dynamic\}(?=\s|$)", normalized)
        )
    if guards != [True]:
        return False
    update_at = normalized.find("update")
    set_at = normalized.find(" set ", update_at)
    if not (update_at >= 0 and set_at >= 0):
        return False
    dynamic_positions = [
        match.start() for match in re.finditer(r"\{dynamic\}", normalized)
    ]
    return bool(dynamic_positions) and all(
        position > set_at for position in dynamic_positions
    )


def _is_unbounded_dynamic_composition(
    sql_argument: ast.expr,
    values: dict[str, tuple[ast.expr, ...]],
) -> bool:
    """Catch append/concatenation forms that can smuggle another statement."""
    if isinstance(sql_argument, ast.BinOp):
        return (
            isinstance(sql_argument.left, ast.Constant)
            and isinstance(sql_argument.left.value, str)
            and isinstance(sql_argument.right, (ast.Name, ast.Attribute))
        ) or (
            isinstance(sql_argument.right, ast.Constant)
            and isinstance(sql_argument.right.value, str)
            and isinstance(sql_argument.left, (ast.Name, ast.Attribute))
        )
    if not isinstance(sql_argument, ast.Name):
        return False
    return any(
        isinstance(definition, ast.BinOp)
        and isinstance(definition.left, ast.Name)
        and definition.left.id == sql_argument.id
        for definition in values.get(sql_argument.id, ())
    )


def _unguarded_album_request_updates(source: str) -> list[tuple[int, str]]:
    tree = ast.parse(source)
    parents = {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }
    offending: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
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
        sql_argument = _sql_argument(node)
        if sql_argument is None:
            continue
        values = _simple_assignments(tree, node, parents)
        variants, unresolved = _sql_variants(sql_argument, values)
        scope = _enclosing_scope(node, parents)
        if not variants:
            if (
                _is_execute_forwarder(sql_argument, scope)
                or _name_is_file_read(sql_argument, values)
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
            guards = _album_request_update_guards(sql)
            if guards and not all(guards):
                offending.append((node.lineno, normalized))
                continue
            if (
                unresolved
                and (
                    _is_unbounded_dynamic_composition(sql_argument, values)
                    or not _unresolved_sql_is_bounded_to_guarded_set(sql)
                )
            ):
                offending.append((node.lineno, normalized))
                continue
            if guards:
                continue
            if _UPDATE_ALBUM_REQUESTS.search(_strip_sql_comments(normalized)):
                # The lexical matcher saw the target but the bounded parser
                # could not prove which UPDATE owns it.
                offending.append((node.lineno, normalized))
    return offending


def _guarded_result_controls_handling(
    node: ast.Call,
    parents: dict[ast.AST, ast.AST],
) -> bool:
    """Prove a guarded result reaches a condition, assertion, or return."""
    current: ast.AST = node
    while current in parents and not isinstance(current, ast.stmt):
        parent = parents[current]
        if isinstance(parent, ast.Return):
            return True
        if isinstance(parent, ast.Assert) and current is parent.test:
            return True
        if isinstance(parent, (ast.If, ast.While)) and current is parent.test:
            return True
        current = parent

    statement = current
    target: ast.Name | None = None
    if (
        isinstance(statement, ast.Assign)
        and len(statement.targets) == 1
        and isinstance(statement.targets[0], ast.Name)
    ):
        target = statement.targets[0]
    elif isinstance(statement, ast.AnnAssign) and isinstance(
        statement.target, ast.Name,
    ):
        target = statement.target
    if target is None:
        return False

    scope = _enclosing_scope(node, parents)
    assigned_line = getattr(statement, "lineno", 0)
    candidate_uses = [
        use
        for use in ast.walk(scope)
        if (
            isinstance(use, ast.Name)
            and isinstance(use.ctx, ast.Load)
            and use.id == target.id
            and use.lineno > assigned_line
            and _enclosing_scope(use, parents) is scope
        )
    ]
    stores = [
        store
        for store in ast.walk(scope)
        if (
            isinstance(store, ast.Name)
            and isinstance(store.ctx, ast.Store)
            and store.id == target.id
            and store.lineno > assigned_line
            and _enclosing_scope(store, parents) is scope
        )
    ]
    for use in candidate_uses:
        if any(assigned_line < store.lineno < use.lineno for store in stores):
            continue
        current = use
        while current in parents and not isinstance(current, ast.stmt):
            parent = parents[current]
            if isinstance(parent, ast.Return):
                return True
            if isinstance(parent, ast.Assert) and current is parent.test:
                return True
            if isinstance(parent, (ast.If, ast.While)) and current is parent.test:
                return True
            current = parent
    return False


def _ignored_guarded_write_results(source: str) -> list[tuple[int, str]]:
    """Find guarded writes without proven success/conflict handling."""
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
        if not _guarded_result_controls_handling(node, parents):
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

    def test_assigned_but_unchecked_guarded_write_is_rejected(self):
        source = '''
def thaw(db, request_id):
    applied = db.update_request_fields(request_id, release_group_year=1999)
    log(applied)
'''
        self.assertEqual(len(_ignored_guarded_write_results(source)), 1)

    def test_tuple_assigned_guarded_write_is_rejected(self):
        source = '''
def thaw(db, request_id):
    applied, label = (
        db.update_request_fields(request_id, release_group_year=1999),
        "metadata",
    )
'''
        self.assertEqual(len(_ignored_guarded_write_results(source)), 1)

    def test_unchecked_walrus_guarded_write_is_rejected(self):
        source = '''
def thaw(db, request_id):
    (applied := db.update_request_fields(
        request_id, release_group_year=1999,
    ))
'''
        self.assertEqual(len(_ignored_guarded_write_results(source)), 1)

    def test_checked_assignment_and_returned_result_are_accepted(self):
        source = '''
def guarded(db, request_id):
    applied = db.update_request_fields(request_id, release_group_year=1999)
    if not applied:
        return conflict()
    return True

def forwarded(db, request_id):
    return db.update_request_fields(request_id, release_group_year=1999)
'''
        self.assertEqual(_ignored_guarded_write_results(source), [])

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

    def test_keyword_dynamic_sql_fails_closed(self):
        source = '''
def thaw(db, dynamic):
    db._execute(sql=dynamic)
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_augmented_dynamic_sql_fails_closed(self):
        source = '''
def thaw(db, dynamic_suffix):
    sql = "SELECT 1"
    sql += dynamic_suffix
    db._execute(sql)
'''
        self.assertGreaterEqual(
            len(_unguarded_album_request_updates(source)), 1,
        )

    def test_select_plus_dynamic_clause_fails_closed(self):
        source = '''
def thaw(db, dynamic_clause):
    db._execute("SELECT 1 " + dynamic_clause)
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

    def test_other_table_status_guard_does_not_guard_target(self):
        source = '''
def thaw(cur, request_id):
    cur.execute(
        "UPDATE album_requests ar SET reasoning = jobs.reason "
        "FROM jobs WHERE ar.id = %s AND jobs.status != 'replaced'",
        (request_id,),
    )
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_subquery_status_guard_does_not_guard_target(self):
        source = '''
def thaw(cur, request_id):
    cur.execute(
        "UPDATE album_requests SET reasoning = 'late' WHERE id = %s "
        "AND EXISTS (SELECT 1 FROM jobs WHERE status != 'replaced')",
        (request_id,),
    )
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_comment_status_guard_does_not_guard_target(self):
        source = '''
def thaw(cur, request_id):
    cur.execute(
        "UPDATE album_requests SET reasoning = 'late' WHERE id = %s "
        "/* status != 'replaced' */",
        (request_id,),
    )
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
