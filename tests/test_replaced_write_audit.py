"""Structural ratchet: every ``album_requests`` UPDATE freezes replacements."""

from __future__ import annotations

import ast
from dataclasses import dataclass
import hashlib
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
_GUARDED_RESULT_HANDLERS = frozenset({
    "_request_fields_applied_or_report",
    "_request_fields_applied_or_respond",
})


@dataclass(frozen=True)
class _SqlFinding:
    line: int
    sql: str
    fingerprint: str
    category: str = "unguarded"
    scope: str | None = None
    exact_source_status_cas: bool = False


@dataclass(frozen=True)
class _AlbumRequestUpdate:
    guarded: bool
    sets_status: bool
    exact_source_status_cas: bool


# Every unresolved production SQL builder is an exact, reviewed exception.
# The key includes path, source line, and a fingerprint of the execute argument
# plus all reachable local/module definitions. Moving or changing a builder
# therefore fails the audit until its bounded construction is reviewed again.
_REVIEWED_DYNAMIC_SQL_CALLS: dict[tuple[str, int, str], str] = {
    ("lib/beets_db.py", 329, "954611b895ed3d2e"): (
        "SQLite IN list is generated only from '?' value placeholders"
    ),
    ("lib/beets_db.py", 340, "bae84f633b79f37f"): (
        "SQLite IN list is generated only from '?' value placeholders"
    ),
    ("lib/beets_db.py", 569, "918c850cccb2485a"): (
        "SQLite IN list is generated only from '?' value placeholders"
    ),
    ("lib/beets_db.py", 577, "0d55058205434484"): (
        "SQLite IN list is generated only from '?' value placeholders"
    ),
    ("lib/pipeline_db/dashboard.py", 481, "26e89c7f809ee743"): (
        "dashboard WHERE and ORDER fragments come from closed enum branches"
    ),
    ("lib/pipeline_db/download_log.py", 123, "261f1904b74e1dc8"): (
        "download outcome filter appends closed literal clauses only"
    ),
    ("lib/pipeline_db/download_log.py", 430, "a33aeaa09fea2658"): (
        "request batch IN list contains only psycopg value placeholders"
    ),
    ("lib/pipeline_db/download_log.py", 512, "bd2fd6f9349d6eb2"): (
        "validation key is selected from a closed server-owned vocabulary"
    ),
    ("lib/pipeline_db/download_log.py", 564, "f16cd364862cb173"): (
        "validation key is selected from a closed server-owned vocabulary"
    ),
    ("lib/pipeline_db/download_log.py", 582, "5030ce9174c5a459"): (
        "validation key is closed vocabulary and IN list is value placeholders"
    ),
    ("lib/pipeline_db/download_log.py", 599, "bf11d54d404789b2"): (
        "JSON path key is selected from a closed server-owned vocabulary"
    ),
    ("lib/pipeline_db/import_jobs.py", 105, "22811037a85650e7"): (
        "optional job filter is a fixed literal WHERE clause"
    ),
    ("lib/pipeline_db/import_jobs.py", 166, "aa163c872b5db255"): (
        "claim exclusion predicate is assembled from fixed literal clauses"
    ),
    ("lib/pipeline_db/misc.py", 188, "4ea7fe952b4d8012"): (
        "track-count batch IN list contains only psycopg value placeholders"
    ),
    ("lib/pipeline_db/misc.py", 381, "e5c8a7e1d1b3c342"): (
        "bulk VALUES fragment contains only fixed value-placeholder tuples"
    ),
    ("lib/pipeline_db/misc.py", 555, "829107b20b4fdbd1"): (
        "triage joins and predicates are selected from closed service enums"
    ),
    ("lib/pipeline_db/requests.py", 68, "7e66fa5f6c7684d8"): (
        "INSERT columns derive from the fixed AddRequestInput schema"
    ),
    ("lib/pipeline_db/requests.py", 96, "57ae317777970714"): (
        "request batch IN list contains only psycopg value placeholders"
    ),
    ("lib/pipeline_db/requests.py", 205, "e870bfc2cb9e7b91"): (
        "release-id lookup selects one of two fixed identity predicates"
    ),
    ("lib/pipeline_db/requests.py", 443, "c34bc26f73d2723f"): (
        "internal kwargs use value placeholders and runtime-reserve lifecycle fields"
    ),
    ("lib/pipeline_db/requests.py", 449, "b9427c9081bad3cd"): (
        "internal kwargs use value placeholders and runtime-reserve lifecycle fields"
    ),
    ("lib/pipeline_db/requests.py", 689, "10f12056e4e32076"): (
        "transition SET list is produced by typed internal transition fields"
    ),
    ("lib/pipeline_db/requests.py", 827, "7889f03d4bd9a080"): (
        "rescue SET list is produced by typed internal outcome fields"
    ),
    ("lib/pipeline_db/requests.py", 939, "2d65aab3efaad95c"): (
        "atomic retry SET list is fixed and guarded by exact source status"
    ),
    ("lib/pipeline_db/requests.py", 979, "2d65aab3efaad95c"): (
        "atomic retry SET list is fixed and guarded by exact source status"
    ),
    ("lib/pipeline_db/requests.py", 1190, "bb09785d41dce4ce"): (
        "optional LIMIT is normalized through int before interpolation"
    ),
    ("lib/pipeline_db/requests.py", 1205, "2dc543213d9d7e2b"): (
        "ORDER is selected from two literals and LIMIT remains a value placeholder"
    ),
    ("lib/pipeline_db/requests.py", 1390, "c6c60c526c37fdd6"): (
        "attempt kind is validated against the fixed retry-counter vocabulary"
    ),
    ("lib/pipeline_db/search_plan.py", 949, "9c0a24417e47169d"): (
        "optional LIMIT is normalized through int before interpolation"
    ),
}


# Status changes are a narrower boundary than ordinary guarded metadata.  Each
# approved call below must live in a typed lifecycle/Replace seam and perform
# an exact compare-and-set against the source status.  Exact keys are populated
# beside the implementation they review; movement or SQL-shape drift fails the
# ratchet just like the dynamic-SQL exceptions above.
_REVIEWED_STATUS_SQL_CALLS: dict[tuple[str, int, str], str] = {
    ("lib/pipeline_db/download_log.py", 253, "b0b281d8842b0ff5"): (
        "atomic abandoned-import recovery performs downloading-to-wanted CAS"
    ),
    ("lib/pipeline_db/requests.py", 301, "1aaccc4a4f5acaaa"): (
        "Replace holds the row lock and CASes the captured active source status"
    ),
    ("lib/pipeline_db/requests.py", 689, "10f12056e4e32076"): (
        "ordinary typed transitions CAS the source status selected by the DAG"
    ),
    ("lib/pipeline_db/requests.py", 827, "7889f03d4bd9a080"): (
        "typed imported transition CASes status with rescue audit atomically"
    ),
    ("lib/pipeline_db/requests.py", 939, "2d65aab3efaad95c"): (
        "typed reset-to-wanted transition CASes its captured source status"
    ),
    ("lib/pipeline_db/requests.py", 979, "2d65aab3efaad95c"): (
        "automatic recovery accepts only downloading as its exact source"
    ),
    ("lib/pipeline_db/requests.py", 1005, "7819aab0f6b7e289"): (
        "typed download claim accepts only the explicit wanted source status"
    ),
    ("lib/pipeline_db/requests.py", 1041, "0f2e049084ae2453"): (
        "plan-aware download claim uses an exact wanted source predicate"
    ),
}


_STATUS_MUTATING_SEAMS = frozenset({
    "abandon_auto_import_request",
    "supersede_request_mbid",
    "update_status",
    "mark_imported_with_rescue",
    "reset_to_wanted",
    "reset_downloading_to_wanted",
    "set_downloading",
    "set_downloading_if_plan_current",
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
        if isinstance(node, ast.Attribute) and not (
            isinstance(node.value, ast.Name)
            and node.value.id in {"self", "cls"}
        ):
            return set(), True
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
    r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|<>|!=|%s|"
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


def _target_status_predicates(
    tokens: list[tuple[str, int]],
    *,
    start: int,
    end: int,
    depth: int,
    target_alias: str,
    target_name: str,
) -> tuple[bool, bool]:
    """Return terminal guard and exact-source CAS on the target status."""
    active_values = {"'wanted'", "'downloading'", "'imported'", "'manual'"}
    if any(
        token_depth == depth and token.lower() == "or"
        for token, token_depth in tokens[start:end]
    ):
        # A top-level OR can make a textual target guard non-constraining:
        # ``status != 'replaced' OR status = 'replaced'``.
        return False, False
    terminal_guard = False
    exact_source_status_cas = False
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
                terminal_guard = True
            if operator == "=" and (
                normalized_value in active_values or normalized_value == "%s"
            ):
                exact_source_status_cas = True
        index = left_end + 1
    return terminal_guard, exact_source_status_cas


def _set_clause_assigns_status(
    tokens: list[tuple[str, int]],
    *,
    start: int,
    end: int,
    depth: int,
) -> bool:
    """Recognise a target-column ``status = ...`` assignment in SET."""
    at_assignment_start = True
    index = start
    while index < end:
        token, token_depth = tokens[index]
        if token_depth != depth:
            index += 1
            continue
        if token == ",":
            at_assignment_start = True
            index += 1
            continue
        if at_assignment_start:
            if (
                _identifier(token) == "status"
                and index + 1 < end
                and tokens[index + 1] == ("=", depth)
            ):
                return True
            at_assignment_start = False
        index += 1
    return False


def _album_request_update_details(sql: str) -> list[_AlbumRequestUpdate]:
    """Describe guards and status mutation for every targeted UPDATE."""
    tokens = _sql_tokens(sql)
    results: list[_AlbumRequestUpdate] = []
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
            results.append(_AlbumRequestUpdate(False, False, False))
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
        sets_status = _set_clause_assigns_status(
            tokens,
            start=set_index + 1,
            end=where_index if where_index is not None else statement_end,
            depth=depth,
        )
        terminal_guard = False
        exact_source_status_cas = False
        if where_index is not None:
            terminal_guard, exact_source_status_cas = (
                _target_status_predicates(
                tokens,
                start=where_index + 1,
                end=statement_end,
                depth=depth,
                target_alias=target_alias,
                target_name=target_name,
            )
        )
        results.append(_AlbumRequestUpdate(
            guarded=terminal_guard or exact_source_status_cas,
            sets_status=sets_status,
            exact_source_status_cas=exact_source_status_cas,
        ))
    return results


def _album_request_update_guards(sql: str) -> list[bool]:
    return [detail.guarded for detail in _album_request_update_details(sql)]


def _expression_mentions_status_assignment(
    sql_argument: ast.expr,
    values: dict[str, tuple[ast.expr, ...]],
) -> bool:
    """Find status SET fragments in the SQL expression's reaching defs."""
    pending = list(ast.walk(sql_argument))
    seen: set[str] = set()
    while pending:
        node = pending.pop()
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
        ):
            lowered = node.value.lower()
            for match in re.finditer(r"\bstatus\s*=", lowered):
                before = lowered[:match.start()]
                last_set = before.rfind("set")
                last_where = before.rfind("where")
                if last_set > last_where or (
                    last_set == -1 and last_where == -1
                ):
                    return True
        name = (
            node.id if isinstance(node, ast.Name)
            else node.attr if isinstance(node, ast.Attribute)
            else None
        )
        if name is None or name in seen or name not in values:
            continue
        seen.add(name)
        for definition in values[name]:
            pending.extend(ast.walk(definition))
    return False


def _sql_call_fingerprint(
    sql_argument: ast.expr,
    values: dict[str, tuple[ast.expr, ...]],
) -> str:
    """Hash the SQL expression plus all statically reachable definitions."""
    parts = [ast.dump(sql_argument, include_attributes=False)]
    pending = list(ast.walk(sql_argument))
    seen: set[str] = set()
    while pending:
        node = pending.pop()
        name = (
            node.id if isinstance(node, ast.Name)
            else node.attr if isinstance(node, ast.Attribute)
            else None
        )
        if name is None or name in seen or name not in values:
            continue
        seen.add(name)
        definitions = values[name]
        parts.append(name)
        for definition in definitions:
            parts.append(ast.dump(definition, include_attributes=False))
            pending.extend(ast.walk(definition))
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()[:16]


def _unguarded_album_request_update_findings(source: str) -> list[_SqlFinding]:
    tree = ast.parse(source)
    parents = {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }
    offending: list[_SqlFinding] = []
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
        fingerprint = _sql_call_fingerprint(sql_argument, values)
        scope = _enclosing_scope(node, parents)
        scope_name = (
            scope.name
            if isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef))
            else None
        )
        expression_sets_status = _expression_mentions_status_assignment(
            sql_argument,
            values,
        )
        if not variants:
            if (
                _is_execute_forwarder(sql_argument, scope)
                or _name_is_file_read(sql_argument, values)
                or _scope_sets_read_only(scope)
            ):
                continue
            offending.append(_SqlFinding(
                node.lineno,
                "<unresolved dynamic SQL capable of updating album_requests>",
                fingerprint,
                category=(
                    "status_dynamic"
                    if expression_sets_status
                    or scope_name in _STATUS_MUTATING_SEAMS
                    else "dynamic"
                ),
                scope=scope_name,
            ))
            continue
        for sql in sorted(variants):
            normalized = " ".join(sql.lower().split())
            details = _album_request_update_details(sql)
            sets_status = bool(details) and (
                expression_sets_status
                or any(detail.sets_status for detail in details)
            )
            exact_source_status_cas = bool(details) and all(
                detail.exact_source_status_cas
                for detail in details
                if detail.sets_status or sets_status
            )
            if details and not all(detail.guarded for detail in details):
                offending.append(_SqlFinding(
                    node.lineno,
                    normalized,
                    fingerprint,
                    category="status" if sets_status else "unguarded",
                    scope=scope_name,
                    exact_source_status_cas=exact_source_status_cas,
                ))
                continue
            if unresolved:
                # Fail closed on every unresolved fragment. A suffix can add
                # another statement or weaken an earlier WHERE guard; its
                # location in the partial string does not make it safe.
                offending.append(_SqlFinding(
                    node.lineno,
                    normalized,
                    fingerprint,
                    category=(
                        "status_dynamic"
                        if sets_status or scope_name in _STATUS_MUTATING_SEAMS
                        else "dynamic"
                    ),
                    scope=scope_name,
                    exact_source_status_cas=exact_source_status_cas,
                ))
                continue
            if sets_status:
                offending.append(_SqlFinding(
                    node.lineno,
                    normalized,
                    fingerprint,
                    category="status",
                    scope=scope_name,
                    exact_source_status_cas=exact_source_status_cas,
                ))
                continue
            if details:
                continue
            if _UPDATE_ALBUM_REQUESTS.search(_strip_sql_comments(normalized)):
                # The lexical matcher saw the target but the bounded parser
                # could not prove which UPDATE owns it.
                offending.append(_SqlFinding(
                    node.lineno,
                    normalized,
                    fingerprint,
                    scope=scope_name,
                ))
    return offending


def _unguarded_album_request_updates(source: str) -> list[tuple[int, str]]:
    return [
        (finding.line, finding.sql)
        for finding in _unguarded_album_request_update_findings(source)
    ]


def _guarded_result_controls_handling(
    node: ast.Call,
    parents: dict[ast.AST, ast.AST],
) -> bool:
    """Prove a guarded result reaches a condition, assertion, or return."""

    def reaches_control(current: ast.AST) -> bool:
        while current in parents and not isinstance(current, ast.stmt):
            parent = parents[current]
            if isinstance(parent, ast.Call):
                handler_name = (
                    parent.func.attr
                    if isinstance(parent.func, ast.Attribute)
                    else parent.func.id
                    if isinstance(parent.func, ast.Name)
                    else None
                )
                if handler_name not in _GUARDED_RESULT_HANDLERS:
                    return False
            if isinstance(parent, ast.Return):
                return True
            if isinstance(parent, ast.Assert) and current is parent.test:
                return True
            if isinstance(parent, (ast.If, ast.While)) and current is parent.test:
                return any(
                    not isinstance(statement, ast.Pass)
                    for statement in (*parent.body, *parent.orelse)
                )
            current = parent
        return False

    current: ast.AST = node
    if reaches_control(current):
        return True
    while current in parents and not isinstance(current, ast.stmt):
        current = parents[current]

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
        if reaches_control(use):
            return True
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
        reviewed_dynamic: set[tuple[str, int, str]] = set()
        reviewed_status: set[tuple[str, int, str]] = set()
        for root_name in PRODUCTION_ROOTS:
            for path in sorted((REPO_ROOT / root_name).rglob("*.py")):
                rel = path.relative_to(REPO_ROOT).as_posix()
                for finding in _unguarded_album_request_update_findings(
                    path.read_text(encoding="utf-8")
                ):
                    key = (rel, finding.line, finding.fingerprint)
                    if finding.category.startswith("status"):
                        if (
                            not finding.exact_source_status_cas
                            or key not in _REVIEWED_STATUS_SQL_CALLS
                        ):
                            offending.append(
                                f"{rel}:{finding.line}:{finding.fingerprint}: "
                                f"{finding.category}:{finding.scope}: "
                                f"exact_source_cas="
                                f"{finding.exact_source_status_cas}: "
                                f"{finding.sql}"
                            )
                            continue
                        reviewed_status.add(key)
                        if finding.category == "status_dynamic":
                            if key not in _REVIEWED_DYNAMIC_SQL_CALLS:
                                offending.append(
                                    f"{rel}:{finding.line}:"
                                    f"{finding.fingerprint}: status builder "
                                    "also lacks dynamic-SQL review"
                                )
                                continue
                            reviewed_dynamic.add(key)
                        continue
                    if key in _REVIEWED_DYNAMIC_SQL_CALLS:
                        reviewed_dynamic.add(key)
                        continue
                    offending.append(
                        f"{rel}:{finding.line}:{finding.fingerprint}: "
                        f"{finding.sql}"
                    )
        self.assertEqual(
            offending,
            [],
            "Every album_requests UPDATE must prove the row is not replaced; "
            "status mutation additionally requires an approved typed seam "
            "with exact source-status CAS. Offending writes:\n"
            + "\n".join(offending),
        )
        self.assertEqual(
            reviewed_dynamic,
            set(_REVIEWED_DYNAMIC_SQL_CALLS),
            "Reviewed dynamic-SQL exceptions must remain exact and live",
        )
        self.assertEqual(
            reviewed_status,
            set(_REVIEWED_STATUS_SQL_CALLS),
            "Reviewed status-transition SQL calls must remain exact and live",
        )

    def test_reviewed_dynamic_sql_rationales_are_nonempty(self):
        self.assertTrue(_REVIEWED_DYNAMIC_SQL_CALLS)
        for key, rationale in _REVIEWED_DYNAMIC_SQL_CALLS.items():
            self.assertTrue(rationale.strip(), f"missing rationale for {key}")

    def test_reviewed_status_sql_rationales_are_nonempty(self):
        self.assertTrue(_REVIEWED_STATUS_SQL_CALLS)
        for key, rationale in _REVIEWED_STATUS_SQL_CALLS.items():
            self.assertTrue(rationale.strip(), f"missing rationale for {key}")

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

    def test_result_hidden_in_arbitrary_condition_call_is_rejected(self):
        source = '''
def thaw(db, request_id):
    applied = db.update_request_fields(request_id, release_group_year=1999)
    if log_and_return_true(applied):
        return True
'''
        self.assertEqual(len(_ignored_guarded_write_results(source)), 1)

    def test_pass_only_condition_is_not_conflict_handling(self):
        source = '''
def thaw(db, request_id):
    applied = db.update_request_fields(request_id, release_group_year=1999)
    if applied:
        pass
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

    def test_status_assignment_with_only_terminal_guard_is_rejected(self):
        source = """
def thaw(cur, request_id):
    cur.execute(
        "UPDATE album_requests SET status = 'manual' "
        "WHERE id = %s AND status != 'replaced'",
        (request_id,),
    )
"""
        findings = _unguarded_album_request_update_findings(source)
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].category, "status")
        self.assertFalse(findings[0].exact_source_status_cas)

    def test_unapproved_status_assignment_with_exact_cas_is_rejected(self):
        source = """
def thaw(cur, request_id, source_status):
    cur.execute(
        "UPDATE album_requests SET status = 'manual' "
        "WHERE id = %s AND status = %s",
        (request_id, source_status),
    )
"""
        findings = _unguarded_album_request_update_findings(source)
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].category, "status")
        self.assertTrue(findings[0].exact_source_status_cas)

    def test_dynamic_status_assignment_is_rejected(self):
        source = """
def thaw(cur, request_id, source_status, target_status):
    assignment = "status = " + target_status
    cur.execute(
        f"UPDATE album_requests SET {assignment} "
        "WHERE id = %s AND status = %s",
        (request_id, source_status),
    )
"""
        findings = _unguarded_album_request_update_findings(source)
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].category, "status_dynamic")
        self.assertTrue(findings[0].exact_source_status_cas)

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

    def test_placeholder_status_guard_is_accepted_for_metadata(self):
        source = '''
def thaw(cur, request_id):
    cur.execute(
        "UPDATE album_requests SET current_evidence_id = %s "
        "WHERE id = %s AND status = %s",
        (9, request_id, "imported"),
    )
'''
        self.assertEqual(_unguarded_album_request_updates(source), [])

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

    def test_augmented_builder_call_fails_closed(self):
        source = '''
def thaw(db):
    sql = "SELECT 1"
    sql += build_suffix()
    db._execute(sql)
'''
        self.assertGreaterEqual(
            len(_unguarded_album_request_updates(source)), 1,
        )

    def test_augmented_f_string_fails_closed(self):
        source = '''
def thaw(db, suffix):
    sql = "SELECT 1"
    sql += f"{suffix}"
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

    def test_select_f_string_suffix_fails_closed(self):
        source = '''
def thaw(db, suffix):
    db._execute(f"SELECT 1 {suffix}")
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_select_format_suffix_fails_closed(self):
        source = '''
def thaw(db, suffix):
    db._execute("SELECT 1 {}".format(suffix))
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_unresolved_sql_parameter_fails_closed(self):
        source = '''
def thaw(cur, sql):
    cur.execute(sql)
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_unrelated_object_attribute_does_not_resolve_module_constant(self):
        source = '''
SQL = "SELECT 1"

def thaw(db):
    db._execute(db.SQL)
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

    def test_dynamic_tail_cannot_weaken_static_guard(self):
        f_string_source = '''
def thaw(cur, request_id, suffix):
    cur.execute(
        f"UPDATE album_requests SET reasoning = 'late' "
        f"WHERE id = %s AND status != 'replaced' {suffix}",
        (request_id,),
    )
'''
        format_source = '''
def thaw(cur, request_id, suffix):
    cur.execute(
        "UPDATE album_requests SET reasoning = 'late' "
        "WHERE id = %s AND status != 'replaced' {}".format(suffix),
        (request_id,),
    )
'''
        self.assertEqual(
            len(_unguarded_album_request_updates(f_string_source)), 1,
        )
        self.assertEqual(
            len(_unguarded_album_request_updates(format_source)), 1,
        )

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

    def test_top_level_or_makes_target_guard_non_constraining(self):
        source = '''
def thaw(cur, request_id):
    cur.execute(
        "UPDATE album_requests SET reasoning = 'late' WHERE id = %s "
        "AND status != 'replaced' OR status = 'replaced'",
        (request_id,),
    )
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_unrelated_dynamic_statement_still_fails_closed(self):
        source = '''
def update_log(cur, suffix):
    cur.execute("UPDATE download_log SET {}".format(suffix))
'''
        self.assertEqual(len(_unguarded_album_request_updates(source)), 1)

    def test_static_unrelated_sql_is_accepted(self):
        source = '''
def update_log(cur, request_id):
    cur.execute(
        "UPDATE download_log SET outcome = 'failed' WHERE request_id = %s",
        (request_id,),
    )
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
