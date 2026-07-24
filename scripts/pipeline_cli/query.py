"""pipeline-cli ``query`` command (#495 carve).

Raw SQL defaults to a bounded read-only transaction. Writes require the
deliberate ``--write --confirm WRITE`` escape hatch.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, time
from decimal import Decimal
from typing import ContextManager, Mapping, Optional, Protocol

import psycopg2

from lib.pipeline_db._core import ReadOnlyQueryCursor
from scripts.pipeline_cli._format import _json_default


def _stringify_query_value(value: object) -> str:
    """Format a SQL value for table output."""
    if value is None:
        return "NULL"
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=_json_default, sort_keys=True)
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def _render_query_table(
    rows: list[Mapping[str, object]], columns: list[str],
) -> list[str]:
    """Render SQL query results as a simple aligned table."""
    widths = {col: len(col) for col in columns}
    string_rows: list[list[str]] = []

    for row in rows:
        rendered: list[str] = []
        for col in columns:
            text = _stringify_query_value(row.get(col))
            widths[col] = max(widths[col], len(text))
            rendered.append(text)
        string_rows.append(rendered)

    header = " | ".join(col.ljust(widths[col]) for col in columns)
    divider = "-+-".join("-" * widths[col] for col in columns)
    lines = [header, divider]
    for rendered in string_rows:
        lines.append(" | ".join(
            value.ljust(widths[col]) for col, value in zip(columns, rendered)
        ))
    row_label = "row" if len(rows) == 1 else "rows"
    lines.append(f"({len(rows)} {row_label})")
    return lines


def _get_query_sql(args: argparse.Namespace) -> str:
    """Resolve SQL text from argv or stdin."""
    sql = sys.stdin.read() if args.sql == "-" else args.sql
    sql = sql.strip()
    if not sql:
        raise ValueError("No SQL provided.")
    return sql


def _read_only_sql(sql: str) -> str:
    """Accept exactly one lexical SQL statement for the read-only scope.

    This deliberately recognises only SQL quoting/comment boundaries, not SQL
    meaning.  A top-level semicolon may terminate the one statement; quoted
    strings, quoted identifiers, dollar strings, and comments may contain
    semicolons normally.  Once a statement terminates, only whitespace or
    comments may follow, preventing ``SET TRANSACTION READ WRITE; ...`` from
    escaping the transaction boundary before any database call.
    """
    error = (
        "Read-only query mode accepts one statement only; use "
        "--write --confirm WRITE for intentional write SQL.")
    index = 0
    terminated = False
    state = "normal"
    block_depth = 0
    dollar_delimiter = ""

    while index < len(sql):
        char = sql[index]

        if state == "standard_string":
            if char == "'" and index + 1 < len(sql) and sql[index + 1] == "'":
                index += 2
            elif char == "'":
                state = "normal"
                index += 1
            else:
                index += 1
            continue

        if state == "escape_string":
            if char == "\\" and index + 1 < len(sql):
                index += 2
            elif char == "'" and index + 1 < len(sql) and sql[index + 1] == "'":
                index += 2
            elif char == "'":
                state = "normal"
                index += 1
            else:
                index += 1
            continue

        if state == "double_quote":
            if char == '"' and index + 1 < len(sql) and sql[index + 1] == '"':
                index += 2
            elif char == '"':
                state = "normal"
                index += 1
            else:
                index += 1
            continue

        if state == "line_comment":
            if char in "\r\n":
                state = "normal"
            index += 1
            continue

        if state == "block_comment":
            if sql.startswith("/*", index):
                block_depth += 1
                index += 2
            elif sql.startswith("*/", index):
                block_depth -= 1
                index += 2
                if block_depth == 0:
                    state = "normal"
            else:
                index += 1
            continue

        if state == "dollar_quote":
            if sql.startswith(dollar_delimiter, index):
                index += len(dollar_delimiter)
                state = "normal"
            else:
                index += 1
            continue

        # Normal SQL text.
        if char.isspace():
            index += 1
        elif sql.startswith("--", index):
            state = "line_comment"
            index += 2
        elif sql.startswith("/*", index):
            state = "block_comment"
            block_depth = 1
            index += 2
        elif terminated:
            raise ValueError(error)
        elif (
            char in "eE"
            and index + 1 < len(sql)
            and sql[index + 1] == "'"
            and (index == 0 or not _is_sql_identifier_char(sql[index - 1]))
        ):
            # PostgreSQL only gives E/e its escape-string meaning at a token
            # boundary. ``nameE'...'`` is not an E string and must retain the
            # ordinary-string scanner semantics below.
            state = "escape_string"
            index += 2
        elif char == "'":
            state = "standard_string"
            index += 1
        elif char == '"':
            state = "double_quote"
            index += 1
        elif char == ";":
            terminated = True
            index += 1
        elif char == "$" and (
            index == 0 or not _is_sql_identifier_char(sql[index - 1])
        ):
            end = index + 1
            if end < len(sql) and sql[end] == "$":
                dollar_delimiter = "$$"
                state = "dollar_quote"
                index += 2
            elif end < len(sql) and (sql[end].isalpha() or sql[end] == "_"):
                end += 1
                while end < len(sql) and (sql[end].isalnum() or sql[end] == "_"):
                    end += 1
                if end < len(sql) and sql[end] == "$":
                    dollar_delimiter = sql[index:end + 1]
                    state = "dollar_quote"
                    index = end + 1
                else:
                    index += 1
            else:
                index += 1
        else:
            index += 1

    return sql


def _is_sql_identifier_char(char: str) -> bool:
    """Return whether *char* prevents an ``E'...'`` token boundary."""
    return char.isalnum() or char in "_$"


class _QueryDB(Protocol):
    """Small raw-query seam: writes use ``_execute``; default reads use a
    pinned, non-retrying connection scope."""

    def _execute(self, sql: str) -> ReadOnlyQueryCursor: ...
    def read_only_query_cursor(self) -> ContextManager[ReadOnlyQueryCursor]: ...


def cmd_query(db: _QueryDB, args: argparse.Namespace) -> Optional[int]:
    """Run raw SQL with an explicit read-only-by-default safety boundary."""
    try:
        sql = _get_query_sql(args)
        write_mode = getattr(args, "write", False) is True
        if write_mode:
            if getattr(args, "confirm", None) != "WRITE":
                raise ValueError(
                    "Write SQL requires --write --confirm WRITE.")
        else:
            sql = _read_only_sql(sql)
    except ValueError as exc:
        print(f"  [ERROR] {exc}", file=sys.stderr)
        return 1

    if write_mode:
        try:
            cur = db._execute(sql)
            columns: list[str] = (
                [str(desc[0]) for desc in cur.description] if cur.description else []
            )
            rows: list[Mapping[str, object]] = (
                [dict(row) for row in cur.fetchall()] if cur.description else []
            )
        except psycopg2.Error as exc:
            message = exc.pgerror or str(exc)
            print(f"  [ERROR] {message.strip()}", file=sys.stderr)
            return 1
    else:
        try:
            with db.read_only_query_cursor() as cur:
                cur.execute(sql)
                columns = [str(desc[0]) for desc in cur.description] if cur.description else []
                rows = [dict(row) for row in cur.fetchall()] if cur.description else []
        except psycopg2.Error as exc:
            message = exc.pgerror or str(exc)
            print(f"  [ERROR] {message.strip()}", file=sys.stderr)
            return 1

    if args.json:
        print(json.dumps(rows, indent=2, default=_json_default))
        return None

    if not columns:
        print("Query executed successfully.")
        return None

    for line in _render_query_table(rows, columns):
        print(line)
    return None


def add_query_subparser(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Add ``query`` (#521 carve out of ``routes_meta._build_parser``,
    verbatim argument definitions)."""
    p_query = sub.add_parser(
        "query", help="Run SQL (read-only by default; explicit write escape hatch)")
    p_query.add_argument("sql", help="SQL query string, or '-' to read SQL from stdin")
    p_query.add_argument("--json", action="store_true", help="Print rows as JSON")
    p_query.add_argument(
        "--write", action="store_true",
        help="Allow intentional write SQL only with --confirm WRITE.")
    p_query.add_argument(
        "--confirm", metavar="TOKEN",
        help="Required exact token WRITE when using --write.")
