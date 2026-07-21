"""pipeline-cli ``query`` command (#495 carve).

Debugging read-only SQL escape hatch — ``pipeline-cli query <sql>``.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, time
from decimal import Decimal
from typing import Mapping, Optional, Protocol, Sequence

import psycopg2

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


class _QueryCursor(Protocol):
    """DB-API cursor slice ``cmd_query`` reads (issue #784, #409 pattern)."""

    description: Optional[Sequence[Sequence[object]]]

    def fetchall(self) -> list[Mapping[str, object]]: ...


class _QueryDB(Protocol):
    """``db`` shape ``cmd_query`` needs — the raw-SQL debugging escape
    hatch touches nothing but ``_execute`` (issue #784, #409 pattern)."""

    def _execute(self, sql: str) -> _QueryCursor: ...


def cmd_query(db: _QueryDB, args: argparse.Namespace) -> Optional[int]:
    """Run a debugging SQL query in a read-only session."""
    try:
        sql = _get_query_sql(args)
    except ValueError as exc:
        print(f"  [ERROR] {exc}", file=sys.stderr)
        return 1

    db._execute("SET SESSION default_transaction_read_only = on")
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
    finally:
        db._execute("SET SESSION default_transaction_read_only = off")

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
    p_query = sub.add_parser("query", help="Run a read-only SQL query for debugging")
    p_query.add_argument("sql", help="SQL query string, or '-' to read SQL from stdin")
    p_query.add_argument("--json", action="store_true", help="Print rows as JSON")
