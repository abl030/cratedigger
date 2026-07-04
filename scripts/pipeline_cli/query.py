"""pipeline-cli ``query`` command (#495 carve).

Debugging read-only SQL escape hatch — ``pipeline-cli query <sql>``.
"""

import json
import sys
from datetime import date, datetime, time
from decimal import Decimal

import psycopg2

from scripts.pipeline_cli._format import _json_default


def _stringify_query_value(value):
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


def _render_query_table(rows, columns):
    """Render SQL query results as a simple aligned table."""
    widths = {col: len(col) for col in columns}
    string_rows = []

    for row in rows:
        rendered = []
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


def _get_query_sql(args):
    """Resolve SQL text from argv or stdin."""
    sql = sys.stdin.read() if args.sql == "-" else args.sql
    sql = sql.strip()
    if not sql:
        raise ValueError("No SQL provided.")
    return sql


def cmd_query(db, args):
    """Run a debugging SQL query in a read-only session."""
    try:
        sql = _get_query_sql(args)
    except ValueError as exc:
        print(f"  [ERROR] {exc}", file=sys.stderr)
        return 1

    db._execute("SET SESSION default_transaction_read_only = on")
    try:
        cur = db._execute(sql)
        columns = [desc[0] for desc in cur.description] if cur.description else []
        rows = [dict(row) for row in cur.fetchall()] if cur.description else []
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
