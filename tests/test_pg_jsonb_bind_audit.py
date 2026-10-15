"""Audit: the DB layer binds JSONB as objects, never as encoded JSON text.

``lib/pipeline_db/pg_safe.py`` cleans every ``str`` and ``Json`` parameter,
but it cannot see escapes (``\\u0000``, ``\\udce9``, ``NaN``) inside JSON
text that was already encoded. Eight DB-layer sites used to encode a
payload and bind the string; each one could fail a write on an unstorable
value.

Bounded grammar: inside ``lib/pipeline_db/``, a call to ``json.dumps``,
``msgspec.json.encode`` or any ``.to_json()`` may appear only in the named
functions below. Every allowlist entry must still match a live call, so the
list cannot rot. Strings handed in by callers are bound with
``pg_jsonb_text``; that half is patrolled by behaviour tests
(``tests/test_pg_safe.py``), not by this audit.
"""

from __future__ import annotations

import ast
import unittest
from pathlib import Path
from typing import ClassVar

PACKAGE = Path(__file__).resolve().parent.parent / "lib" / "pipeline_db"

ALLOWED: dict[tuple[str, str], str] = {
    ("pg_safe.py", "pg_json_dumps"):
        "the one storable encoder every Json parameter goes through",
    ("cleanup_journal.py", "_json_clone"):
        "JSON round-trip to validate and detach a value; never bound",
    ("terminal_outcomes.py", "_automation_audit"):
        "re-encodes TerminalDownloadAudit.validation_result, whose bind "
        "decodes it again through pg_jsonb_text",
}


def _encoder_name(call: ast.Call) -> str | None:
    func = call.func
    if not isinstance(func, ast.Attribute):
        return None
    if func.attr == "to_json":
        return ".to_json()"
    base = func.value
    if func.attr == "dumps" and isinstance(base, ast.Name) and base.id == "json":
        return "json.dumps"
    if (
        func.attr == "encode"
        and isinstance(base, ast.Attribute)
        and base.attr == "json"
        and isinstance(base.value, ast.Name)
        and base.value.id == "msgspec"
    ):
        return "msgspec.json.encode"
    return None


def encoder_calls(source: str, filename: str) -> list[tuple[str, str, int]]:
    """(enclosing function, encoder, line) for every encoder call."""
    found: list[tuple[str, str, int]] = []

    def visit(node: ast.AST, function: str) -> None:
        for child in ast.iter_child_nodes(node):
            name = function
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                name = child.name
            if isinstance(child, ast.Call):
                encoder = _encoder_name(child)
                if encoder is not None:
                    found.append((name, encoder, child.lineno))
            visit(child, name)

    visit(ast.parse(source, filename=filename), "<module>")
    return found


def violations(files: dict[str, str]) -> list[str]:
    """Every disallowed encoder call and every stale allowlist entry."""
    problems: list[str] = []
    live: set[tuple[str, str]] = set()
    for filename, source in sorted(files.items()):
        for function, encoder, line in encoder_calls(source, filename):
            key = (filename, function)
            if key in ALLOWED:
                live.add(key)
                continue
            problems.append(
                f"lib/pipeline_db/{filename}:{line} {function}: {encoder} — "
                "bind psycopg2.extras.Json(obj) instead",
            )
    for key in sorted(set(ALLOWED) - live):
        problems.append(f"stale allowlist entry {key}: no encoder call there")
    return problems


class TestPipelineDbBindsJsonbAsObjects(unittest.TestCase):

    def test_no_encoded_json_outside_the_allowlist(self) -> None:
        files = {path.name: path.read_text() for path in PACKAGE.glob("*.py")}
        self.assertEqual(violations(files), [])


class TestAuditTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests, one per clause and encoder shape."""

    _ALLOWED_SOURCES: ClassVar[dict[str, str]] = {
        "pg_safe.py": "def pg_json_dumps(v):\n    return msgspec.json.encode(v)\n",
        "cleanup_journal.py": "def _json_clone(v):\n    return msgspec.json.encode(v)\n",
        "terminal_outcomes.py": "def _automation_audit(p):\n    return json.dumps(p)\n",
    }

    def _with(self, extra: str) -> list[str]:
        return violations({**self._ALLOWED_SOURCES, "requests.py": extra})

    def test_msgspec_encode_bind_is_named(self) -> None:
        found = self._with("def w(s):\n    x = msgspec.json.encode(s).decode()\n")
        self.assertEqual(len(found), 1)
        self.assertRegex(found[0], r"requests\.py:2 w: msgspec\.json\.encode")

    def test_json_dumps_bind_is_named(self) -> None:
        self.assertRegex(
            self._with("def w(s):\n    return json.dumps(s)\n")[0],
            r"requests\.py:2 w: json\.dumps",
        )

    def test_to_json_bind_is_named(self) -> None:
        self.assertRegex(
            self._with("def w(s):\n    return s.to_json()\n")[0],
            r"requests\.py:2 w: \.to_json\(\)",
        )

    def test_stale_allowlist_entry_is_named(self) -> None:
        sources = dict(self._ALLOWED_SOURCES)
        sources["cleanup_journal.py"] = "def _json_clone(v):\n    return v\n"
        self.assertEqual(
            violations(sources),
            [(
                "stale allowlist entry ('cleanup_journal.py', '_json_clone'): "
                "no encoder call there"
            )],
        )

    def test_quiet_for_json_objects_and_decodes(self) -> None:
        self.assertEqual(self._with(
            "def w(s):\n"
            "    a = psycopg2.extras.Json(s)\n"
            "    b = json.loads(s)\n"
            "    c = msgspec.json.decode(s)\n"
            "    return a, b, c\n",
        ), [])


if __name__ == "__main__":
    unittest.main()
