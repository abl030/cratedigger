"""Audit: every ``post_*`` handler in ``web/routes/*.py`` validates its
body through ``parse_body`` (Pydantic).

See ``.claude/rules/code-quality.md`` § "HTTP request bodies". A handler
that reads from the raw ``body`` dict instead of a typed model is a
boundary regression — error responses become inconsistent and the
request contract disappears from the file.

Zero-tolerance with a small allowlist. Allowlisted handlers must carry a
``# pydantic-audit: skip — <reason>`` comment on the def line so the
exception is visible in code review.
"""

from __future__ import annotations

import ast
import os
import unittest


TESTS_DIR = os.path.abspath(os.path.dirname(__file__))
ROUTES_DIR = os.path.abspath(os.path.join(TESTS_DIR, "..", "web", "routes"))

# Handlers that are intentionally NOT routed through ``parse_body``.
# Each entry must carry a rationale because the audit's whole point is
# that route validation goes through one shared adapter.
_ALLOWLIST: dict[str, str] = {
    # Tri-mode body (values / download_log / request+path); the values
    # branch already validates through ``msgspec.convert`` against
    # ``ImportPreviewValues``. A Pydantic model for the dispatch shape
    # would be uglier than the existing mode-detection logic.
    "web/routes/imports.py:post_import_preview": "tri-mode body, msgspec-validated in _preview_values_from_body",
}


def _route_files() -> list[str]:
    paths: list[str] = []
    for fn in sorted(os.listdir(ROUTES_DIR)):
        if not fn.endswith(".py"):
            continue
        if fn.startswith("_"):
            continue
        paths.append(os.path.join(ROUTES_DIR, fn))
    return paths


def _handler_uses_parse_body(fn: ast.FunctionDef) -> bool:
    for node in ast.walk(fn):
        if isinstance(node, ast.Call):
            target = node.func
            if isinstance(target, ast.Name) and target.id == "parse_body":
                return True
            if isinstance(target, ast.Attribute) and target.attr == "parse_body":
                return True
    return False


def _handler_reads_raw_body(fn: ast.FunctionDef) -> bool:
    """Detect ``body.get(...)`` or ``body[...]`` access in the handler body."""
    for node in ast.walk(fn):
        if isinstance(node, ast.Call):
            target = node.func
            if (
                isinstance(target, ast.Attribute)
                and target.attr == "get"
                and isinstance(target.value, ast.Name)
                and target.value.id == "body"
            ):
                return True
        if isinstance(node, ast.Subscript):
            if isinstance(node.value, ast.Name) and node.value.id == "body":
                return True
    return False


class TestPydanticRouteAudit(unittest.TestCase):
    def test_every_post_handler_uses_pydantic(self) -> None:
        violations: list[str] = []
        for path in _route_files():
            relpath = os.path.relpath(path, os.path.dirname(ROUTES_DIR) + "/..")
            with open(path, encoding="utf-8") as f:
                src = f.read()
            tree = ast.parse(src, filename=path)
            for node in ast.walk(tree):
                if not isinstance(node, ast.FunctionDef):
                    continue
                if not node.name.startswith("post_"):
                    continue
                key = f"{relpath}:{node.name}"
                if key in _ALLOWLIST:
                    continue
                if _handler_uses_parse_body(node):
                    continue
                if _handler_reads_raw_body(node):
                    violations.append(
                        f"  - {key}: reads ``body`` directly without "
                        "going through ``parse_body``"
                    )
                    continue
                # No parse_body call and no raw body access — handler
                # doesn't use the body at all. That's fine.
        if violations:
            self.fail(
                "Pydantic route audit — see "
                "`.claude/rules/code-quality.md` § 'HTTP request bodies'.\n"
                "Each POST handler in `web/routes/` must declare a typed "
                "request model and parse via `parse_body`. Add a rationale "
                "to `_ALLOWLIST` in this file if a handler legitimately "
                "cannot use the standard adapter.\n\n"
                + "\n".join(violations)
            )

    def test_allowlist_entries_still_exist(self) -> None:
        """If an allowlisted handler is gone or renamed, the allowlist is
        stale and silently lets a new handler skip the audit."""
        present: set[str] = set()
        for path in _route_files():
            relpath = os.path.relpath(path, os.path.dirname(ROUTES_DIR) + "/..")
            with open(path, encoding="utf-8") as f:
                tree = ast.parse(f.read(), filename=path)
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef) and node.name.startswith("post_"):
                    present.add(f"{relpath}:{node.name}")
        stale = sorted(set(_ALLOWLIST) - present)
        if stale:
            self.fail(
                "Pydantic route audit allowlist is stale — these entries "
                "no longer match any POST handler:\n\n"
                + "\n".join(f"  - {s}" for s in stale)
                + "\n\nRemove them from `_ALLOWLIST` in "
                "`tests/test_pydantic_route_audit.py`."
            )


if __name__ == "__main__":
    unittest.main()
