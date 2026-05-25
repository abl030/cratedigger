"""AST audits that lock in the single canonical stopword source.

Two structural guards live here so the "stopwords live in ONE place"
invariant from U6 of the search-plan iter2 plan can't be quietly
violated by a future PR:

  * `test_no_external_imports_of_stopwords_constant` — anything under
    `lib/` that imports `STOPWORDS` directly is a bug. Callers go through
    `lib.search.strip_stopwords()`. The audit walks every `lib/**/*.py`,
    parses it, and fails on any `from lib.search import STOPWORDS`.

  * `test_no_inline_stopword_set_literals` — anything under `lib/` that
    defines a set / frozenset / list literal whose lowercased contents
    are a non-empty subset of `STOPWORDS` is almost certainly someone
    re-inventing a local stopword list. Fail with file:line.

The owner of these audits is `lib/search.py` itself — the constant is
defined there, the helper is defined there, and the file's allowed to
own both.
"""

from __future__ import annotations

import ast
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.search import STOPWORDS

REPO_ROOT = Path(__file__).resolve().parent.parent
LIB_ROOT = REPO_ROOT / "lib"

# The only file allowed to read STOPWORDS directly OR define the literal.
CANONICAL_OWNER = LIB_ROOT / "search.py"

# False-positive allowlist for literal scan. Add a tuple of (path, line, reason)
# only when a literal genuinely isn't a stopword-list re-definition.
LITERAL_ALLOWLIST: set[tuple[str, int]] = set()


def _iter_lib_py_files() -> list[Path]:
    return sorted(p for p in LIB_ROOT.rglob("*.py") if p.is_file())


class TestStopwordsImportGuard(unittest.TestCase):
    """STOPWORDS is private to lib.search; callers use strip_stopwords()."""

    def test_no_external_imports_of_stopwords_constant(self):
        offenders: list[str] = []
        for path in _iter_lib_py_files():
            if path == CANONICAL_OWNER:
                continue
            try:
                tree = ast.parse(path.read_text())
            except SyntaxError as exc:  # pragma: no cover — caught by other tests
                self.fail(f"Could not parse {path}: {exc}")
            for node in ast.walk(tree):
                if not isinstance(node, ast.ImportFrom):
                    continue
                if node.module != "lib.search":
                    continue
                for alias in node.names:
                    if alias.name == "STOPWORDS":
                        offenders.append(
                            f"{path.relative_to(REPO_ROOT)}:{node.lineno}"
                            " imports STOPWORDS — use strip_stopwords() instead."
                        )
        if offenders:
            self.fail(
                "Direct STOPWORDS imports are forbidden outside lib/search.py.\n"
                "Route through lib.search.strip_stopwords() so the set's\n"
                "contents can change in exactly one place.\n\nOffenders:\n  - "
                + "\n  - ".join(offenders)
            )


class TestStopwordsInlineLiteralGuard(unittest.TestCase):
    """Hard-coded stopword set literals are forbidden anywhere under lib/.

    Catches the pattern where a future PR drops a `{"the", "and"}` literal
    into a new helper instead of reaching for `strip_stopwords()`.
    """

    def _literal_strings(self, node: ast.AST) -> list[str] | None:
        """Return the literal string elements of a Set/FrozenSet/List, else None."""
        elements: list[ast.expr] | None = None
        if isinstance(node, ast.Set):
            elements = list(node.elts)
        elif isinstance(node, (ast.List, ast.Tuple)):
            elements = list(node.elts)
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in ("frozenset", "set")
            and len(node.args) == 1
            and isinstance(node.args[0], (ast.Set, ast.List, ast.Tuple))
        ):
            elements = list(node.args[0].elts)
        if elements is None or not elements:
            return None
        strings: list[str] = []
        for elt in elements:
            if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                strings.append(elt.value)
            else:
                return None  # mixed contents — not a pure stopword literal
        return strings

    def test_no_inline_stopword_set_literals(self):
        offenders: list[str] = []
        for path in _iter_lib_py_files():
            if path == CANONICAL_OWNER:
                continue
            try:
                tree = ast.parse(path.read_text())
            except SyntaxError as exc:  # pragma: no cover
                self.fail(f"Could not parse {path}: {exc}")
            rel = str(path.relative_to(REPO_ROOT))
            for node in ast.walk(tree):
                strings = self._literal_strings(node)
                if not strings:
                    continue
                lowered = {s.lower() for s in strings}
                if not lowered:
                    continue
                if not lowered.issubset({s.lower() for s in STOPWORDS}):
                    continue
                line = getattr(node, "lineno", 0)
                if (rel, line) in LITERAL_ALLOWLIST:
                    continue
                offenders.append(
                    f"{rel}:{line} literal {sorted(strings)} overlaps STOPWORDS"
                )
        if offenders:
            self.fail(
                "Inline stopword-set literals are forbidden under lib/.\n"
                "Use lib.search.strip_stopwords() instead.\n\nOffenders:\n  - "
                + "\n  - ".join(offenders)
            )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
