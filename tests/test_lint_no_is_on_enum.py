"""Lint test: ban `is` / `is not` comparisons against Enum members.

Python idiom encourages ``x is MyEnum.VALUE`` because enum members are
singletons. That holds as long as the enum class is imported exactly once.
The moment a module gets loaded twice (PYTHONPATH ambiguity, pickle
round-trips, importlib.reload, test monkey-patching), the members compare
False under ``is`` even though they compare True under ``==``.

This test walks every .py file in the repo and flags ``Compare`` nodes
whose operator is ``Is`` / ``IsNot`` and whose right-hand side is an
attribute access with an ALL_CAPS member name (the conventional enum
naming). It catches the live bug from PR #94 post-deploy (cfg.bitrate_metric
is RankBitrateMetric.AVG) and anything similar.

``is None`` / ``is True`` / ``is False`` / ``is not None`` etc. are
allowed — those are the canonical identity-compare cases Python actually
guarantees.
"""
from __future__ import annotations

import ast
import os
import unittest


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Directories to skip entirely.
SKIP_DIRS = {
    ".git", "__pycache__", ".pytest_cache", "node_modules", "fixtures",
}

# Files/paths allowed to use `is EnumMember`. Add here with a comment
# naming the specific reason if you ever need an exception — the review
# should push back hard before granting one.
ALLOWLIST: set[str] = set()


def _find_violations(path: str, source: str) -> list[tuple[int, str]]:
    """Return a list of (line_number, snippet) for each `is <EnumMember>` hit."""
    try:
        tree = ast.parse(source, filename=path)
    except SyntaxError:
        # Let pyright/py-compile catch syntax errors; we only lint parseable files.
        return []

    violations: list[tuple[int, str]] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Compare):
            continue
        for op, right in zip(node.ops, node.comparators):
            if not isinstance(op, (ast.Is, ast.IsNot)):
                continue
            # Allowed: `is None`, `is True`, `is False`.
            if isinstance(right, ast.Constant) and right.value in (None, True, False):
                continue
            # Allowed: `is type(...)` or `is some_variable` — not an enum access.
            if not isinstance(right, ast.Attribute):
                continue
            # Flag when the attribute name is ALL_CAPS (enum member convention)
            # AND NOT leading-underscore (private, usually sentinel objects).
            # Examples caught:
            #   cfg.bitrate_metric is RankBitrateMetric.AVG
            #   status is Color.RED
            # Examples NOT caught:
            #   obj is self._SENTINEL   (leading underscore → sentinel, not enum)
            #   obj is self.foo_bar     (lowercase → not an enum member)
            if not right.attr.isupper() or len(right.attr) < 2:
                continue
            if right.attr.startswith("_"):
                continue
            op_str = "is" if isinstance(op, ast.Is) else "is not"
            # Reconstruct the right-hand side for the error message.
            try:
                rhs = ast.unparse(right)
            except Exception:
                rhs = right.attr
            violations.append((
                node.lineno,
                f"{op_str} {rhs}",
            ))

    return violations


def _iter_python_files(root: str):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for name in filenames:
            if name.endswith(".py"):
                yield os.path.join(dirpath, name)


class TestNoIsOnEnumMember(unittest.TestCase):
    """Scan the whole repo for `is <EnumMember>` comparisons.

    Rationale in the module docstring. If this test fails, rewrite the
    offending line to use ``==`` / ``!=``. Enum equality by value is what
    you want in 100% of real-world cases — identity compare is the buggy
    shortcut."""

    def test_no_is_on_enum_member(self):
        all_violations: list[str] = []
        for path in _iter_python_files(REPO_ROOT):
            rel = os.path.relpath(path, REPO_ROOT)
            if rel in ALLOWLIST:
                continue
            try:
                with open(path, encoding="utf-8") as f:
                    source = f.read()
            except OSError:
                continue
            for lineno, snippet in _find_violations(path, source):
                all_violations.append(f"{rel}:{lineno}: `{snippet}`")

        if all_violations:
            msg = (
                "`is` / `is not` comparison against an enum member detected. "
                "Use `==` / `!=` instead — identity compare silently breaks "
                "when the enum class gets loaded twice (PYTHONPATH ambiguity, "
                "reload, pickle round-trip, etc.). See PR #94 post-deploy "
                "hotfix for the live failure mode.\n\n"
                + "\n".join(all_violations)
            )
            self.fail(msg)


if __name__ == "__main__":
    unittest.main()
