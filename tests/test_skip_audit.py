"""Audit: no test in the suite is allowed to be skip-gated.

See CLAUDE.md § "Skipped tests are an anti-pattern". A skipped test either
runs every invocation of ``bash scripts/run_tests.sh`` or it doesn't belong
in the suite. This audit fails CI if any test file reintroduces a skip
marker — there is no allowlist.

Forbidden patterns:
    * ``@unittest.skipUnless(...)``
    * ``@unittest.skipIf(...)``
    * ``@unittest.skip("...")``
    * ``raise unittest.SkipTest(...)``
    * ``raise SkipTest(...)``

If your test legitimately needs an external resource, the nix-shell dev
shell must provide it. If it cannot, the test belongs elsewhere (a manual
procedure doc, a separate harness, a slice with a fake) — not as a
``unittest.TestCase`` masquerading as coverage.
"""

from __future__ import annotations

import os
import re
import unittest


TESTS_DIR = os.path.dirname(__file__)

FORBIDDEN_PATTERNS = [
    re.compile(r"@unittest\.skipUnless\b"),
    re.compile(r"@unittest\.skipIf\b"),
    re.compile(r"@unittest\.skip\("),
    re.compile(r"\braise\s+unittest\.SkipTest\b"),
    re.compile(r"\braise\s+SkipTest\b"),
]


class TestNoSkippedTestsAllowed(unittest.TestCase):
    """The whole suite must run every time. No exceptions."""

    @staticmethod
    def _iter_py_files():
        """Yield ``(relpath, abspath)`` for every .py under tests/, recursively.

        Excludes only this exact file (by relpath, not basename — a future
        ``tests/web/test_skip_audit.py`` must not inherit the exemption).
        """
        for dirpath, dirnames, filenames in os.walk(TESTS_DIR):
            dirnames[:] = sorted(d for d in dirnames if d != "__pycache__")
            for fname in sorted(filenames):
                if not fname.endswith(".py"):
                    continue
                path = os.path.join(dirpath, fname)
                rel = os.path.relpath(path, TESTS_DIR)
                if rel == os.path.basename(__file__):
                    continue  # this file mentions the patterns in its docstring
                yield rel, path

    def test_no_skip_markers_in_tests_dir(self) -> None:
        offenders: list[str] = []
        for rel, path in self._iter_py_files():
            with open(path, encoding="utf-8") as f:
                for lineno, line in enumerate(f, start=1):
                    for pat in FORBIDDEN_PATTERNS:
                        if pat.search(line):
                            offenders.append(f"{rel}:{lineno}: {line.rstrip()}")
        self.assertEqual(
            offenders, [],
            "Skipped/gated tests are forbidden — see CLAUDE.md "
            "§ 'Skipped tests are an anti-pattern'. Offenders:\n  "
            + "\n  ".join(offenders),
        )

    def test_scan_reaches_tests_web_subpackage(self) -> None:
        """Pin the recursive walk (#408) — a revert to os.listdir would
        silently drop tests/web/ from the audit."""
        rels = {rel for rel, _ in self._iter_py_files()}
        self.assertIn(os.path.join("web", "_harness.py"), rels)
        self.assertIn(os.path.join("web", "test_route_audit.py"), rels)


if __name__ == "__main__":
    unittest.main()
