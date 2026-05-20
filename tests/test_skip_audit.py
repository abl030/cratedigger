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

    def test_no_skip_markers_in_tests_dir(self) -> None:
        offenders: list[str] = []
        for fname in sorted(os.listdir(TESTS_DIR)):
            if not fname.endswith(".py"):
                continue
            if fname == os.path.basename(__file__):
                continue  # this file mentions the patterns in its docstring
            path = os.path.join(TESTS_DIR, fname)
            with open(path, encoding="utf-8") as f:
                for lineno, line in enumerate(f, start=1):
                    for pat in FORBIDDEN_PATTERNS:
                        if pat.search(line):
                            offenders.append(f"{fname}:{lineno}: {line.rstrip()}")
        self.assertEqual(
            offenders, [],
            "Skipped/gated tests are forbidden — see CLAUDE.md "
            "§ 'Skipped tests are an anti-pattern'. Offenders:\n  "
            + "\n  ".join(offenders),
        )


if __name__ == "__main__":
    unittest.main()
