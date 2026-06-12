"""Audit: ban stateful-collaborator MagicMock and patches against our own code.

See ``.claude/rules/code-quality.md`` § "Mocks: leaf-seam only".

Zero-tolerance: any flagged usage anywhere under ``tests/`` fails the
audit. New anti-pattern call sites are not allowed; if you genuinely
need to mock something, either drive the real function with constructed
inputs, use a typed fake from ``tests/fakes.py``, refactor the caller
to take a kwarg-DI seam, or add the target to the leaf-seam allowlist
in ``_mock_audit_scanner.py`` with a one-line rationale.
"""

from __future__ import annotations

import os
import sys
import unittest

# Import through the ``tests`` package, NOT via a tests-dir sys.path
# insert — putting tests/ at sys.path[0] makes a later ``import
# web.server`` resolve ``web`` to tests/web in module-order-dependent
# ways (the package shadows the real one whenever no earlier import
# already registered repo-root ``web`` in sys.modules).
sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".."))
from tests._mock_audit_scanner import (
    WEB_HARNESS_MOCK_BASELINE,
    count_harness_overrides,
    iter_scan_paths,
    scan_tree,
    scan_web_harness_overrides,
)


class TestStatefulMockAudit(unittest.TestCase):
    def test_scan_reaches_tests_web_subpackage(self) -> None:
        """Pin the recursive walk (#408) — a revert to os.listdir would
        silently drop tests/web/ (including the shared harness) from
        the audit."""
        rels = {rel for rel, _ in iter_scan_paths()}
        self.assertIn(os.path.join("web", "test_routes_pipeline.py"), rels)
        self.assertIn(os.path.join("web", "_harness.py"), rels)

    def test_no_anti_pattern_call_sites(self) -> None:
        current = scan_tree()
        if not current:
            return
        lines = []
        for fname in sorted(current):
            for kind, count in sorted(current[fname].items()):
                lines.append(f"  - {fname}: {kind} ({count}×)")
        self.fail(
            "Stateful-MagicMock audit — see "
            "`.claude/rules/code-quality.md` § 'Mocks: leaf-seam only'.\n"
            "Replace each flagged usage with a typed fake, kwarg-DI seam, "
            "or real-input call.\n\n"
            + "\n".join(lines)
        )


class TestWebHarnessMockRatchet(unittest.TestCase):
    """Ratchet for the #430 MagicMock → FakePipelineDB harness migration.

    Per-file counts of remaining MagicMock-harness usage in ``tests/web``
    must match ``WEB_HARNESS_MOCK_BASELINE`` exactly: growth means a new
    test leaned on mock shapes instead of FakePipelineDB state; shrink
    means a migration landed and the baseline entry must drop with it.
    """

    def test_counter_pins_evasion_shapes(self) -> None:
        """Document exactly what the ratchet counts — occurrences, not
        lines, including alias/bare-reference shapes that a dotted-only
        regex would miss (the r1 adversarial-review evasion vectors)."""
        cases = [
            ("dotted config", "self.mock_db.get_log.return_value = []", 1),
            ("alias assignment", "m = self.mock_db", 1),
            ("getattr reflection", "getattr(self.mock_db, name)", 1),
            ("bare positional arg", "helper(self.mock_db, x)", 1),
            ("two occurrences one line",
             "mock_db.a.return_value = 1; mock_db.b.side_effect = e", 2),
            ("substring does not count", "my_mock_database = 1", 0),
            ("harness ctor", "db = _pipeline_db_test_harness()", 1),
        ]
        for desc, line, expected in cases:
            with self.subTest(desc=desc):
                self.assertEqual(
                    count_harness_overrides(line, web_file=True), expected)
        # mock_db is only meaningful inside tests/web; the transitional
        # wrapped-MagicMock constructor is counted EVERYWHERE so it
        # cannot leak outside tests/web unseen.
        self.assertEqual(count_harness_overrides(
            "self.mock_db.get_log()", web_file=False), 0)
        self.assertEqual(count_harness_overrides(
            "db = _pipeline_db_test_harness()", web_file=False), 1)

    def test_counts_match_baseline_exactly(self) -> None:
        current = scan_web_harness_overrides()
        problems: list[str] = []
        for rel in sorted(set(current) | set(WEB_HARNESS_MOCK_BASELINE)):
            cur = current.get(rel, 0)
            base = WEB_HARNESS_MOCK_BASELINE.get(rel, 0)
            if cur > base:
                problems.append(
                    f"  - {rel}: {base} → {cur} MagicMock-harness occurrences. "
                    "New tests must seed FakePipelineDB state (see "
                    "tests/fakes.py), not configure mock_db returns."
                )
            elif cur < base:
                problems.append(
                    f"  - {rel}: {base} → {cur} MagicMock-harness occurrences. "
                    "Migration progress — shrink WEB_HARNESS_MOCK_BASELINE "
                    "in tests/_mock_audit_scanner.py to match"
                    + (" (delete the entry)." if cur == 0 else ".")
                )
        if problems:
            self.fail(
                "Web-harness MagicMock ratchet (#430) out of sync with "
                "WEB_HARNESS_MOCK_BASELINE:\n" + "\n".join(problems)
            )


if __name__ == "__main__":
    unittest.main()
