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

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _mock_audit_scanner import iter_scan_paths, scan_tree


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


if __name__ == "__main__":
    unittest.main()
