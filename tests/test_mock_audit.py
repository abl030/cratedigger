"""Audit: ban stateful-collaborator MagicMock and patches against our own code.

See CLAUDE.md § "Mocks: leaf-seam only" and issue #290.

This audit enforces a grandfathered shrink. We froze every existing
anti-pattern call site in ``tests/mock_audit_baseline.json``. New code may
**never** add a finding — the audit fails if any (file, finding_key) pair's
count exceeds its baseline. Refactors that *remove* anti-pattern uses are
expected; the baseline can shrink at any time (run
``python3 tests/_rebuild_mock_audit_baseline.py``).

The end state is ``mock_audit_baseline.json == {}`` — every flagged usage
has been migrated to ``FakePipelineDB`` / ``FakeBeetsDB`` / ``FakeSlskdAPI``
or to driving the real function with constructed inputs. At that point
Phase 3 of the issue will delete the baseline entirely and the audit will
require zero findings.

Forbidden patterns the scanner flags:

* ``db = MagicMock(...)``, ``mock_db = MagicMock(...)``, ``ctx = MagicMock(...)``
  and similar variable-name conventions for stateful collaborators. Use a
  ``Fake*`` from ``tests/fakes.py``.
* ``patch("lib.foo.our_function")`` for any target that isn't on the
  leaf-seam allowlist in ``_mock_audit_scanner.py``. Leaf seams are
  subprocess / urllib / requests / os.path / time.sleep / music_tag /
  redis / fire-and-forget notifiers — the genuine outermost edge.

If you're mocking your own code, you're testing the mock — drive the
real function with constructed inputs instead.
"""

from __future__ import annotations

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _mock_audit_scanner import scan_tree


TESTS_DIR = os.path.abspath(os.path.dirname(__file__))
BASELINE_PATH = os.path.join(TESTS_DIR, "mock_audit_baseline.json")


def _load_baseline() -> dict:
    if not os.path.exists(BASELINE_PATH):
        return {}
    with open(BASELINE_PATH, encoding="utf-8") as f:
        return json.load(f)


class TestStatefulMockAudit(unittest.TestCase):
    """Anti-pattern call sites may only decrease over time, never grow."""

    def test_no_new_anti_pattern_call_sites(self) -> None:
        baseline = _load_baseline()
        current = scan_tree()

        regressions: list[str] = []
        for fname, kinds in current.items():
            baseline_kinds = baseline.get(fname, {})
            for kind, count in kinds.items():
                allowed = baseline_kinds.get(kind, 0)
                if count > allowed:
                    regressions.append(
                        f"{fname}: '{kind}' now appears {count}× "
                        f"(baseline allowed {allowed})"
                    )

        if regressions:
            self.fail(
                "Stateful-MagicMock audit regression — see CLAUDE.md "
                "§ 'Mocks: leaf-seam only' and issue #290.\n"
                "New anti-pattern call sites are not allowed; the "
                "baseline can only shrink, never grow.\n\n"
                + "\n".join(f"  - {r}" for r in regressions)
                + "\n\nIf you genuinely removed an anti-pattern, "
                "re-snapshot the baseline:\n"
                "  python3 tests/_rebuild_mock_audit_baseline.py"
            )

    def test_baseline_entries_still_exist(self) -> None:
        """If a baseline entry has zero current findings, the baseline is
        stale — re-snapshot it so the audit accurately reflects what's
        left to migrate. Otherwise reviewers can't tell whether a PR
        actually shrunk the surface or just happened not to add to it."""
        baseline = _load_baseline()
        current = scan_tree()

        stale: list[str] = []
        for fname, kinds in baseline.items():
            current_kinds = current.get(fname, {})
            for kind, count in kinds.items():
                if current_kinds.get(kind, 0) < count:
                    stale.append(
                        f"{fname}: '{kind}' baseline={count} but "
                        f"current={current_kinds.get(kind, 0)} — "
                        "re-snapshot the baseline"
                    )

        if stale:
            self.fail(
                "Mock audit baseline is stale — anti-pattern count "
                "decreased but the baseline wasn't updated.\n"
                "Re-snapshot:  python3 tests/_rebuild_mock_audit_baseline.py\n\n"
                + "\n".join(f"  - {s}" for s in stale)
            )


if __name__ == "__main__":
    unittest.main()
