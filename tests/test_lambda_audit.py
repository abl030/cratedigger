"""Audit: ban bare-``None`` lambdas for strict-raise mirror adapters.

Layer 3 of the test-fidelity hardening (#382). See
``.claude/rules/test-fidelity.md`` § "Rule B — Fakes must mirror real-adapter
exception contracts" and ``tests/_lambda_audit.py`` for the heuristic.

Zero-tolerance, allowlist-grandfathered: any *new* ``mb_get_release=lambda
…: None`` (or the other strict-raise adapters) anywhere under ``tests/``
fails the audit. The real adapters raise ``urllib.error.HTTPError`` on 404 —
fake the miss with ``tests/fakes.py::FakeMBLookup`` / ``FakeDiscogsLookup``
(``raises_on_404=True``) so the test exercises the branch production actually
takes. If a flagged site is genuinely benign (the consumer has an explicit
``None`` branch), add it to ``_lambda_audit.ALLOWLIST`` with a one-line
rationale.
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _lambda_audit import ALLOWLIST, iter_test_files, scan_file, scan_tree  # noqa: E402


class TestAdapterLambdaAudit(unittest.TestCase):
    def test_no_strict_raise_none_lambdas(self) -> None:
        current = scan_tree()
        if not current:
            return
        lines = []
        for fname in sorted(current):
            for func, lineno, kwarg in sorted(current[fname]):
                lines.append(f"  - {fname}:{lineno} {func}: {kwarg}=lambda …: None")
        self.fail(
            "Adapter-lambda audit (test-fidelity Rule B). A strict-raise "
            "mirror adapter is faked with `lambda …: None`, but production "
            "raises urllib.error.HTTPError on 404 — the None branch is "
            "unreachable in production.\nUse tests/fakes.py::FakeMBLookup / "
            "FakeDiscogsLookup (raises_on_404=True), or — if the consumer has "
            "a real None branch — allowlist it in _lambda_audit.ALLOWLIST "
            "with a rationale.\n\n" + "\n".join(lines)
        )

    def test_allowlist_entries_still_match_real_sites(self) -> None:
        """Catch stale allowlist entries — a grandfathered site that was
        renamed, deleted, or migrated to the fake but left its row behind."""
        live: set[str] = set()
        for name, path in iter_test_files():
            for func, _lineno, _kwarg in scan_file(path):
                live.add(f"{name}::{func}")
        stale = sorted(k for k in ALLOWLIST if k not in live)
        self.assertEqual(
            stale, [],
            msg=(
                "ALLOWLIST has entries that no longer match a flagged site — "
                "delete them (the site was migrated to a fake, which is the "
                "goal):\n  - " + "\n  - ".join(stale)
            ),
        )

    def test_allowlist_rationales_are_non_empty(self) -> None:
        empty = sorted(k for k, v in ALLOWLIST.items() if not v.strip())
        self.assertEqual(empty, [], msg="ALLOWLIST entries need a rationale.")

    def test_scan_reaches_tests_web_subpackage(self) -> None:
        """Pin the recursive walk (#408) — a revert to os.listdir would
        silently drop tests/web/ from the audit."""
        rels = {rel for rel, _ in iter_test_files()}
        self.assertIn(os.path.join("web", "test_routes_browse.py"), rels)
        self.assertIn(os.path.join("web", "_harness.py"), rels)


if __name__ == "__main__":
    unittest.main()
