"""Audit: production strict-mode errors only ever decrease (issue #784).

The annotation campaign's enforcement: per-file strict-error counts under
``pyrightconfig.strict-production.json`` must EXACTLY match the committed
baseline. New strict errors fail; improvements must tighten the baseline in
the same PR. The final campaign PR flips the production config to strict and
deletes this machinery.
"""

from __future__ import annotations

import os
import tempfile
import unittest

from tests._strict_ratchet_baseline import STRICT_RATCHET_BASELINE
from tests._strict_ratchet_scanner import count_strict_errors

_REGEN = (
    'nix-shell --run "python3 -m tests._strict_ratchet_scanner"'
    " > tests/_strict_ratchet_baseline.py"
)


class TestStrictCoverageRatchet(unittest.TestCase):
    """Live per-file strict counts must equal the baseline exactly."""

    def test_strict_counts_match_baseline_exactly(self) -> None:
        live = count_strict_errors()
        if live == STRICT_RATCHET_BASELINE:
            return
        regressions: list[str] = []
        improvements: list[str] = []
        for rel in sorted(set(live) | set(STRICT_RATCHET_BASELINE)):
            n_live = live.get(rel, 0)
            n_base = STRICT_RATCHET_BASELINE.get(rel, 0)
            if n_live > n_base:
                regressions.append(f"{rel}: {n_base} -> {n_live}")
            elif n_live < n_base:
                improvements.append(f"{rel}: {n_base} -> {n_live}")
        msg = ["Strict-coverage ratchet mismatch (issue #784)."]
        if regressions:
            msg.append(
                "NEW strict errors in production code — annotate instead "
                "of widening:\n  " + "\n  ".join(regressions)
            )
        if improvements:
            msg.append(
                "Strict errors reduced — tighten the baseline in this "
                f"same PR:\n  {_REGEN}\nImproved:\n  "
                + "\n  ".join(improvements)
            )
        self.fail("\n".join(msg))


class TestScannerTripsOnViolations(unittest.TestCase):
    """Known-bad self-test: the pyright pipeline must count real errors."""

    def test_planted_strict_error_is_counted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            os.mkdir(os.path.join(tmp, "prod"))
            with open(os.path.join(tmp, "prod", "bad.py"), "w") as f:
                f.write("def f(x):\n    return x.wat()\n")
            with open(os.path.join(tmp, "strict.json"), "w") as f:
                f.write('{"typeCheckingMode": "strict"}')
            counts = count_strict_errors(repo_root=tmp, config="strict.json")
        planted = os.path.join("prod", "bad.py")
        self.assertIn(planted, counts)
        self.assertGreater(counts[planted], 0)

    def test_clean_project_counts_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "ok.py"), "w") as f:
                f.write("def f(x: int) -> int:\n    return x\n")
            with open(os.path.join(tmp, "strict.json"), "w") as f:
                f.write('{"typeCheckingMode": "strict"}')
            counts = count_strict_errors(repo_root=tmp, config="strict.json")
        self.assertEqual(counts, {})


if __name__ == "__main__":
    unittest.main()
