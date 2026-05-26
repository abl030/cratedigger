"""Unit tests for ``lib.search_classification.classify_failure_class`` (U12).

Pure-function decision-matrix coverage. The classifier maps a list of
``SearchSummary`` from one completed plan cycle (plus the request's
current status) onto one of the 5 failure-class buckets, or ``None``
when there is no signal to record.

Pattern: one ``test_classify_returns_expected_bucket`` method walks a
``CASES`` table covering every branch + the documented edge cases. New
buckets / branches should be added as rows, not new methods.
"""

from __future__ import annotations

import unittest

from lib.search_classification import (
    FAILURE_CLASS_A_ZERO_RESULTS_DOMINANT,
    FAILURE_CLASS_B_CANDS_NEVER_MATCH,
    FAILURE_CLASS_D_FOUND_BUT_NO_IMPORT,
    FAILURE_CLASS_E_MIXED,
    FAILURE_CLASS_RESOLVED,
    SearchSummary,
    classify_failure_class,
)


def _no_results(n: int) -> list[SearchSummary]:
    return [SearchSummary(outcome="no_results") for _ in range(n)]


def _no_match(n: int, *, reason: str | None = "strict_count_mismatch"
              ) -> list[SearchSummary]:
    return [SearchSummary(outcome="no_match", rejection_reason=reason)
            for _ in range(n)]


def _found(n: int) -> list[SearchSummary]:
    return [SearchSummary(outcome="found") for _ in range(n)]


class TestClassifyFailureClass(unittest.TestCase):
    """Decision-matrix coverage of the pure classifier."""

    CASES = [
        # 1. A dominant: 9 of 10 are no_results (90% > 80%) → A.
        (
            "A: 9 of 10 no_results (90%) → A_zero_results_dominant",
            _no_results(9) + _no_match(1),
            "wanted",
            FAILURE_CLASS_A_ZERO_RESULTS_DOMINANT,
        ),
        # 2. A boundary: exactly 80% no_results → NOT A (strict >).
        #    8 of 10 no_results = 0.8 exactly. Falls through to E_mixed
        #    (no_results + no_match present, no found, no dominance).
        (
            "A boundary: exactly 80% no_results → E_mixed (strict >)",
            _no_results(8) + _no_match(2),
            "wanted",
            FAILURE_CLASS_E_MIXED,
        ),
        # 3. B: all 10 produced candidates (no_match), none found → B.
        (
            "B: 10 of 10 no_match with candidates → B_cands_never_match",
            _no_match(10),
            "wanted",
            FAILURE_CLASS_B_CANDS_NEVER_MATCH,
        ),
        # 4. D: 1 of 5 is found, request still wanted → D.
        #    The import never landed despite the matcher accepting one.
        (
            "D: 1 found + 4 no_match, status wanted → D_found_but_no_import",
            _found(1) + _no_match(4),
            "wanted",
            FAILURE_CLASS_D_FOUND_BUT_NO_IMPORT,
        ),
        # 5. E_mixed: 40% no_results + 60% no_match, no found → E.
        (
            "E_mixed: 40% no_results + 60% no_match → E_mixed",
            _no_results(4) + _no_match(6),
            "wanted",
            FAILURE_CLASS_E_MIXED,
        ),
        # 6. resolved: request status moved past wanted mid-cycle. The
        #    status overrides any search-pattern verdict because the
        #    cycle outcome is, by definition, resolved.
        (
            "resolved: status=imported overrides all_no_match",
            _no_match(5),
            "imported",
            FAILURE_CLASS_RESOLVED,
        ),
        (
            "resolved: status=downloading overrides empty searches",
            [],
            "downloading",
            FAILURE_CLASS_RESOLVED,
        ),
        (
            "resolved: status=manual overrides A-dominant pattern",
            _no_results(10),
            "manual",
            FAILURE_CLASS_RESOLVED,
        ),
        # 7. Edge: zero searches in cycle with status=wanted → None.
        #    A degenerate cycle (e.g. all searches were stale rejects)
        #    must NOT overwrite a previously-classified failure_class
        #    with "no signal".
        (
            "edge: zero searches + status=wanted → None (preserve prior)",
            [],
            "wanted",
            None,
        ),
        # 8. Edge: a no_match row whose rejection_reason is
        #    'strict_count_mismatch' is the dominant pattern.
        #    Aggregates into B (not its own bucket) — the classifier
        #    doesn't peek at rejection_reason for the bucket decision;
        #    rejection_reason is captured on each row for downstream
        #    forensics but the cycle bucket is shape-driven.
        (
            "edge: all no_match w/ strict_count_mismatch → still B",
            _no_match(10, reason="strict_count_mismatch"),
            "wanted",
            FAILURE_CLASS_B_CANDS_NEVER_MATCH,
        ),
        # Single search rows — extremes of N=1.
        (
            "N=1 found, status wanted → D",
            _found(1),
            "wanted",
            FAILURE_CLASS_D_FOUND_BUT_NO_IMPORT,
        ),
        (
            "N=1 no_results, status wanted → A (1/1=100% > 80%)",
            _no_results(1),
            "wanted",
            FAILURE_CLASS_A_ZERO_RESULTS_DOMINANT,
        ),
        (
            "N=1 no_match, status wanted → B (no zero-results)",
            _no_match(1),
            "wanted",
            FAILURE_CLASS_B_CANDS_NEVER_MATCH,
        ),
        # found dominates D even when no_results would also trigger A.
        # (Branch order: D fires before A.)
        (
            "found + many no_results → D (found dominates A)",
            _found(1) + _no_results(9),
            "wanted",
            FAILURE_CLASS_D_FOUND_BUT_NO_IMPORT,
        ),
        # Other outcomes (timeout, error, empty_query) count toward
        # the denominator. They had no candidates (B doesn't fit) and
        # they're not no_results (A doesn't fit) — they're explicitly
        # "the search couldn't complete cleanly". Bucket: E_mixed.
        (
            "timeouts only → E_mixed (no found, no no_match candidates)",
            [SearchSummary(outcome="timeout") for _ in range(3)],
            "wanted",
            FAILURE_CLASS_E_MIXED,
        ),
        # A mix of no_match and timeout still falls into E_mixed —
        # B requires ALL consumed attempts to be no_match.
        (
            "mixed no_match + timeout → E_mixed (B requires all no_match)",
            _no_match(7) + [SearchSummary(outcome="timeout") for _ in range(3)],
            "wanted",
            FAILURE_CLASS_E_MIXED,
        ),
    ]

    def test_classify_returns_expected_bucket(self):
        for desc, searches, status, expected in self.CASES:
            with self.subTest(desc=desc):
                got = classify_failure_class(
                    searches, current_status=status,
                )
                self.assertEqual(
                    got, expected,
                    f"{desc}: got {got!r}, expected {expected!r}",
                )


class TestSearchSummaryDataclassContract(unittest.TestCase):
    """Pin the internal type's shape so DB-row → dataclass conversion
    has one declared contract."""

    def test_dataclass_is_frozen(self):
        s = SearchSummary(outcome="no_match", rejection_reason="x")
        with self.assertRaises(Exception):
            s.outcome = "found"  # type: ignore[misc]

    def test_dataclass_defaults_rejection_reason_to_none(self):
        s = SearchSummary(outcome="found")
        self.assertEqual(s.outcome, "found")
        self.assertIsNone(s.rejection_reason)


if __name__ == "__main__":
    unittest.main()
