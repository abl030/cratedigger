"""Tests for ``lib.unfindable_detection_service`` (U13 of search-plan-iter2).

Three layers of coverage:

1. **Pure classifier** (``TestClassifyUnfindableFromState``) — subTest
   table covering all 4 buckets, boundary, downgrade, and edge cases.
2. **Service layer** (``TestUnfindableDetectionService``) — drives
   ``UnfindableDetectionService.categorise_request`` /
   ``categorise_due_batch`` against ``FakePipelineDB`` + ``FakeSlskdAPI``,
   asserts persisted state (probe + category writes), cadence
   behaviour, downgrade, and probe-failure isolation.
3. **R20 invariant guards** (``TestR20CursorIsolation``) — belt-and-
   braces enforcement that the detection unit can never throttle the
   regular search cadence:
   * **Structural** (``test_module_does_not_reference_cursor_mutators``):
     AST walk over the detection module and oneshot script rejects any
     reference to the cursor-mutation banned-list.
   * **Runtime** (``test_categorise_run_does_not_touch_cursor_state``):
     after a representative cohort run, every cursor-mutation
     ``FakePipelineDB`` recorder has ``call_count == 0``.

R20 enforcement is "two layers, required" by the U13 plan; both must
exist and both must pass.
"""

from __future__ import annotations

import ast
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from lib.unfindable_detection_service import (
    ARTIST_MATCH_THRESHOLD,
    CATEGORY_ALBUM_ABSENT_ARTIST_PRESENT,
    CATEGORY_ARTIST_ABSENT,
    CATEGORY_ONE_TRACK_STRUCTURAL,
    CATEGORY_WRONG_PRESSING_AVAILABLE,
    PROBE_INTERVAL_DAYS,
    REQUIRED_LOW_PROBES,
    REQUIRED_ZERO_FIND_CYCLES,
    RESULT_CATEGORISED,
    RESULT_DOWNGRADED,
    RESULT_NOT_DUE,
    RESULT_NO_CHANGE,
    RESULT_PROBE_FAILED,
    RESULT_REQUEST_NOT_FOUND,
    WRONG_PRESSING_MIN_HITS,
    ArtistProbeResult,
    UnfindableDetectionService,
    UnfindableInputs,
    UnfindableSearchLogSignal,
    classify_unfindable_from_state,
    fuzzy_artist_observed_in_probe,
)
from tests.fakes import FakePipelineDB, FakeSlskdAPI


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _zero_signal() -> UnfindableSearchLogSignal:
    return UnfindableSearchLogSignal(
        cycles_observed=0, zero_find_cycles=0, wrong_pressing_hits=0,
    )


def _inputs(
    *,
    total_tracks: int | None = 12,
    current_category: str | None = None,
    probe_match_counts: tuple[int, ...] = (),
    probe_observed_artist_match: bool = False,
    search_log_signal: UnfindableSearchLogSignal | None = None,
) -> UnfindableInputs:
    return UnfindableInputs(
        total_tracks=total_tracks,
        current_category=current_category,
        probe_match_counts=probe_match_counts,
        probe_observed_artist_match=probe_observed_artist_match,
        search_log_signal=search_log_signal or _zero_signal(),
    )


# ---------------------------------------------------------------------------
# 1. Pure classifier — subTest table.
# ---------------------------------------------------------------------------


class TestClassifyUnfindableFromState(unittest.TestCase):
    """Decision-matrix coverage over the 4 categories + edges.

    Pattern: one ``CASES`` table of ``(desc, inputs, expected_category
    or None)`` rows, one subTest per row. Per code-quality.md §
    Pure-function tests.
    """

    CASES: list[tuple[str, UnfindableInputs, str | None]] = [
        # 1. one_track_structural — total_tracks == 1 dominates.
        (
            "one_track_structural fires regardless of probes",
            _inputs(
                total_tracks=1,
                probe_match_counts=(50, 60, 70),
                probe_observed_artist_match=True,
            ),
            CATEGORY_ONE_TRACK_STRUCTURAL,
        ),
        (
            "one_track_structural fires for zero-track unset rows",
            _inputs(total_tracks=0),
            CATEGORY_ONE_TRACK_STRUCTURAL,
        ),
        # 2. wrong_pressing_available — outranks artist_absent because
        # the network DOES have the album, just not in the operator's
        # pressing.
        (
            "wrong_pressing_available fires when hits >= min",
            _inputs(
                total_tracks=12,
                probe_match_counts=(0, 0),
                probe_observed_artist_match=False,
                search_log_signal=UnfindableSearchLogSignal(
                    cycles_observed=3,
                    zero_find_cycles=3,
                    wrong_pressing_hits=WRONG_PRESSING_MIN_HITS,
                ),
            ),
            CATEGORY_WRONG_PRESSING_AVAILABLE,
        ),
        # 3. artist_absent — K consecutive low probes AND no fuzzy
        # artist match observed.
        (
            "artist_absent: 2 low probes + no fuzzy match",
            _inputs(
                total_tracks=12,
                probe_match_counts=(0, 1),
                probe_observed_artist_match=False,
            ),
            CATEGORY_ARTIST_ABSENT,
        ),
        # 4. album_absent_artist_present — probe shows artist present,
        # M cycles zero finds.
        (
            "album_absent_artist_present: probe high + zero_find_cycles met",
            _inputs(
                total_tracks=12,
                probe_match_counts=(20,),
                probe_observed_artist_match=True,
                search_log_signal=UnfindableSearchLogSignal(
                    cycles_observed=3,
                    zero_find_cycles=REQUIRED_ZERO_FIND_CYCLES,
                    wrong_pressing_hits=0,
                ),
            ),
            CATEGORY_ALBUM_ABSENT_ARTIST_PRESENT,
        ),
        # 5. Boundary: match_count == threshold is INCLUSIVE
        # ("artist is on the network"). Two probes at exactly the
        # threshold do NOT fire artist_absent.
        (
            "boundary: match_count == threshold is not low",
            _inputs(
                total_tracks=12,
                probe_match_counts=(
                    ARTIST_MATCH_THRESHOLD, ARTIST_MATCH_THRESHOLD,
                ),
                probe_observed_artist_match=False,
            ),
            None,
        ),
        # 6. Boundary: match_count == threshold-1 IS low.
        (
            "boundary: match_count == threshold-1 is low",
            _inputs(
                total_tracks=12,
                probe_match_counts=(
                    ARTIST_MATCH_THRESHOLD - 1,
                    ARTIST_MATCH_THRESHOLD - 1,
                ),
                probe_observed_artist_match=False,
            ),
            CATEGORY_ARTIST_ABSENT,
        ),
        # 7. Insufficient signal: only one probe so far, K=2 required
        # for artist_absent.
        (
            "insufficient signal: one probe, K=2 required",
            _inputs(
                total_tracks=12,
                probe_match_counts=(0,),
                probe_observed_artist_match=False,
            ),
            None,
        ),
        # 8. Fuzzy match defangs artist_absent (quiet day on a known
        # artist).
        (
            "fuzzy artist match defangs artist_absent",
            _inputs(
                total_tracks=12,
                probe_match_counts=(0, 0),
                probe_observed_artist_match=True,
            ),
            None,
        ),
        # 9. Zero-find cycles below M do not fire album_absent_present.
        (
            "zero_find_cycles below threshold: no album_absent verdict",
            _inputs(
                total_tracks=12,
                probe_match_counts=(50,),
                probe_observed_artist_match=True,
                search_log_signal=UnfindableSearchLogSignal(
                    cycles_observed=2,
                    zero_find_cycles=REQUIRED_ZERO_FIND_CYCLES - 1,
                    wrong_pressing_hits=0,
                ),
            ),
            None,
        ),
        # 10. Downgrade: previously artist_absent, recent probe surged
        # → returns None (service then clears the column).
        (
            "downgrade: prior artist_absent + match surge → None",
            _inputs(
                total_tracks=12,
                current_category=CATEGORY_ARTIST_ABSENT,
                probe_match_counts=(50, 0),
                probe_observed_artist_match=True,
            ),
            None,
        ),
        # 11. Wrong-pressing hits just below threshold do not fire.
        (
            "wrong_pressing_hits below min: no verdict",
            _inputs(
                total_tracks=12,
                search_log_signal=UnfindableSearchLogSignal(
                    cycles_observed=2,
                    zero_find_cycles=0,
                    wrong_pressing_hits=WRONG_PRESSING_MIN_HITS - 1,
                ),
            ),
            None,
        ),
    ]

    def test_decision_matrix(self) -> None:
        for desc, inputs, expected in self.CASES:
            with self.subTest(desc=desc):
                verdict = classify_unfindable_from_state(inputs)
                if expected is None:
                    self.assertIsNone(
                        verdict,
                        msg=f"{desc}: got {verdict!r}, expected None",
                    )
                else:
                    self.assertIsNotNone(verdict, msg=f"{desc}: got None")
                    assert verdict is not None  # for pyright
                    self.assertEqual(
                        verdict.category, expected,
                        msg=f"{desc}: got {verdict.category!r}",
                    )
                    # Every verdict carries a non-empty reason.
                    self.assertTrue(
                        verdict.reason and verdict.reason.strip(),
                        msg=f"{desc}: empty reason",
                    )


class TestFuzzyArtistObservedInProbe(unittest.TestCase):
    """``fuzzy_artist_observed_in_probe`` heuristic — narrow contract."""

    def test_username_substring_match(self) -> None:
        responses = [
            {"username": "GreatRussianWinters42", "files": []},
        ]
        self.assertTrue(
            fuzzy_artist_observed_in_probe("Russian Winters", responses),
        )

    def test_filename_substring_match(self) -> None:
        responses = [
            {
                "username": "random",
                "files": [
                    {"filename": "/music/Russian-Winters/track01.flac"},
                ],
            },
        ]
        self.assertTrue(
            fuzzy_artist_observed_in_probe("Russian Winters", responses),
        )

    def test_no_match(self) -> None:
        responses = [
            {"username": "someone", "files": [{"filename": "kid_a.mp3"}]},
        ]
        self.assertFalse(
            fuzzy_artist_observed_in_probe("Russian Winters", responses),
        )

    def test_empty_artist_is_falsy(self) -> None:
        self.assertFalse(
            fuzzy_artist_observed_in_probe("", [{"username": "x"}]),
        )

    def test_empty_responses(self) -> None:
        self.assertFalse(
            fuzzy_artist_observed_in_probe("Radiohead", []),
        )


# ---------------------------------------------------------------------------
# 2. Service-layer tests.
# ---------------------------------------------------------------------------


def _seed_wanted_request(
    db: FakePipelineDB,
    *,
    artist_name: str = "Russian Winters",
    total_tracks: int = 12,
    last_artist_probe_at: datetime | None = None,
    last_artist_probe_match_count: int | None = None,
    unfindable_category: str | None = None,
) -> int:
    rid = db.add_request(
        artist_name=artist_name,
        album_title=f"{artist_name} Album",
        source="request",
        mb_release_id=f"mb-{artist_name.replace(' ', '_')}-1",
    )
    if total_tracks > 0:
        db.set_tracks(rid, [
            {"disc_number": 1, "track_number": i + 1, "title": f"T{i}"}
            for i in range(total_tracks)
        ])
    overrides: dict[str, Any] = {}
    if last_artist_probe_at is not None:
        overrides["last_artist_probe_at"] = last_artist_probe_at
    if last_artist_probe_match_count is not None:
        overrides["last_artist_probe_match_count"] = (
            last_artist_probe_match_count)
    if unfindable_category is not None:
        overrides["unfindable_category"] = unfindable_category
    if overrides:
        db.update_request_fields(rid, **overrides)
        db.update_request_fields_calls.pop()  # don't pollute call recorder
    return rid


class _StubProbe:
    """Drop-in for ``run_artist_probe`` — records calls + returns canned."""

    def __init__(
        self,
        *,
        match_count: int = 0,
        artist_observed: bool = False,
        raise_exc: Exception | None = None,
    ) -> None:
        self.match_count = match_count
        self.artist_observed = artist_observed
        self.raise_exc = raise_exc
        self.calls: list[tuple[Any, str]] = []

    def __call__(
        self,
        slskd_client: Any,
        *,
        artist_name: str,
        **_kwargs: Any,
    ) -> ArtistProbeResult:
        self.calls.append((slskd_client, artist_name))
        if self.raise_exc is not None:
            raise self.raise_exc
        return ArtistProbeResult(
            match_count=self.match_count,
            artist_observed=self.artist_observed,
        )


class TestUnfindableDetectionService(unittest.TestCase):
    """Drive the service against FakePipelineDB + FakeSlskdAPI."""

    def setUp(self) -> None:
        self.db = FakePipelineDB()
        self.slskd = FakeSlskdAPI()

    def _service(
        self,
        probe: _StubProbe,
        *,
        now: datetime | None = None,
    ) -> UnfindableDetectionService:
        now_fn = (
            (lambda: now) if now is not None
            else (lambda: datetime.now(timezone.utc))
        )
        return UnfindableDetectionService(
            self.db, self.slskd,
            probe_runner=probe, now_fn=now_fn,
        )

    def test_request_not_found(self) -> None:
        probe = _StubProbe(match_count=0)
        svc = self._service(probe)
        result = svc.categorise_request(9999)
        self.assertEqual(result.outcome, RESULT_REQUEST_NOT_FOUND)
        self.assertEqual(probe.calls, [])
        self.assertEqual(self.db.record_artist_probe_calls, [])

    def test_artist_absent_categorisation_writes_probe_and_category(self) -> None:
        rid = _seed_wanted_request(
            self.db,
            last_artist_probe_match_count=0,
            last_artist_probe_at=datetime.now(timezone.utc) - timedelta(days=14),
        )
        now = datetime.now(timezone.utc)
        probe = _StubProbe(match_count=0, artist_observed=False)
        svc = self._service(probe, now=now)

        result = svc.categorise_request(rid)

        self.assertEqual(result.outcome, RESULT_CATEGORISED)
        self.assertEqual(result.new_category, CATEGORY_ARTIST_ABSENT)
        self.assertEqual(result.probe_match_count, 0)
        # Probe recorded.
        self.assertEqual(len(self.db.record_artist_probe_calls), 1)
        rec_rid, rec_count, rec_ts = self.db.record_artist_probe_calls[0]
        self.assertEqual(rec_rid, rid)
        self.assertEqual(rec_count, 0)
        self.assertEqual(rec_ts, now)
        # Category written.
        self.assertEqual(len(self.db.set_unfindable_category_calls), 1)
        cat_rid, cat_val, _ = self.db.set_unfindable_category_calls[0]
        self.assertEqual(cat_rid, rid)
        self.assertEqual(cat_val, CATEGORY_ARTIST_ABSENT)
        # Row state reflects both writes.
        row = self.db.request(rid)
        self.assertEqual(row["unfindable_category"], CATEGORY_ARTIST_ABSENT)
        self.assertEqual(row["last_artist_probe_match_count"], 0)

    def test_one_track_structural_no_probe_needed_to_decide(self) -> None:
        rid = _seed_wanted_request(self.db, total_tracks=1)
        probe = _StubProbe(match_count=42, artist_observed=True)
        svc = self._service(probe)

        result = svc.categorise_request(rid)
        # Probe still runs (the categorise pass is the probe pass),
        # but the structural rule fires regardless.
        self.assertEqual(result.outcome, RESULT_CATEGORISED)
        self.assertEqual(result.new_category, CATEGORY_ONE_TRACK_STRUCTURAL)
        self.assertEqual(len(probe.calls), 1)

    def test_album_absent_artist_present(self) -> None:
        rid = _seed_wanted_request(self.db)
        # Seed M cycles of zero-find consumed attempts in the
        # search-log so the signal aggregator picks them up.
        for cycle in range(REQUIRED_ZERO_FIND_CYCLES):
            self.db.log_search(
                request_id=rid, outcome="no_match", query=f"q{cycle}",
            )
            # Stamp the plan-context fields directly via the row so the
            # aggregator's plan_cycle_snapshot filter fires.
            self.db.search_logs[-1].plan_cycle_snapshot = cycle
            self.db.search_logs[-1].attempt_consumed = True

        probe = _StubProbe(match_count=20, artist_observed=True)
        svc = self._service(probe)
        result = svc.categorise_request(rid)
        self.assertEqual(result.outcome, RESULT_CATEGORISED)
        self.assertEqual(
            result.new_category, CATEGORY_ALBUM_ABSENT_ARTIST_PRESENT,
        )

    def test_wrong_pressing_available(self) -> None:
        rid = _seed_wanted_request(self.db)
        for i in range(WRONG_PRESSING_MIN_HITS):
            self.db.log_search(
                request_id=rid, outcome="no_match", query=f"q{i}",
                rejection_reason="strict_count_mismatch",
                matcher_score_top1=0.9,
            )
            self.db.search_logs[-1].attempt_consumed = True
            self.db.search_logs[-1].plan_cycle_snapshot = 0

        probe = _StubProbe(match_count=0, artist_observed=False)
        svc = self._service(probe)
        result = svc.categorise_request(rid)
        # Wrong-pressing dominates artist_absent because the network
        # demonstrably has SOME pressing of the album.
        self.assertEqual(result.outcome, RESULT_CATEGORISED)
        self.assertEqual(result.new_category, CATEGORY_WRONG_PRESSING_AVAILABLE)

    def test_downgrade_clears_prior_category(self) -> None:
        """Prior artist_absent + probe match surge → clear column."""
        now = datetime.now(timezone.utc)
        rid = _seed_wanted_request(
            self.db,
            unfindable_category=CATEGORY_ARTIST_ABSENT,
            last_artist_probe_match_count=0,
            last_artist_probe_at=now - timedelta(days=14),
        )
        # Probe now surges + fuzzy artist match observed → classifier
        # returns None → service clears column.
        probe = _StubProbe(match_count=100, artist_observed=True)
        svc = self._service(probe, now=now)
        result = svc.categorise_request(rid)
        self.assertEqual(result.outcome, RESULT_DOWNGRADED)
        self.assertEqual(result.previous_category, CATEGORY_ARTIST_ABSENT)
        self.assertIsNone(result.new_category)
        row = self.db.request(rid)
        self.assertIsNone(row["unfindable_category"])
        self.assertIsNotNone(row["unfindable_categorised_at"])

    def test_no_change_leaves_unset_row_untouched(self) -> None:
        """No prior category + classifier returns None → no category write."""
        rid = _seed_wanted_request(self.db)  # single probe → insufficient
        probe = _StubProbe(match_count=2, artist_observed=False)
        svc = self._service(probe)
        result = svc.categorise_request(rid)
        self.assertEqual(result.outcome, RESULT_NO_CHANGE)
        # Probe IS recorded; category is NOT touched.
        self.assertEqual(len(self.db.record_artist_probe_calls), 1)
        self.assertEqual(len(self.db.set_unfindable_category_calls), 0)
        row = self.db.request(rid)
        self.assertIsNone(row["unfindable_category"])

    def test_not_due_skips_slskd_call(self) -> None:
        """Probe within PROBE_INTERVAL_DAYS → skipped, no slskd hit."""
        now = datetime.now(timezone.utc)
        # 1 day old → well within the 7d window.
        recent = now - timedelta(days=1)
        rid = _seed_wanted_request(
            self.db,
            last_artist_probe_at=recent,
            last_artist_probe_match_count=10,
        )
        probe = _StubProbe(match_count=999)
        svc = self._service(probe, now=now)
        result = svc.categorise_request(rid)
        self.assertEqual(result.outcome, RESULT_NOT_DUE)
        self.assertEqual(probe.calls, [])

    def test_force_probe_overrides_cadence(self) -> None:
        now = datetime.now(timezone.utc)
        recent = now - timedelta(days=1)
        rid = _seed_wanted_request(
            self.db, total_tracks=1,
            last_artist_probe_at=recent,
            last_artist_probe_match_count=10,
        )
        probe = _StubProbe(match_count=999)
        svc = self._service(probe, now=now)
        result = svc.categorise_request(rid, force_probe=True)
        self.assertEqual(len(probe.calls), 1)
        self.assertEqual(result.new_category, CATEGORY_ONE_TRACK_STRUCTURAL)

    def test_probe_failure_isolated(self) -> None:
        rid = _seed_wanted_request(self.db)
        probe = _StubProbe(raise_exc=RuntimeError("slskd connection lost"))
        svc = self._service(probe)
        result = svc.categorise_request(rid)
        self.assertEqual(result.outcome, RESULT_PROBE_FAILED)
        # Row state untouched by the failed probe.
        self.assertEqual(len(self.db.record_artist_probe_calls), 0)
        self.assertEqual(len(self.db.set_unfindable_category_calls), 0)
        self.assertIn("RuntimeError", result.error_message or "")

    def test_cadence_independent_of_plan_cursor(self) -> None:
        """next_plan_ordinal stays unchanged across a categorisation."""
        now = datetime.now(timezone.utc)
        rid = _seed_wanted_request(
            self.db,
            last_artist_probe_at=now - timedelta(days=8),
            last_artist_probe_match_count=0,
        )
        # Seed the row with a non-zero cursor + cycle so we can verify
        # the service does not bump them.
        self.db.update_request_fields(
            rid, next_plan_ordinal=5, plan_cycle_count=2,
        )
        self.db.update_request_fields_calls.pop()
        probe = _StubProbe(match_count=0)
        svc = self._service(probe, now=now)
        svc.categorise_request(rid)
        row = self.db.request(rid)
        self.assertEqual(row["next_plan_ordinal"], 5)
        self.assertEqual(row["plan_cycle_count"], 2)

    def test_categorise_due_batch_processes_oldest_first(self) -> None:
        now = datetime.now(timezone.utc)
        # Three rows; one freshly-probed (NOT due), two due.
        rid_fresh = _seed_wanted_request(
            self.db, artist_name="Fresh",
            last_artist_probe_at=now - timedelta(days=1),
        )
        rid_due_a = _seed_wanted_request(
            self.db, artist_name="Old A",
            last_artist_probe_at=now - timedelta(days=PROBE_INTERVAL_DAYS + 2),
        )
        rid_due_never = _seed_wanted_request(
            self.db, artist_name="Never",
            # last_artist_probe_at NULL by default — sorts first.
        )

        probe = _StubProbe(match_count=0)
        svc = self._service(probe, now=now)
        results = svc.categorise_due_batch(limit=10)

        rids_processed = [r.request_id for r in results]
        # Fresh row not included.
        self.assertNotIn(rid_fresh, rids_processed)
        # Never-probed sorts before any timestamp (oldest first).
        self.assertEqual(rids_processed[0], rid_due_never)
        self.assertIn(rid_due_a, rids_processed)

    def test_rescue_race_late_writes_become_silent_noop(self) -> None:
        """Detection's late writes do not clobber a concurrent rescue.

        Adversarial race: the daily detection job reads a ``wanted``
        row, fires slskd (slow), then writes ``record_artist_probe``
        and ``set_unfindable_category``. In the slskd window,
        ``mark_imported_with_rescue`` (U14) flips status to
        ``imported`` and clears ``unfindable_category``. Without the
        ``AND status='wanted'`` guard on both writers, detection's
        late writes would re-stamp ``last_artist_probe_*`` and
        ``unfindable_category='artist_absent'`` on top of the
        rescued row, leaving an incoherent ``status='imported' AND
        unfindable_category='…'`` audit trail.

        With the guard, both writes are silent no-ops — rescue wins.
        """
        now = datetime.now(timezone.utc)
        rid = _seed_wanted_request(
            self.db,
            last_artist_probe_at=now - timedelta(days=14),
            last_artist_probe_match_count=0,
        )

        # Probe stub mutates the row in the slskd window — simulates
        # a rescue (mark_imported_with_rescue) landing between read
        # and write.
        class _RaceProbe:
            def __init__(self, db: FakePipelineDB, request_id: int) -> None:
                self.db = db
                self.request_id = request_id

            def __call__(
                self, _client: Any, *, artist_name: str, **_kw: Any,
            ) -> ArtistProbeResult:
                # Mid-probe rescue: status flips to imported,
                # unfindable_category cleared, rescue stamp written.
                self.db.update_request_fields(
                    self.request_id,
                    status="imported",
                    unfindable_category=None,
                    rescued_at=datetime.now(timezone.utc),
                )
                # Drop the recorder write so it doesn't pollute the
                # service's own assertion targets.
                self.db.update_request_fields_calls.pop()
                return ArtistProbeResult(
                    match_count=0, artist_observed=False,
                )

        probe = _RaceProbe(self.db, rid)
        svc = self._service(_StubProbe(match_count=0), now=now)
        # Swap in the race probe so the rescue fires inside the probe call.
        svc._probe_runner = probe

        svc.categorise_request(rid)

        # The row is the rescued shape — detection's late writes were
        # silent no-ops. status='imported', unfindable_category=NULL,
        # last_artist_probe_match_count stays at its pre-race value
        # (the detection's late record_artist_probe was a no-op).
        row = self.db.request(rid)
        self.assertEqual(row["status"], "imported")
        self.assertIsNone(row["unfindable_category"])
        self.assertIsNotNone(row["rescued_at"])
        # The pre-race probe count (0) is preserved, not overwritten.
        # last_artist_probe_at was last bumped 14 days ago at seed time.
        self.assertEqual(row["last_artist_probe_match_count"], 0)
        self.assertNotEqual(row["last_artist_probe_at"], now)

    def test_categorise_due_batch_isolates_per_row_crash(self) -> None:
        """A single bad row does not poison the rest of the batch."""
        rid_a = _seed_wanted_request(self.db, artist_name="Alpha")
        rid_b = _seed_wanted_request(self.db, artist_name="Beta")

        class _CrashingProbe:
            def __init__(self) -> None:
                self.n = 0

            def __call__(
                self, _client: Any, *, artist_name: str, **_kw: Any,
            ) -> ArtistProbeResult:
                self.n += 1
                if self.n == 1:
                    raise RuntimeError("transient")
                return ArtistProbeResult(
                    match_count=0, artist_observed=False,
                )

        probe = _CrashingProbe()
        svc = UnfindableDetectionService(
            self.db, self.slskd, probe_runner=probe,
        )
        results = svc.categorise_due_batch(limit=10)
        # Both rows produced an outcome — the crash didn't drop the
        # second row.
        outcomes = {r.request_id: r.outcome for r in results}
        self.assertIn(rid_a, outcomes)
        self.assertIn(rid_b, outcomes)
        self.assertEqual(
            sum(1 for r in results if r.outcome == RESULT_PROBE_FAILED),
            1,
        )


# ---------------------------------------------------------------------------
# 3. R20 invariant — TWO LAYERS (structural + runtime).
# ---------------------------------------------------------------------------


# Names that, if referenced from the detection module or its oneshot,
# would let the daily job touch the regular plan cadence. Kept as a
# single source of truth so the structural test matches what the
# runtime guard asserts on. The list mirrors the U13 plan's banned
# names plus every cursor-mutating PipelineDB method enumerable from
# the production class.
_CURSOR_MUTATION_BANNED_NAMES: frozenset[str] = frozenset({
    # Per-request cursor / cycle columns the executor walks.
    "next_plan_ordinal",
    "plan_cycle_count",
    # SearchPlanService / pipeline_db cursor-mutation methods.
    "advance_for_request",
    "advance_search_plan_cursor",
    "record_consumed_search_attempt",
    "record_non_consuming_search_attempt",
})


def _walk_names(tree: ast.AST) -> set[str]:
    """Return every Name / Attribute identifier in the AST."""
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            found.add(node.id)
        elif isinstance(node, ast.Attribute):
            found.add(node.attr)
        elif isinstance(node, ast.alias):
            # Imports — catch ``from X import advance_for_request``.
            found.add(node.name.split(".")[-1])
            if node.asname:
                found.add(node.asname)
    return found


class TestR20CursorIsolation(unittest.TestCase):
    """R20: the detection unit cannot touch the regular search cadence.

    Two enforcement layers — both required by the U13 plan:

    * **Structural**: AST walk over the detection module + oneshot
      script rejects any reference to the cursor-mutation banlist.
    * **Runtime**: a representative cohort run leaves every cursor-
      mutation recorder on ``FakePipelineDB`` at ``call_count == 0``.
    """

    REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    AUDITED_FILES = (
        "lib/unfindable_detection_service.py",
        "scripts/run_unfindable_detection.py",
    )

    def test_module_does_not_reference_cursor_mutators(self) -> None:
        for rel in self.AUDITED_FILES:
            with self.subTest(file=rel):
                path = os.path.join(self.REPO_ROOT, rel)
                with open(path, "r", encoding="utf-8") as fh:
                    source = fh.read()
                tree = ast.parse(source, filename=path)
                names = _walk_names(tree)
                offenders = names & _CURSOR_MUTATION_BANNED_NAMES
                self.assertFalse(
                    offenders,
                    msg=(
                        f"{rel} references cursor-mutation names "
                        f"{sorted(offenders)}; R20 (cadence-never-"
                        "changes) forbids this — see CLAUDE.md and "
                        "the U13 plan."
                    ),
                )

    def test_categorise_run_does_not_touch_cursor_state(self) -> None:
        """Runtime guard: cursor recorders stay empty across a batch."""
        db = FakePipelineDB()
        slskd = FakeSlskdAPI()

        # Cover all four category branches in one batch — the most
        # representative cohort we can exercise.
        # 1. one_track_structural
        rid_ots = _seed_wanted_request(
            db, artist_name="Solo", total_tracks=1,
        )
        # 2. artist_absent
        rid_abs = _seed_wanted_request(
            db, artist_name="GoneArtist",
            last_artist_probe_at=(
                datetime.now(timezone.utc)
                - timedelta(days=PROBE_INTERVAL_DAYS + 1)),
            last_artist_probe_match_count=0,
        )
        # 3. wrong_pressing_available
        rid_wp = _seed_wanted_request(db, artist_name="WP")
        for i in range(WRONG_PRESSING_MIN_HITS):
            db.log_search(
                request_id=rid_wp, outcome="no_match",
                query=f"wp{i}",
                rejection_reason="strict_count_mismatch",
                matcher_score_top1=0.9,
            )
            db.search_logs[-1].attempt_consumed = True
            db.search_logs[-1].plan_cycle_snapshot = 0
        # 4. album_absent_artist_present
        rid_aap = _seed_wanted_request(db, artist_name="Present")
        for cycle in range(REQUIRED_ZERO_FIND_CYCLES):
            db.log_search(
                request_id=rid_aap, outcome="no_match",
                query=f"aap{cycle}",
            )
            db.search_logs[-1].attempt_consumed = True
            db.search_logs[-1].plan_cycle_snapshot = cycle
        # 5. downgrade (clears prior category)
        rid_dg = _seed_wanted_request(
            db, artist_name="Recovered",
            unfindable_category=CATEGORY_ARTIST_ABSENT,
            last_artist_probe_at=(
                datetime.now(timezone.utc)
                - timedelta(days=PROBE_INTERVAL_DAYS + 1)),
            last_artist_probe_match_count=0,
        )

        # Drive each scenario with a tailored probe stub. The
        # ArtistMatch / matchcount drives which branch wins.
        per_artist_probe = {
            "Solo": ArtistProbeResult(match_count=0, artist_observed=False),
            "GoneArtist": ArtistProbeResult(match_count=0, artist_observed=False),
            "WP": ArtistProbeResult(match_count=0, artist_observed=False),
            "Present": ArtistProbeResult(match_count=50, artist_observed=True),
            "Recovered": ArtistProbeResult(match_count=100, artist_observed=True),
        }

        def _probe(_client: Any, *, artist_name: str, **_kw: Any) -> ArtistProbeResult:
            return per_artist_probe[artist_name]

        svc = UnfindableDetectionService(db, slskd, probe_runner=_probe)
        results = svc.categorise_due_batch(limit=100)
        # Sanity: every seeded request got an outcome.
        rids_seen = {r.request_id for r in results}
        self.assertEqual(
            rids_seen,
            {rid_ots, rid_abs, rid_wp, rid_aap, rid_dg},
        )

        # The actual R20 contract — none of the cursor mutators fired.
        self.assertEqual(
            db.record_consumed_search_attempt_calls, [],
            msg="record_consumed_search_attempt fired during detection",
        )
        self.assertEqual(
            db.record_non_consuming_search_attempt_calls, [],
            msg="record_non_consuming_search_attempt fired during detection",
        )
        self.assertEqual(
            db.advance_search_plan_cursor_calls, [],
            msg="advance_search_plan_cursor fired during detection",
        )

        # Belt-and-braces row-level check: cursor / cycle / failure_class
        # columns are exactly what we seeded for every row.
        for rid in rids_seen:
            row = db.request(rid)
            self.assertEqual(
                row["next_plan_ordinal"], 0,
                msg=f"request {rid} next_plan_ordinal mutated",
            )
            self.assertEqual(
                row["plan_cycle_count"], 0,
                msg=f"request {rid} plan_cycle_count mutated",
            )


if __name__ == "__main__":
    unittest.main()
